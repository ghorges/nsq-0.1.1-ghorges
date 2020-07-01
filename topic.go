package main

import (
	"log"
)

type Topic struct {
	name                 string
	newChannelChan       chan ChanReq
	channelMap           map[string]*Channel
	backend              NSQueue
	incomingMessageChan  chan *Message
	msgChan              chan *Message
	routerSyncChan       chan int
	readSyncChan         chan int
	exitChan             chan int
	channelWriterStarted bool
}

// 这个 map 是指：每一个 topic name 对应一个 * Topic
var topicMap = make(map[string]*Topic)
var newTopicChan = make(chan ChanReq)

// Topic constructor
func NewTopic(topicName string, inMemSize int) *Topic {
	topic := &Topic{name: topicName,
		newChannelChan:       make(chan ChanReq),
		channelMap:           make(map[string]*Channel),
		backend:              NewDiskQueue(topicName),
		incomingMessageChan:  make(chan *Message, 5),
		msgChan:              make(chan *Message, inMemSize),
		routerSyncChan:       make(chan int, 1),
		readSyncChan:         make(chan int),
		exitChan:             make(chan int),
		channelWriterStarted: false}
	// 每一个 topic 都会有自身的循环
	go topic.Router(inMemSize)
	return topic
}

// todo 为什么是线程安全的
// 解答：这个类似于锁，但是效率更搞，用 channel 的方式替换锁。
// 将每一个消息放入 channel 中，然后等待 channel 返回即可。
// 这波 channel 用的挺迷惑，但是本质上就是创建/返回 topic。
// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
// see: topicFactory()
func GetTopic(topicName string) *Topic {
	topicChan := make(chan interface{})
	newTopicChan <- ChanReq{topicName, topicChan}
	return (<-topicChan).(*Topic)
}

// topicFactory 的主要作用是通过 topic name 返回 topic。
// 此函数单独运行了一个协程。
// topicFactory is executed in a goroutine and manages
// the creation/retrieval of Topic objects
func topicFactory(inMemSize int) {
	var topic *Topic
	var ok bool

	for {
		topicReq := <-newTopicChan
		name := topicReq.variable.(string)
		// 查看是否在 map 中。
		if topic, ok = topicMap[name]; !ok {
			topic = NewTopic(name, inMemSize)
			topicMap[name] = topic
			log.Printf("TOPIC(%s): created", topic.name)
		}
		// 将获取到的 topic 返回
		topicReq.retChan <- topic
	}
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
// see: Topic.Router()
func (t *Topic) GetChannel(channelName string) *Channel {
	channelChan := make(chan interface{})
	t.newChannelChan <- ChanReq{channelName, channelChan}
	return (<-channelChan).(*Channel)
}

// 最先从 http 调用到这里来。
// PutMessage writes to the appropriate incoming
// message channel
func (t *Topic) PutMessage(msg *Message) {
	// log.Printf("TOPIC(%s): PutMessage(%s)", t.name, string(msg.Body()))
	t.incomingMessageChan <- msg
}

// 将消息放入 put 到 channel 中。
// MessagePump selects over the in-memory and backend queue and 
// writes messages to every channel for this topic, synchronizing
// with the channel router
func (t *Topic) MessagePump() {
	var msg *Message

	for {
		select {
		// 如果是 msg，跳过 select 并执行下面的。
		case msg = <-t.msgChan:
		case <-t.backend.ReadReadyChan():
			buf, err := t.backend.Get()
			if err != nil {
				log.Printf("ERROR: t.backend.Get() - %s", err.Error())
				continue
			}
			msg = NewMessage(buf)
		}

		// 这里和下面的 t.routerSyncChan <- 1 遥相呼应。
		// 相当于对每一个 topic 加锁，而不是对这个函数加锁。
		t.readSyncChan <- 1
		log.Printf("TOPIC(%s): channelMap %#v", t.name, t.channelMap)
		for _, channel := range t.channelMap {
			go func(c *Channel) {
				// log.Printf("TOPIC(%s): writing message to channel(%s)", t.name, c.name)
				c.PutMessage(msg)
			}(channel)
		}
		t.routerSyncChan <- 1
	}
}

// Router handles muxing of Topic messages including
// creation of new Channel objects, proxying messages
// to memory or backend, and synchronizing reads
func (t *Topic) Router(inMemSize int) {
	var msg *Message

	for {
		select {
		// todo 新建立 channel 时，上面是被 sub 调用的
		// 如果没有此 channel，建立这个 channel 并关联到
		// 	channelMap 中，否则返回此 channel。
		case channelReq := <-t.newChannelChan:
			name := channelReq.variable.(string)
			channel, ok := t.channelMap[name]
			if !ok {
				channel = NewChannel(name, inMemSize)
				t.channelMap[name] = channel
				log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
			}
			channelReq.retChan <- channel
			if !t.channelWriterStarted {
				// 每次新建 channel 时，会启动此协程
				go t.MessagePump()
				t.channelWriterStarted = true
			}
			// 这里是当有消息来的时候触发的 channel
		case msg = <-t.incomingMessageChan:
			select {
			case t.msgChan <- msg:
				// log.Printf("TOPIC(%s): wrote to messageChan", t.name)
			default:
				// 仅仅是 put 到队列中去了，但是基本不会执行到这，
				// 前面便会结束，走到这说明 msgChan 被阻塞了。
				err := t.backend.Put(msg.Data)
				if err != nil {
					log.Printf("ERROR: t.backend.Put() - %s", err.Error())
					// TODO: requeue?
				}
				// log.Printf("TOPIC(%s): wrote to backend", t.name)
			}
		case <-t.readSyncChan:
			// log.Printf("TOPIC(%s): read sync START", t.name)
			<-t.routerSyncChan
			// log.Printf("TOPIC(%s): read sync END", t.name)
		case <-t.exitChan:
			return
		}
	}
}

func (t *Topic) Close() error {
	var err error

	log.Printf("TOPIC(%s): closing", t.name)

	t.exitChan <- 1

	for _, channel := range t.channelMap {
		// 依次关闭每一个 channel。
		err = channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			log.Printf("ERROR: channel(%s) close - %s", channel.name, err.Error())
		}
	}

	err = t.backend.Close()
	if err != nil {
		return err
	}

	return nil
}
