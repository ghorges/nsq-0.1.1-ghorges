package main

import (
	"errors"
	"log"
	"time"
)

type Channel struct {
	name             string
	addClientChan    chan ChanReq
	removeClientChan chan ChanReq
	// 一个 channel 对应 多个 client 是因为
	// 可能有多个相同的 channel name 连接。
	clients             []*Client
	backend             NSQueue
	incomingMessageChan chan *Message
	msgChan             chan *Message
	inFlightMessageChan chan *Message
	requeueMessageChan  chan ChanReq
	finishMessageChan   chan ChanReq
	exitChan            chan int
	inFlightMessages    map[string]*Message
}

// Channel constructor
func NewChannel(channelName string, inMemSize int) *Channel {
	channel := &Channel{name: channelName,
		addClientChan:       make(chan ChanReq),
		removeClientChan:    make(chan ChanReq),
		clients:             make([]*Client, 0, 5),
		backend:             NewDiskQueue(channelName),
		incomingMessageChan: make(chan *Message, 5),
		msgChan:             make(chan *Message, inMemSize),
		inFlightMessageChan: make(chan *Message),
		requeueMessageChan:  make(chan ChanReq),
		finishMessageChan:   make(chan ChanReq),
		exitChan:            make(chan int),
		inFlightMessages:    make(map[string]*Message)}
	// 此 channel 启动一个协程。处理 chan。
	go channel.Router()
	return channel
}

// AddClient performs a thread safe operation
// to add a Client to a Channel
// see: Channel.Router()
func (c *Channel) AddClient(client *Client) {
	log.Printf("CHANNEL(%s): adding client...", c.name)
	doneChan := make(chan interface{})
	c.addClientChan <- ChanReq{client, doneChan}
	<-doneChan
}

// close client 的时候才会调用。
func (c *Channel) RemoveClient(client *Client) {
	log.Printf("CHANNEL(%s): removing client...", c.name)
	doneChan := make(chan interface{})
	c.removeClientChan <- ChanReq{client, doneChan}
	<-doneChan
}

// 这个是最先从 topic put 进来的。
// PutMessage writes to the appropriate incoming
// message channel
func (c *Channel) PutMessage(msg *Message) {
	c.incomingMessageChan <- msg
}

// 消费者发送 FIN 信号。
func (c *Channel) FinishMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.finishMessageChan <- ChanReq{uuidStr, errChan}
	return (<-errChan).(error)
}

// 消费者发送 REQ 信号。
func (c *Channel) RequeueMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.requeueMessageChan <- ChanReq{uuidStr, errChan}
	return (<-errChan).(error)
}

// Router handles the muxing of Channel messages including
// the addition of a Client to the Channel
func (c *Channel) Router() {
	var clientReq ChanReq

	helperCloseChan := make(chan int)

	go func() {
		for {
			select {
			case msg := <-c.inFlightMessageChan:
				// 将已发送的 msg 放入 InFlight 的
				// map 中。
				c.pushInFlightMessage(msg)
				// 开启此协程
				go func(msg *Message) {
					select {
					// 如果 60s 后 msg.timerChan 还没有
					// 消息，那么就会调用 RequeueMessage。
					// 重新发送此消息。
					case <-time.After(60 * time.Second):
						log.Printf("CHANNEL(%s): auto requeue of message(%s)", c.name, UuidToStr(msg.Uuid()))
					// 接受到这个 channel 就会停止执行此协程。
					// 调用了 popInFlightMessage 就会停止。
					case <-msg.timerChan:
						return
					}
					err := c.RequeueMessage(UuidToStr(msg.Uuid()))
					if err != nil {
						log.Printf("ERROR: channel(%s) - %s", c.name, err.Error())
					}
				}(msg)
			// todo 没有搞懂
			// 搞懂了，如果 requeueMessageChan 表示此消息
			// 需要重新发送，所以回调用 c.PutMessage。
			// 一共有两种可能会调用：1.60s 超时。
			// 2.客户端发来 REQ 表示此消息需要重新发送。
			case requeueReq := <-c.requeueMessageChan:
				uuidStr := requeueReq.variable.(string)
				msg, err := c.popInFlightMessage(uuidStr)
				if err != nil {
					log.Printf("ERROR: failed to requeue message(%s) - %s", uuidStr, err.Error())
					continue
				} else {
					go func(msg *Message) {
						c.PutMessage(msg)
					}(msg)
				}
				requeueReq.retChan <- err
			// 客户端发来 FIN 命令，会调用 popInFlightMessage。
			case finishReq := <-c.finishMessageChan:
				uuidStr := finishReq.variable.(string)
				_, err := c.popInFlightMessage(uuidStr)
				if err != nil {
					log.Printf("ERROR: failed to finish message(%s) - %s", uuidStr, err.Error())
				}
				finishReq.retChan <- err
			case <-helperCloseChan:
				return
			}
		}
	}()

	for {
		select {
		// 将新来的 client 加入到 clients 中。
		case clientReq = <-c.addClientChan:
			client := clientReq.variable.(*Client)
			c.clients = append(c.clients, client)
			log.Printf("CHANNEL(%s): added client(%#v)", c.name, client)
			clientReq.retChan <- 1
		case clientReq = <-c.removeClientChan:
			indexToRemove := -1
			client := clientReq.variable.(*Client)
			// 找到对应的那个 client 并删除。
			for k, v := range c.clients {
				if v == client {
					indexToRemove = k
					break
				}
			}
			if indexToRemove == -1 {
				log.Printf("ERROR: could not find client(%#v) in clients(%#v)", client, c.clients)
			} else {
				c.clients = append(c.clients[:indexToRemove], c.clients[indexToRemove+1:]...)
				log.Printf("CHANNEL(%s): removed client(%#v)", c.name, client)
			}
			clientReq.retChan <- 1
		// 新来的消息或者从 pop 中来的消息会触发。
		case msg := <-c.incomingMessageChan:
			select {
			case c.msgChan <- msg:
				log.Printf("CHANNEL(%s): wrote to msgChan", c.name)
			default:
				err := c.backend.Put(msg.Data)
				if err != nil {
					log.Printf("ERROR: t.backend.Put() - %s", err.Error())
					// TODO: requeue?
				}
				log.Printf("CHANNEL(%s): wrote to backend", c.name)
			}
		case <-c.exitChan:
			helperCloseChan <- 1
			return
		}
	}
}

func (c *Channel) pushInFlightMessage(msg *Message) {
	uuidStr := UuidToStr(msg.Uuid())
	// 在这个 channel 中，将已发送的 uuid Str 和 msg 关联起来。
	// todo 为什么要关联？进入 inFlightMessages 不应该
	// 已经发送了？
	// 搞懂了。。。放入这里是防止万一客户端并没有收到这个消息，所以
	// 保存一段时间，如果 60s 还没有消息返回，或者客户端发来 REQ，
	// 那么就会重新发送。
	c.inFlightMessages[uuidStr] = msg
}

// 将 msg 从 inFlightMessages 取出，并调用 msg.EndTimer。
func (c *Channel) popInFlightMessage(uuidStr string) (*Message, error) {
	msg, ok := c.inFlightMessages[uuidStr]
	if !ok {
		return nil, errors.New("UUID not in flight")
	}
	delete(c.inFlightMessages, uuidStr)
	msg.EndTimer()
	return msg, nil
}

// GetMessage pulls a single message off the client channel
func (c *Channel) GetMessage() *Message {
	var msg *Message

	for {
		select {
		// 如果客户端一直不取，那么消息会积压在
		// msgChan 和队列中。
		case msg = <-c.msgChan:
		case <-c.backend.ReadReadyChan():
			buf, err := c.backend.Get()
			if err != nil {
				log.Printf("ERROR: c.backend.Get() - %s", err.Error())
				continue
			}
			msg = NewMessage(buf)
		}
		// todo 后面 return 已经将 msg 发送，那这个
		// inFlightMessageChan 是用来干什么的？
		c.inFlightMessageChan <- msg
		break
	}

	return msg
}

func (c *Channel) Close() error {
	var err error

	log.Printf("CHANNEL(%s): closing", c.name)

	c.exitChan <- 1

	// TODO: close (and wait to flush?) all clients

	err = c.backend.Close()
	if err != nil {
		return err
	}

	return nil
}
