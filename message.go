package main

import (
	"bytes"
	"encoding/gob"
)

// 将消息向上封装一层，加了一个 timerChan
// 向 timerChan 赋值会使 Router 结束循环
type Message struct {
	// first 16 bytes are the UUID
	Data      []byte
	timerChan chan int
}

func NewMessage(data []byte) *Message {
	return &Message{data, make(chan int)}
}

func NewMessageDecoded(byteBuf []byte) (*Message, error) {
	var buf bytes.Buffer
	var msg *Message
	var err error

	_, err = buf.Write(byteBuf)
	if err != nil {
		return nil, err
	}

	decoder := gob.NewDecoder(&buf)
	err = decoder.Decode(&msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Message) Uuid() []byte {
	return m.Data[:16]
}

func (m *Message) Body() []byte {
	return m.Data[16:]
}

// 为什么要这么写，为什么不写成：m.timerChan <- 1？
// 因为 timerChan 没有大小限制的。make(chan int)
// 猜测可能是因为 NewMessageDecoded 中的万一有大小
// 限制，所以这么写。
func (m *Message) EndTimer() {
	select {
	case m.timerChan <- 1:
	default:
	}
}

func (m *Message) Encode() ([]byte, error) {
	var buf bytes.Buffer

	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(*m)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
