package main

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

import _ "net/http/pprof"

type ReqParams struct {
	params url.Values
	body   []byte
}

func NewReqParams(req *http.Request) (*ReqParams, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	return &ReqParams{reqParams, data}, nil
}

func (r *ReqParams) Query(key string) (string, error) {
	keyData := r.params[key]
	if len(keyData) == 0 {
		return "", errors.New("key not in query params")
	}
	return keyData[0], nil
}

func httpServer(address string, port string, endChan chan int) {
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/put", putHandler)
	go func() {
		err := http.ListenAndServe(address+":"+port, nil)
		if err != nil {
			log.Fatal("http.ListenAndServe:", err)
		}
	}()
	<-endChan
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

// 消费者调用 http
func putHandler(w http.ResponseWriter, req *http.Request) {
	var buf bytes.Buffer

	reqParams, err := NewReqParams(req)
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	// 获取 topic name
	topicName, err := reqParams.Query("topic")
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	_, err = buf.Write(Uuid())
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	// body 中存放的是需要发的消息，http 头中的是 topic。
	_, err = buf.Write(reqParams.body)
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	// 现在 buf 中存的是：uuid + body

	topic := GetTopic(topicName)
	topic.PutMessage(NewMessage(buf.Bytes()))
}
