package main

type ChanReq struct {
	// topic name	这个同下。
	variable interface{}
	// 对应的 topic	写的有问题，应该是什么都可以用。
	retChan  chan interface{}
}

type ChanRet struct {
	err      error
	variable interface{}
}
