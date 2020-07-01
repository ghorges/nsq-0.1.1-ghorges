package main

import (
	"bitly/settings"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
)

var environment = flag.String("environment", "dev", "environment to execute in")
var settingsFile = flag.String("settings-file", "conf/settings.json", "path to the settings.json file")
var bindAddress = flag.String("address", "", "address to bind to")
var webPort = flag.Int("web-port", 5150, "port to listen on for HTTP connections")
var tcpPort = flag.Int("tcp-port", 5151, "port to listen on for TCP connections")
var debugMode = flag.Bool("debug", false, "enable debug mode")
var memQueueSize = flag.Int("mem-queue-size", 10000, "number of messages to keep in memory")
var cpuProfile = flag.String("cpu-profile", "", "write cpu profile to file")

func main() {
	runtime.GOMAXPROCS(4)

	nsqEndChan := make(chan int)
	signalChan := make(chan os.Signal, 1)

	flag.Parse()

	// 分析 cpu 效率
	if *cpuProfile != "" {
		log.Printf("CPU Profiling Enabled")
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	err := settings.Load(*settingsFile, *environment)
	if err != nil {
		log.Fatal("settings.Load:", err)
	}

	go func() {
		<-signalChan
		nsqEndChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)

	// 开启四个协程。
	// 分别是：获取 topic、获取 uuid、
	// consumer 连接、消费者连接。
	go topicFactory(*memQueueSize)
	go uuidFactory()
	go tcpServer(*bindAddress, strconv.Itoa(*tcpPort))
	httpServer(*bindAddress, strconv.Itoa(*webPort), nsqEndChan)

	for _, topic := range topicMap {
		topic.Close()
	}
}
