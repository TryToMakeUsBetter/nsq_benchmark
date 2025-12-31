package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "nsq_benchmark/gen"
	"nsq_benchmark/pkg/core"

	"github.com/nsqio/go-nsq"
)

type myMessageHandler struct{}

// HandleMessage implements the Handler interface.
func (h *myMessageHandler) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		// In this case, a message with an empty body is simply ignored/discarded.
		return nil
	}

	// do whatever actual message processing is desired
	err := processMessage(m.Body)

	// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
	return err
}

func processMessage(body []byte) error {
	fmt.Println(string(body))
	// 将body反序列化
	// 假设body是一个JSON格式的字符串，包含一个字段"msg"和"is_alive"

	var msg pb.MqMessage
	json.Unmarshal(body, &msg)
	fmt.Printf("Process message: %s,Index: %d, IsAlive: %v\n", time.Now().String(), msg.Index, msg.IsAlive)
	if !msg.IsAlive {
		return fmt.Errorf("message is not alive, re-queuing")
	} else {
		fmt.Println("Message is alive, processing complete.")
	}
	return nil
}

func main() {
	nsqList := core.GobalCore.NsqList
	nsqConfig := nsq.NewConfig()
	nsqCC := nsqList[0]
	consumer, err := nsq.NewConsumer(nsqCC.Topic, "channel", nsqConfig)
	if err != nil {
		panic(err)
	}

	consumer.AddHandler(&myMessageHandler{})

	// Use nsqlookupd to discover nsqd instances.
	// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err = consumer.ConnectToNSQLookupd("localhost:4161")
	if err != nil {
		log.Fatal(err)
	}

	// wait for signal to exit
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Gracefully stop the consumer.
	consumer.Stop()
}
