package main

import (
	"encoding/json"
	"sync"
	"time"

	pb "nsq_benchmark/gen"
	"nsq_benchmark/pkg/core"

	"github.com/nsqio/go-nsq"
)

// B分支的修改

// 测试合并冲突
// 测试合并冲突1
// 测试合并冲突2
// 测试合并冲突3
func main() {
	TenProducerTenTopic400Msg(core.GobalCore.NsqList)
	//OneProducerOneTopic4000Msg(core.GobalCore.NsqList)
}

// 10个生产者，10个Topic，每个400个Msg
func TenProducerTenTopic400Msg(nsqList []*core.Nsq) {

	var producerList []*nsq.Producer
	for i := 0; i < len(nsqList); i++ {
		nsqConfig := nsq.NewConfig()
		producer, err := nsq.NewProducer(nsqList[i].ProducerAddr, nsqConfig)
		if err != nil {
			panic(err)
		}

		producerList = append(producerList, producer)
	}

	var wg sync.WaitGroup
	// 模拟十个生产者同时往里面塞数据
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(producer *nsq.Producer, topic string, index int) {
			defer wg.Done()
			for j := 0; j < 400; j++ {
				msg := &pb.MqMessage{
					Msg:     time.Now().String(),
					Index:   int32(j),
					IsAlive: false,
				}
				data, _ := json.Marshal(msg)
				err := producer.Publish(topic, data)
				if err != nil {
					panic(err)
				}
			}
		}(producerList[i], nsqList[i].Topic, i)
	}

	wg.Wait()
	msg := &pb.MqMessage{
		Msg:     time.Now().String(),
		IsAlive: true,
	}
	data, _ := json.Marshal(msg)
	// 往第一个塞可用的数据
	producerList[0].Publish(nsqList[0].Topic, data)
}

// 1个生产者，1个Topic,4000个Msg
func OneProducerOneTopic4000Msg(nsqList []*core.Nsq) {

	var producerList []*nsq.Producer
	for i := 0; i < len(nsqList); i++ {
		nsqConfig := nsq.NewConfig()
		producer, err := nsq.NewProducer(nsqList[i].ProducerAddr, nsqConfig)
		if err != nil {
			panic(err)
		}

		producerList = append(producerList, producer)
	}

	go func(producer *nsq.Producer, topic string) {
		for j := 0; j < 4000; j++ {
			msg := &pb.MqMessage{
				Msg:     time.Now().String(),
				Index:   int32(j),
				IsAlive: j == 4000,
			}
			data, _ := json.Marshal(msg)
			err := producer.Publish(topic, data)
			if err != nil {
				panic(err)
			}
		}
	}(producerList[0], nsqList[0].Topic)
}
