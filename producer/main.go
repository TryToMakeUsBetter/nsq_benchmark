package main

import (
	"encoding/json"
	"sync"
	"time"

	pb "nsq_benchmark/gen"
	"nsq_benchmark/pkg/core"

	"github.com/nsqio/go-nsq"
)

// 10
// 9
// 8
// 7

// 1
// 2
// 3
// 4
// 6
func main() {
	//TenProducerTenTopic400Msg(core.GobalCore.NsqList)
	OneProducerOneTopic(core.GobalCore.NsqList)
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
func OneProducerOneTopic(nsqList []*core.Nsq) {

	var producerList []*nsq.Producer
	for i := 0; i < len(nsqList); i++ {
		nsqConfig := nsq.NewConfig()
		producer, err := nsq.NewProducer(nsqList[i].ProducerAddr, nsqConfig)
		if err != nil {
			panic(err)
		}

		producerList = append(producerList, producer)
	}
	producer := producerList[0]
	topic := nsqList[0].Topic

	for j := 0; j < 100; j++ {
		isAlive := true
		if (j >= 7 && j <= 11) || j == 15 || j == 16 {
			isAlive = false
		}
		msg := &pb.MqMessage{
			Msg:     time.Now().String(),
			Index:   int32(j),
			IsAlive: isAlive,
		}
		data, _ := json.Marshal(msg)
		err := producer.Publish(topic, data)
		if err != nil {
			panic(err)
		}
	}

}
