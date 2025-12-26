package core

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

var (
	GobalCore Core
)

// 主要是包含了NSQ的一些配置
type Core struct {
	NsqList []*Nsq `yaml:"nsq_list"`
}

// Nsq的配置
type Nsq struct {
	Topic        string `yaml:"topic"`         // topicName
	ProducerAddr string `yaml:"producer_addr"` // 生产者队列
}

// 从yaml文件中读取NSQ的配置
func init() {
	// 读取配置文件
	configFile, err := os.Open("../config/config.yaml")
	if err != nil {
		panic(err)
	}
	defer configFile.Close()

	// 解析yaml
	decoder := yaml.NewDecoder(configFile)
	if err := decoder.Decode(&GobalCore); err != nil {
		panic(err)
	}

	fmt.Println(GobalCore)

	return
}
