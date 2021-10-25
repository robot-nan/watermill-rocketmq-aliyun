package rocketmq_test

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	aliRocketSDK "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/robot-nan/watermill-rocketmq-aliyun/pkg/rocketmq"
	"testing"
	"time"
)

func TestCreatePubSub(t *testing.T) {
	client, err := rocketmq.NewPublisher(
		rocketmq.PublisherConfig{
			Endpoint:   "http://xxx.xxx.cn-beijing.aliyuncs.com",
			AccessKey:  "xxx",
			SecretKey:  "xxx",
			InstanceID: "xxx",
			Topic:      "test",
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		fmt.Println("err:", err)
	}
	msg := aliRocketSDK.PublishMessageRequest{
		MessageBody: "hello mq!", // 消息内容。
		MessageTag:  "test 1",    // 消息标签。
		//ShardingKey: 1, // 设置分区顺序消息的Sharding Key，用于标识不同的分区。Sharding Key与消息的Key是完全不同的概念。
		StartDeliverTime: (time.Now().UnixNano() / 1e6) + (10 * 1000),
		//Properties:  map[string]string{}, // 消息属性。
	}
	// 设置消息自定义属性。
	err = client.Publish("UserCreate", msg)
}
