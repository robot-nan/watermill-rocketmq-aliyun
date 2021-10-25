package rocketmq_test

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/robot-nan/watermill-rocketmq-aliyun/pkg/rocketmq"
	"testing"
	"time"
)

func Test(t *testing.T) {
	logger := watermill.NewStdLogger(false, false)
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		fmt.Println(err)
	}
	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(
		middleware.CorrelationID,
		middleware.Retry{
			MaxRetries:      3,
			InitialInterval: time.Millisecond * 100,
			Logger:          logger,
		}.Middleware,
		middleware.Recoverer,
	)
	subscriber := rocketmq.NewSubscriber(
		rocketmq.SubscriberConfig{
			Endpoint:   "http://xxx.xxx.cn-beijing.aliyuncs.com",
			AccessKey:  "xxx",
			SecretKey:  "xxx",
			InstanceID: "xxx",
			Topic:      "test",
			GroupID:    "test",
		},
		watermill.NewStdLogger(false, false),
	)

	router.AddNoPublisherHandler("test", "test", subscriber, process)
	err = router.Run(context.Background())
	if err != nil {
		fmt.Println(err)
	}

}

func process(msg *message.Message) (err error) {
	fmt.Println("牛逼了:", msg)
	msg.Ack()
	return nil
}
