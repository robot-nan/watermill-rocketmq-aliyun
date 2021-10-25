package rocketmq

import (
	"github.com/ThreeDotsLabs/watermill"
	aliRocketSDK "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/pkg/errors"
)

type Publisher struct {
	config   PublisherConfig
	producer aliRocketSDK.MQProducer
	logger   watermill.LoggerAdapter
	closed   bool
}

func NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*Publisher, error) {
	client := aliRocketSDK.NewAliyunMQClient(
		config.Endpoint,
		config.AccessKey,
		config.SecretKey,
		config.Token,
	)
	publisher := client.GetProducer(config.InstanceID, config.Topic)

	return &Publisher{
		config:   config,
		producer: publisher,
		logger:   logger,
	}, nil
}

// PublisherConfig the rocketmq publisher config
type PublisherConfig struct {
	Endpoint   string
	AccessKey  string
	SecretKey  string
	Token      string
	InstanceID string
	Topic      string
}

func (p *Publisher) Publish(key string, msg aliRocketSDK.PublishMessageRequest) (err error) {
	if p.closed {
		return errors.New("publisher closed")
	}
	msg.MessageKey = key
	_, err = p.producer.PublishMessage(msg)
	if err != nil {
		return err
	}
	return nil
}

// Close closes the publisher
func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	return nil
}
