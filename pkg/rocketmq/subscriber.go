package rocketmq

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	aliRocketSDK "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/gogap/errors"
	"strings"
	"sync"
	"time"
)

type Subscriber struct {
	config        SubscriberConfig
	logger        watermill.LoggerAdapter
	closing       chan struct{}
	consumer      aliRocketSDK.MQConsumer
	subscribersWg sync.WaitGroup
	closed        bool
}

func NewSubscriber(
	config SubscriberConfig,
	logger watermill.LoggerAdapter,
) *Subscriber {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	logger = logger.With(watermill.LogFields{
		"subscriber_uuid": watermill.NewShortUUID(),
	})

	client := aliRocketSDK.NewAliyunMQClient(
		config.Endpoint,
		config.AccessKey,
		config.SecretKey,
		config.Token,
	)
	consumer := client.GetConsumer(config.InstanceID, config.Topic, config.GroupID, config.Token)
	return &Subscriber{
		config:   config,
		logger:   logger,
		consumer: consumer,
		closing:  make(chan struct{}),
	}
}

type SubscriberConfig struct {
	Endpoint            string
	AccessKey           string
	SecretKey           string
	Token               string
	InstanceID          string
	GroupID             string
	Topic               string
	ReconnectRetrySleep time.Duration
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	output := make(chan *message.Message)
	logFields := watermill.LogFields{
		"provider":            "kafka",
		"topic":               topic,
		"consumer_group":      s.config.GroupID,
		"kafka_consumer_uuid": watermill.NewShortUUID(),
	}
	s.subscribersWg.Add(1)
	go func() {
		for {
			endChan := make(chan int)
			respChan := make(chan aliRocketSDK.ConsumeMessageResponse)
			errChan := make(chan error)
			go func() {
				select {
				case resp := <-respChan:
					{
						for _, v := range resp.Messages {
							err := s.processMessage(ctx, output, v, logFields)
							if err != nil {
								s.logger.Error("Cannot reconnect messages consumer", err, logFields)
								return
							}
						}
						endChan <- 1
					}
				case err := <-errChan:
					{
						// Topic中没有消息可消费。
						if !strings.Contains(err.(errors.ErrCode).Error(), "MessageNotExist") {								s.logger.Error("Cannot reconnect messages consumer", err, logFields)
							s.logger.Error("no message error:", err, logFields)
							time.Sleep(time.Duration(5) * time.Second)
						}
						endChan <- 1
					}

				case <-time.After(35 * time.Second):
					{
						fmt.Println("Timeout of consumer message ??")
						endChan <- 1
					}
				}
			}()

			s.consumer.ConsumeMessage(respChan, errChan,
				3, // 一次最多消费3条（最多可设置为16条）。
				3, // 长轮询时间3s（最多可设置为30s）。
			)
			<-endChan
		}
	}()

	return output, nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()

	s.logger.Debug("rocketmq subscriber closed", nil)

	return nil
}

func (s *Subscriber) processMessage(
	ctx context.Context,
	output chan *message.Message,
	rocketMqMsg aliRocketSDK.ConsumeMessageEntry,
	logFields watermill.LogFields,
) (err error) {
	msg := message.NewMessage(rocketMqMsg.MessageId, message.Payload(rocketMqMsg.MessageBody))
	metaData := map[string]string{
		"MessageKey": rocketMqMsg.MessageKey,
		"MessageTag": rocketMqMsg.MessageTag,
	}
	msg.Metadata = metaData
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()
ResendLoop:
	for {
		select {
		case output <- msg:
			fmt.Println("Message sent to consumer")
		case <-s.closing:
			fmt.Println("Closing, message discarded")
			return nil
		case <-ctx.Done():
			fmt.Println("Closing, ctx cancelled before sent to consumer")
			return nil
		}
		select {
		case <-msg.Acked():
			err := s.consumer.AckMessage([]string{rocketMqMsg.ReceiptHandle})
			if err != nil {
				s.logger.Info("AckMessage error:", logFields.Add(watermill.LogFields{"err": err.Error()}))
				return err
			}
			break ResendLoop
		case <-msg.Nacked():
			msg = msg.Copy()
			continue ResendLoop
		case <-s.closing:
			fmt.Println("Closing, message discarded before ack")
			return nil
		case <-ctx.Done():
			fmt.Println("close")
			return nil
		}
	}

	return nil
}
