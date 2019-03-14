package instance

import (
	"context"
	"time"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/log"
)

type LogAppender struct {
	PulsarClient pulsar.Client
	LogTopic     string
	Fqn          string
	Producer     pulsar.Producer
}

func NewLogAppender(client pulsar.Client, logTopic, fqn string) *LogAppender {
	logAppender := &LogAppender{
		PulsarClient: client,
		LogTopic:     logTopic,
		Fqn:          fqn,
	}
	return logAppender
}

func (la *LogAppender) Start() {
	producer, err := la.PulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic:                   la.LogTopic,
		BlockIfQueueFull:        false,
		Batching:                true,
		CompressionType:         pulsar.LZ4,
		BatchingMaxPublishDelay: 100 * time.Millisecond,
		Properties: map[string]string{
			"function": la.Fqn,
		},
	})
	if err != nil {
		log.Errorf("create producer error:%s", err.Error())
	}
	la.Producer = producer
}

func (la *LogAppender) Append(logbyte []byte) {
	ctx := context.Background()
	asyncMsg := pulsar.ProducerMessage{
		Payload: logbyte,
	}
	la.Producer.SendAsync(ctx, asyncMsg, func(msg pulsar.ProducerMessage, err error) {
		if err != nil {
			log.Fatal(err)
		}
	})
}

func (la *LogAppender) GetName() string {
	return la.Fqn
}

func (la *LogAppender) Stop() {
	err := la.Producer.Close()
	if err != nil {
		log.Errorf("close log append error:%s", err.Error())
	}
	la.Producer = nil
}
