package benchmark

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type KafkaBenchmark struct {
	payloadSize       uint
	urls              []string
	topic             string
	recv              chan []byte
	producer          sarama.AsyncProducer
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	msg               *sarama.ProducerMessage
	errors            uint
}

func NewKafkaBenchmark(urls []string, topic string, payloadSize uint) *KafkaBenchmark {
	return &KafkaBenchmark{
		payloadSize: payloadSize,
		urls:        urls,
		topic:       topic,
		recv:        make(chan []byte, 65536),
	}
}

func (k *KafkaBenchmark) Setup(consumer bool) error {
	if consumer {
		return k.setupConsumer()
	}
	return k.setupProducer()

}

func (k *KafkaBenchmark) setupProducer() error {
	config := sarama.NewConfig()
	producer, err := sarama.NewAsyncProducer(k.urls, config)
	if err != nil {
		return err
	}
	k.producer = producer
	k.msg = &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.ByteEncoder(make([]byte, k.payloadSize)),
	}

	go func() {
		for err := range k.producer.Errors() {
			fmt.Println(err)
			k.errors++
		}
	}()

	return nil
}

func (k *KafkaBenchmark) setupConsumer() error {
	consumer, err := sarama.NewConsumer(k.urls, nil)
	if err != nil {
		return err
	}
	partitionConsumer, err := consumer.ConsumePartition(k.topic, 0, sarama.OffsetNewest)
	if err != nil {
		consumer.Close()
		return err
	}

	k.consumer = consumer
	k.partitionConsumer = partitionConsumer

	go func() {
		for msg := range k.partitionConsumer.Messages() {
			k.recv <- msg.Value
		}
	}()

	go func() {
		for err := range k.partitionConsumer.Errors() {
			fmt.Println(err)
			k.errors++
		}
	}()

	return nil
}

func (k *KafkaBenchmark) Send() error {
	k.producer.Input() <- k.msg
	return nil
}

func (k *KafkaBenchmark) Recv() <-chan []byte {
	return k.recv
}

func (k *KafkaBenchmark) Errors() uint {
	return k.errors
}
