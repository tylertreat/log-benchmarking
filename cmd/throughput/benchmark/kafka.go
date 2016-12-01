package benchmark

import (
	"fmt"
	"math/rand"

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
	acked             uint
	numMsgs           uint
	sendDone          chan bool
}

func NewKafkaBenchmark(urls []string, topic string, payloadSize uint) *KafkaBenchmark {
	return &KafkaBenchmark{
		payloadSize: payloadSize,
		urls:        urls,
		topic:       topic,
		recv:        make(chan []byte, 65536),
		sendDone:    make(chan bool),
	}
}

func (k *KafkaBenchmark) Setup(consumer bool, numMsgs uint) error {
	k.numMsgs = numMsgs
	if consumer {
		return k.setupConsumer()
	}
	return k.setupProducer()

}

func (k *KafkaBenchmark) setupProducer() error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = 1
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer(k.urls, config)
	if err != nil {
		return err
	}
	k.producer = producer
	msg := make([]byte, k.payloadSize)
	for i := 0; i < int(k.payloadSize); i++ {
		msg[i] = 'A' + uint8(rand.Intn(26))
	}
	k.msg = &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.ByteEncoder(msg),
	}

	go func() {
		for err := range k.producer.Errors() {
			fmt.Println(err)
			k.errors++
		}
	}()

	go func() {
		for _ = range k.producer.Successes() {
			k.acked++
			if k.acked == k.numMsgs {
				k.sendDone <- true
			}
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

func (k *KafkaBenchmark) SendDone() <-chan bool {
	return k.sendDone
}
