package benchmark

import (
	"fmt"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
)

type NATSBenchmark struct {
	url         string
	payloadSize uint
	subject     string
	recv        chan []byte
	errors      uint
	conn        stan.Conn
	sub         stan.Subscription
	msg         []byte
	acked       uint
	numMsgs     uint
	sendDone    chan bool
}

func NewNATSBenchmark(url, subject string, payloadSize uint) *NATSBenchmark {
	return &NATSBenchmark{
		payloadSize: payloadSize,
		url:         url,
		subject:     subject,
		recv:        make(chan []byte, 65536),
		sendDone:    make(chan bool),
	}
}

func (n *NATSBenchmark) Setup(consumer bool, numMsgs uint) error {
	n.numMsgs = numMsgs
	if consumer {
		return n.setupConsumer()
	}
	return n.setupProducer()
}

func (n *NATSBenchmark) setupConsumer() error {
	conn, err := stan.Connect("test-cluster", "consumer", stan.NatsURL(n.url))
	if err != nil {
		return err
	}
	sub, err := conn.Subscribe(n.subject, func(msg *stan.Msg) {
		n.recv <- msg.Data
	})
	if err != nil {
		return err
	}
	n.conn = conn
	conn.NatsConn().SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		fmt.Println(err)
		n.errors++
	})
	n.sub = sub
	return nil
}

func (n *NATSBenchmark) setupProducer() error {
	conn, err := stan.Connect("test-cluster", "producer", stan.NatsURL(n.url), stan.MaxPubAcksInflight(1024))
	if err != nil {
		return err
	}
	n.conn = conn
	conn.NatsConn().SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		fmt.Println(err)
		n.errors++
	})
	n.msg = make([]byte, n.payloadSize)
	return nil
}

func (n *NATSBenchmark) Send() error {
	_, err := n.conn.PublishAsync(n.subject, n.msg, func(id string, err error) {
		if err != nil {
			fmt.Println(err)
			n.errors++
		}
		n.acked++
		if n.acked == n.numMsgs {
			n.sendDone <- true
		}
	})
	return err
}

func (n *NATSBenchmark) Recv() <-chan []byte {
	return n.recv
}

func (n *NATSBenchmark) Errors() uint {
	return n.errors
}

func (n *NATSBenchmark) SendDone() <-chan bool {
	return n.sendDone
}
