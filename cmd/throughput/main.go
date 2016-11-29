package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/tylertreat/log-benchmarking/cmd/throughput/benchmark"
)

type Benchmark interface {
	Setup(consumer bool, numMsgs uint) error
	Send() error
	Recv() <-chan []byte
	Errors() uint
	SendDone() <-chan bool
}

func main() {
	var (
		system      = flag.String("s", "", "[kafka, nats]")
		size        = flag.Uint("sz", 200, "message size")
		numMessages = flag.Uint("n", 50000, "number of messages")
		url         = flag.String("url", "", "broker url")
		consumer    = flag.Bool("consumer", false, "Consumer or producer")
	)
	flag.Parse()

	topic := "benchmark"

	var b Benchmark
	switch *system {
	case "kafka":
		b = benchmark.NewKafkaBenchmark([]string{*url}, topic, *size)
	case "nats":
		b = benchmark.NewNATSBenchmark(*url, topic, *size)
	}

	var err error
	if *consumer {
		err = runConsumer(b, *numMessages)
	} else {
		err = runProducer(b, *numMessages)
	}

	if err != nil {
		fmt.Println("An error occurred", err)
		return
	}

	if b.Errors() > 0 {
		fmt.Printf("%d errors occurred\n", b.Errors())
	}
	time.Sleep(time.Hour)
}

func runConsumer(b Benchmark, numMessages uint) error {
	if err := b.Setup(true, numMessages); err != nil {
		return err
	}
	fmt.Println("Running consumer...")

	var (
		recv  = uint(0)
		start time.Time
	)
	for recv < numMessages {
		<-b.Recv()
		if start.IsZero() {
			start = time.Now()
		}
		recv++
	}
	dur := time.Since(start)

	fmt.Printf("Recv: %d\n", recv)
	fmt.Printf("Elapsed: %s\n", dur)
	fmt.Printf("Recv Throughput: %f\n", float64(recv)/dur.Seconds())
	return nil
}

func runProducer(b Benchmark, numMessages uint) error {
	if err := b.Setup(false, numMessages); err != nil {
		return err
	}
	fmt.Println("Running producer...")

	var (
		sent  = uint(0)
		start = time.Now()
	)

	for sent < numMessages {
		if err := b.Send(); err != nil {
			return err
		}
		sent++
	}
	<-b.SendDone()
	dur := time.Since(start)

	fmt.Printf("Sent: %d\n", sent)
	fmt.Printf("Elapsed: %s\n", dur)
	fmt.Printf("Send Throughput: %f\n", float64(sent)/dur.Seconds())
	return nil
}
