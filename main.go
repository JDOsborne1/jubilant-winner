package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

var docstring string = "This is a simulator, using Kafka as a collection of marketplaces to simulate trade in a virtual world"

type Closer interface {
	Close() error
}

func close_or_die(_input Closer) {
	if err := _input.Close(); err != nil {
		log.Panicf("Failed to close closer of type %T, with error %v", _input, err)
	}
}

func write_test_content(_topic string, _partition int) {

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", _topic, _partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	// Defer cleanup and logging issues
	defer close_or_die(conn)

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
	)

	if err != nil {
		log.Fatal("failed to write messages: ", err)
	}

}

func main() {
	fmt.Println(docstring)
	topic := "test"
	partition := 0

	write_test_content(topic, partition)

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader: ", err)
	}
	defer close_or_die(conn)

	conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	batch := conn.ReadBatch(10e3, 10e6)
	defer close_or_die(batch)

	b := make([]byte, 10e3)
	counter := 0
	max := 4
	for {

		counter++
		fmt.Println("batch: " + fmt.Sprint(counter) + " / " + fmt.Sprint(max))
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b[:n]))

		if counter >= max {
			break
		}
	}

}
