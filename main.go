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
		kafka.Message{Value: []byte("three!")},
		kafka.Message{Value: []byte("four!")},
		kafka.Message{Value: []byte("five!")},
		kafka.Message{Value: []byte("six!")},
	)

	if err != nil {
		log.Fatal("failed to write messages: ", err)
	}

}

func main() {
	fmt.Println(docstring)
	topic := "test"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader: ", err)
	}
	defer close_or_die(conn)

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	_, err = conn.Seek(0, kafka.SeekEnd)

	if err != nil {
		log.Fatal("failed to seek latest message with: ", err)
	}

	write_test_content(topic, partition)

	counter := 0
	max := 10
	for {

		last, err := conn.ReadLastOffset()
		if err != nil {
			log.Fatal(err)
		}

		off, _ := conn.Offset()
		if off == last {
			fmt.Println("Reached end of topic")
			break
		}

		counter++
		fmt.Println("batch: " + fmt.Sprint(counter) + " / " + fmt.Sprint(max))

		msg, err := conn.ReadMessage(10)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Key: %v, Value: %v \n", string(msg.Key), string(msg.Value))

		if counter >= max {
			break
		}
	}

}
