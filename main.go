package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

var docstring string = "This is a simulator, using Kafka as a collection of marketplaces to simulate trade in a virtual world"

func close_or_die(_input io.Closer) {
	if err := _input.Close(); err != nil {
		log.Panicf("Failed to close closer of type %T, with error %v", _input, err)
	}
}

func write_test_content(_connection io.Writer) error {

	_, err := _connection.Write([]byte("one!"))
	_, err = _connection.Write([]byte("two!"))
	_, err = _connection.Write([]byte("three!"))
	_, err = _connection.Write([]byte("four!"))
	_, err = _connection.Write([]byte("five!"))
	_, err = _connection.Write([]byte("six!"))

	if err != nil {
		return fmt.Errorf("failed to write messages: %w", err)
	}

	return nil
}

func read_until_done(_connection io.Reader) error {

	conn := _connection
	counter := 0
	max := 10
	for {
		counter++
		fmt.Println("batch: " + fmt.Sprint(counter) + " / " + fmt.Sprint(max))

		buf := make([]byte, 10)
		_, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.ErrShortBuffer) {
				fmt.Println("Buffer too short, try a longer one")
			}

			if err == kafka.RequestTimedOut {
				fmt.Println("Request timed out, likely no more messages to parse")
				return nil
			}

			return fmt.Errorf("Unable to read to the buffer, with error %w", err)
		}

		fmt.Println(string(buf))

		if counter >= max {
			break
		}
	}

	return nil

}

func generate_from_now_connection(_topic string, _partition int) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", _topic, _partition)
	if err != nil {
		return &kafka.Conn{}, fmt.Errorf("failed to dial leader: %w ", err)
	}

	conn.SetDeadline(time.Now().Add(1 * time.Second))

	_, err = conn.Seek(0, kafka.SeekEnd)

	if err != nil {
		return &kafka.Conn{}, fmt.Errorf("failed to seek latest message with: %w", err)
	}

	return conn, nil

}

func main() {
	fmt.Println(docstring)
	topic := "test"
	partition := 0

	conn, err := generate_from_now_connection(topic, partition)
	defer close_or_die(conn)
	if err != nil {
		log.Fatal(err)
	}

	err = write_test_content(conn)
	if err != nil {
		log.Fatal(err)
	}

	err = read_until_done(conn)
	if err != nil {
		log.Fatal(err)
	}

}
