package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

func main() {
	go client()
	go server()

	var a string
	fmt.Scanln(&a)
}

func client() {
	conn, ch, q := getQueue()
	defer conn.Close()
	defer ch.Close()
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to load message")
	for msg := range msgs {
		log.Printf("Received message msg: %s", msg.Body)
	}
}

func server() {
	conn, ch, q := getQueue()
	defer conn.Close()
	defer ch.Close()

	msg := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("Hello to RabbitMQ"),
	}
	for i := 0; i < 1000000; i++ {
		ch.Publish("", q.Name, false, false, msg)
	}

}

func getQueue() (*amqp.Connection, *amqp.Channel, *amqp.Queue) {
	var err error
	conn, err := amqp.Dial("amqp://guest@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open the channel")
	q, err := ch.QueueDeclare("hello",
		false,
		false,
		false,
		false,
		nil)
	failOnError(err, "Failed to declare the queue")
	return conn, ch, &q
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatal("%s: %s", msg, err)
		panic(fmt.Sprintf("%s:%s", msg, err))
	}
}
