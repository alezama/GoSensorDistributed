package qutils

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

const SensorListQueue = "SensorList"
const SensorDiscoveryExchange = "SensorDiscovery"
const PersistReadingQueue = "PersistReading"

func GetChannel(url string) (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(url)
	panicOnFail(err, "Failed obtaining the connection")
	cha, err := conn.Channel()
	panicOnFail(err, "Failed on obtaining the channel")

	return conn, cha
}

func GetQueue(name string, ch *amqp.Channel, autoDelete bool) *amqp.Queue {
	q, err := ch.QueueDeclare(
		name,
		false,
		autoDelete,
		false,
		false,
		nil)
	panicOnFail(err, "Failed on getting the queue")
	return &q
}

func panicOnFail(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
