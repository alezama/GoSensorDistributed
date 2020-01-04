package main

import (
	"GoConcurrency/distributed/dto"
	"GoConcurrency/distributed/qutils"
	"bytes"
	"encoding/gob"
	"flag"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"strconv"
	"time"
)

var url = "amqp://guest:guest@localhost:5672"

var name = flag.String("name", "sensor", "name of the sensor")
var freq = flag.Uint("freq", 5, "update frequency in cycles per second")
var max = flag.Float64("max", 5., "maximum value of the generated readings")
var min = flag.Float64("min", 1., "minimum value of the generated readings")
var stepSize = flag.Float64("step", 0.1, "maximum allowable change of measurement")
var r = rand.New(rand.NewSource(time.Now().UnixNano()))
var value = r.Float64()*(*max-*min) + *min
var nom = (*max-*min)/2 + *min

func main() {
	flag.Parse()

	conn, ch := qutils.GetChannel(url)
	defer conn.Close()
	defer ch.Close()

	dataQueue := qutils.GetQueue(*name, ch, false)

	publishQueueName(ch)

	discoveryQueue := qutils.GetQueue("", ch, true)

	ch.QueueBind(discoveryQueue.Name,
		"",
		qutils.SensorDiscoveryExchange,
		false,
		nil)

	go listenForDiscoverRequest(discoveryQueue.Name, ch)

	dur, _ := time.ParseDuration(strconv.Itoa(1000/int(*freq)) + "ms")
	signal := time.Tick(dur)

	buf := new(bytes.Buffer)

	for range signal {
		buf.Truncate(0)
		enc := gob.NewEncoder(buf)
		calcValue()
		reading := dto.SensorMessage{
			Name:      *name,
			Value:     value,
			Timestamp: time.Now(),
		}
		enc.Encode(reading)

		msg := amqp.Publishing{
			Body: buf.Bytes(),
		}

		ch.Publish(
			"",
			dataQueue.Name,
			false,
			false,
			msg)

		log.Printf("Reading sent. Value %v\n", reading)
	}
}

func listenForDiscoverRequest(n string, ch *amqp.Channel) {
	msgs, _ := ch.Consume(n, "", true, false, false, false, nil)

	for range msgs {
		publishQueueName(ch)
	}
}

func publishQueueName(ch *amqp.Channel) {
	msgName := amqp.Publishing{Body: []byte(*name), ContentType: "text/plain"}

	ch.Publish("amq.fanout",
		"",
		false,
		false,
		msgName)
}

func calcValue() {
	var maxStep, minStep float64
	if value < nom {
		maxStep = *stepSize
		minStep = -1 * *stepSize * (value - *min) / (nom - *min)
	} else {
		maxStep = *stepSize * (*max - value) / (*max - nom)
		minStep = -1 * *stepSize
	}

	value += r.Float64()*(maxStep-minStep) + minStep
}
