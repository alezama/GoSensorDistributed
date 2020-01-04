package coordinator

import (
	"GoConcurrency/distributed/dto"
	"GoConcurrency/distributed/qutils"
	"bytes"
	"encoding/gob"
	"github.com/streadway/amqp"
	"log"
)

const url = "amqp://guest:guest@localhost:5672"

type QueueListener struct {
	conn            *amqp.Connection
	ch              *amqp.Channel
	sources         map[string]<-chan amqp.Delivery
	eventAggregator *EventAggregator
}

func NewQueueListener(ea *EventAggregator) *QueueListener {
	ql := QueueListener{
		sources:         make(map[string]<-chan amqp.Delivery),
		eventAggregator: ea,
	}

	ql.conn, ql.ch = qutils.GetChannel(url)

	return &ql
}

func (ql *QueueListener) ListenForNewSources() {
	q := qutils.GetQueue("", ql.ch, true)
	ql.ch.QueueBind(
		q.Name,
		"",
		"amq.fanout",
		false,
		nil)

	msgs, _ := ql.ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil)

	ql.DiscoverSensors()
	log.Println("listening for new sources")

	for msg := range msgs {
		log.Println("new source discovered")
		ql.eventAggregator.PublishEvent("DataSourceDiscover", string(msg.Body))
		sourceChan, _ := ql.ch.Consume(
			string(msg.Body),
			"",
			true,
			false,
			false,
			false,
			nil)

		if ql.sources[string(msg.Body)] == nil {
			ql.sources[string(msg.Body)] = sourceChan
			go ql.AddListener(sourceChan)
		}
	}
}

func (ql *QueueListener) AddListener(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		r := bytes.NewReader(msg.Body)
		d := gob.NewDecoder(r)
		sd := new(dto.SensorMessage)
		d.Decode(sd)
		log.Printf("Received message: %v\n", sd)

		ql.eventAggregator.PublishEvent("MessageReceived_"+msg.RoutingKey, EventData{
			Name:      sd.Name,
			Value:     sd.Value,
			Timestamp: sd.Timestamp,
		})
	}
}

func (ql *QueueListener) DiscoverSensors() {
	ql.ch.ExchangeDeclare(
		qutils.SensorDiscoveryExchange,
		"fanout",
		false,
		false,
		false,
		false,
		nil)

	ql.ch.Publish(
		qutils.SensorDiscoveryExchange,
		"",
		false,
		false,
		amqp.Publishing{})
}
