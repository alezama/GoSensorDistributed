package main

import (
	"GoConcurrency/distributed/datamanager"
	"GoConcurrency/distributed/dto"
	"GoConcurrency/distributed/qutils"
	"bytes"
	"encoding/gob"
	"log"
)

var url = "amqp://guest:guest@localhost:5672"

func main() {
	conn, ch := qutils.GetChannel(url)
	defer conn.Close()
	defer ch.Close()

	msgs, err := ch.Consume(qutils.PersistReadingQueue,
		"",
		false,
		true,
		false,
		false,
		nil)

	if err != nil {
		log.Fatal("Failed to get access to messages")
	}

	for msg := range msgs {
		buf := bytes.NewReader(msg.Body)
		dec := gob.NewDecoder(buf)
		sd := &dto.SensorMessage{}
		dec.Decode(sd)

		err := datamanager.SaveReader(sd)

		if err != nil {
			log.Println("Failed to save reading from sensor %v. Error %s", sd.Name, err.Error())
		} else {
			msg.Ack(false)
		}
	}
}
