package main

import (
	"GoConcurrency/distributed/coordinator"
	"fmt"
)

var dc *coordinator.DatabaseConsumer

func main() {
	ea := coordinator.NewEventAggregator()
	dc = coordinator.NewDatabaseConsumer(ea)
	ql := coordinator.NewQueueListener(ea)
	go ql.ListenForNewSources()

	var a string
	fmt.Scanln(&a)
}
