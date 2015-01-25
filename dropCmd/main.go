package main

import (
	"drop/drop"
	"flag"
	"fmt"
	"net/http"
)

const (
	defaultAddress    = ":6000"
	defaultCapacity   = 2000
	defaultMessageDir = "/var/go-drop/messages"
)

var (
	address    = flag.String("address", defaultAddress, "TCP address this server should listen on")
	capacity   = flag.Int("capacity", defaultCapacity, "Capacity of drop message ring storage")
	messageDir = flag.String("msgdir", defaultMessageDir, "Location of the message ring storage")
	verbose    = flag.Bool("verbose", false, "Verbose output")
)

func main() {
	flag.Parse()
	if *verbose {
		fmt.Printf("Serve drop on %v from directory %v with capacity %v\n", *address, *messageDir, *capacity)
	}
	http.Handle("/", drop.DropServer(*capacity, *messageDir))
	http.ListenAndServe(*address, nil)
}
