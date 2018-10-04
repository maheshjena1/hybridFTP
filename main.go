package main

import (
	"log"
	"os"
	"os/signal"
)

func main() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go startListen()
	var rcv receiver
	go (&rcv).StartFileStreaming()

	log.Println("Kill Signal received", <-signalCh)
}
