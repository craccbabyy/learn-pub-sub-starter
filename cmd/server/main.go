package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	//os.Getenv()
	// declare connection string
	const rabbitCnxStr = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitCnxStr)
	if err != nil {
		log.Fatalf("unable to connect to rabbit server!")
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ Server")

	// create connection to channel
	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("can not create channel: %v", err)
	}

	err = pubsub.PublishJSON(publishCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		log.Fatalf("can not publish JSON: %v", err)
	}
	fmt.Println("Pause Message Sent!")

}
