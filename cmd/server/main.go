package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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

	// show commands the REPL user can use
	gamelogic.PrintServerHelp()
loop:
	for {
		// wait for a []words from user
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		// get first word and check it
		firstWord := words[0]

		switch firstWord {
		case "pause":
			log.Printf("sending a 'pause' message")
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("can not publish time: %v", err)
			}
			fmt.Println("Pause Message Sent!")
		case "resume":
			log.Printf("sending a 'resume' message")
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("can not publish time: %v", err)
			}
			fmt.Println("Resume Message Sent!")
		case "quit":
			log.Printf("exiting... goodbye!")
			break loop
		default:
			log.Printf("unknown command, try again")
		}
	}
}
