package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	const rabbitCnxStr = "amqp://guest:guest@localhost:5672/"
	fmt.Println("Starting Peril Client")
	conn, err := amqp.Dial(rabbitCnxStr)
	if err != nil {
		log.Fatalf("unable to connect to rabbit server!")
	}
	defer conn.Close()
	fmt.Println("Peril Client Connected to RabbitMQ Server")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Unable to login as user: %v", err)
	}

	const exchange string = "peril_direct"
	var queueName string = fmt.Sprintf("pause.%s", userName)
	const routingKey string = "pause"
	var queueType pubsub.SimpleQueueType

	_, queue, err := pubsub.DeclareAndBind(conn, exchange, queueName, routingKey, queueType)
	if err != nil {
		log.Fatalf("can not subscribe to 'pause' %v", err)
	}

	fmt.Printf("Queue %v declared and bound\n", queue.Name)

	// wait for exit signal
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
	fmt.Println("RabbitMQ connection closed... Bye!")

}
