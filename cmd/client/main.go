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

	const rabbitCnxStr = "amqp://guest:guest@localhost:5672/"
	fmt.Println("Starting Peril Client")
	conn, err := amqp.Dial(rabbitCnxStr)
	if err != nil {
		log.Fatalf("unable to connect to rabbit server!")
	}
	defer conn.Close()
	fmt.Println("Peril Client Connected to RabbitMQ Server")

	// need to create a channel on the new connection
	newChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("unable to create amqp channel needed: %s", err)
	}

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Unable to login as user: %v", err)
	}

	queueName := routing.PauseKey + "." + userName
	gameState := gamelogic.NewGameState(userName)
	// sub to the main queue
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("can not subscribe to 'pause' %v", err)
	}

	// each game client need to subscribe to other player's moves before the REPL starts
	movesKey := "army_moves.*"
	movesQueue := routing.ArmyMovesPrefix + "." + userName
	movesExchange := "peril_topic" // these vals should be moved to .env
	err = pubsub.SubscribeJSON(conn, movesExchange, movesQueue, movesKey, pubsub.Transient, func(move gamelogic.ArmyMove) {
		_ = gameState.HandleMove(move)
		fmt.Print("> ")
	})
	if err != nil {
		log.Fatalf("can not subscribe to the 'moves' exchange: %v", err)
	}

	// REPL
	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}
		switch userInput[0] {
		case "spawn":
			err = gameState.CommandSpawn(userInput)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			mover, err := gameState.CommandMove(userInput)
			if err != nil {
				fmt.Println(err)
				continue
			} // publish the move to peril_topic exchange with routing key set to 'army_moves.username'
			userMoveKey := routing.ArmyMovesPrefix + "." + userName
			err = pubsub.PublishJSON(newChan, movesExchange, userMoveKey, mover)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("%v move: %v successfully published\n", userName, mover)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("invalid command")
		}

	}

}
