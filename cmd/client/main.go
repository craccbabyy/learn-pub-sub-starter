package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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

	// need to create a channel for MOVEs
	moveChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("unable to create amqp channel for MOVES: %s", err)
	}
	// and a channel for WAR
	warChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("unable to create amqp channel for WAR: %s", err)
	}
	// and one for LOGS!
	logChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("unable to create amqp channel for LOGS: %s", err)
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
	// MOVE queue and handling
	// each game client need to subscribe to other player's moves before the REPL starts
	movesKey := "army_moves.*"
	movesQueue := routing.ArmyMovesPrefix + "." + userName
	movesExchange := "peril_topic" // these vals should be moved to .env
	err = pubsub.SubscribeJSON(
		conn,
		movesExchange,
		movesQueue,
		movesKey,
		pubsub.Transient,
		handlerMove(gameState, moveChan)) // pass the channel for war recognition
	if err != nil {
		log.Fatalf("can not subscribe to the 'moves' exchange: %v", err)
	}

	// WAR QUEUE/HANDLING
	warKey := routing.WarRecognitionsPrefix + ".*"
	err = pubsub.SubscribeJSON(
		conn,
		"peril_topic",
		"war",
		warKey,
		pubsub.Durable,
		handlerWar(gameState, warChan))
	if err != nil {
		log.Fatalf("unable to subscribe to war queue: %v", err)
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
			err = pubsub.PublishJSON(moveChan, movesExchange, userMoveKey, mover)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("%v move: %v successfully published\n", userName, mover)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(userInput) < 2 {
				fmt.Printf("usage: %v <iterations>", userInput[0])
				continue
			}
			n, err := strconv.Atoi(userInput[1])
			if err != nil {
				fmt.Printf("usage: %v <iterations>", userInput[0])
			}
			for i := 0; i < n; i++ {
				mal := gamelogic.GetMaliciousLog()
				spamKey := routing.GameLogSlug + "." + userName

				// need a routing.GameLog struct to pass, NOT a string (can't GOB a string apparently)
				logSpam := routing.GameLog{
					CurrentTime: time.Now(),
					Message:     mal,
					Username:    userName,
				}

				err = pubsub.PublishGob(logChan, routing.ExchangePerilTopic, spamKey, logSpam)
				if err != nil {
					fmt.Printf("error publishing log: %v\n", err)
					continue
				}
				fmt.Println("MALICIOUS LOG PUBLISHED")

			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("invalid command")
		}

	}

}

// moved this here to easily access the user name
func PublishGameLog(ch *amqp.Channel, username, msg string) error {
	var gameLog = routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    username,
	}
	key := routing.GameLogSlug + "." + username
	exch := routing.ExchangePerilTopic
	return pubsub.PublishGob(ch, exch, key, gameLog)
}
