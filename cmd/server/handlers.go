package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLogs() func(gameLog routing.GameLog) pubsub.AckType {
	return func(gameLog routing.GameLog) pubsub.AckType {
		defer fmt.Println("> ")
		err := gamelogic.WriteLog(gameLog)
		if err != nil {
			fmt.Printf("unable to write log: %v\n", err)
			return pubsub.NackRequeue
		}

		// DEBUG PRINT
		fmt.Println("LOG WRITTEN SUCCESSFULLY!!")
		return pubsub.Ack
	}

}
