package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	//"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		buffer := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buffer)
		var target T
		err := decoder.Decode(&target)
		return target, err
	},
	)
}

// helper func to serve JSON and GOB subscriptions
func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {

	// make sure the given queue exists and is bound to exchange
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Unable to Declare/Bind Queue: %s", err)
	}

	deliveries, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil, // Args
	)
	if err != nil {
		return fmt.Errorf("Unable to consume messages: %v", err)
	}

	// goroutine
	go func() {
		for delivery := range deliveries {

			// DEBUG PRINT
			fmt.Printf("body: %s", delivery.Body)
			// unmarshal the body of each message
			msg, err := unmarshaller(delivery.Body)
			if err != nil {
				fmt.Printf("can not unmarshal message body: %v\n", err)
				continue
			}
			/// DEBUG PRINT
			fmt.Printf("routing key: %v - message %s\n", delivery.RoutingKey, delivery.Body)

			// update this section to switch handler functions
			switch handler(msg) {
			case Ack:
				delivery.Ack(false)
			case NackRequeue:
				delivery.Nack(false, true)
			case NackDiscard:
				delivery.Nack(false, false)
			}
		}
	}()
	return nil
}

////////////////////////////////////////////
