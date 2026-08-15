package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {

	// make sure the given queue exists and is bound to exchange
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Unable to Declare/Bind: %s", err)
	}

	deliveries, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// define goroutine to range over channel of deliveries
	unmarsh := func(data []byte) (T, error) {
		var holder T
		err := json.Unmarshal(data, &holder)
		return holder, err
	}

	go func() {
		for delivery := range deliveries {
			// unmarshal the body of each message
			msg, err := unmarsh(delivery.Body)
			if err != nil {
				fmt.Printf("can not unmarshal message body")
				continue
			}
			// call the handler function with unmarshaled message
			handler(msg)

			// acknowledge message to remove from queue (it blocks) - delivery.Ack(false)
			err = delivery.Ack(false)
			if err != nil {
				fmt.Printf("unable to acknowledge! this will block communication..")
				continue
			}
		}
	}()
	return nil
}
