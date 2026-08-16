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
	handler func(T) AckType,
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
		nil, // Args
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
			// update this section to switch handler functions
			switch handler(msg) {
			case Ack:
				delivery.Ack(false)
				fmt.Println("Acknowledge")
			case NackRequeue:
				delivery.Nack(false, true)
				fmt.Println("Nack Requeue")
			case NackDiscard:
				delivery.Nack(false, false)
				fmt.Println("Nack Discard")
			}

		}
	}()
	return nil
}
