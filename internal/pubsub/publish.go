package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// reusable code for rabbitMQ interaction (for server and client)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
}

type Ack int

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	// create a new channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("unable to create channel: %v", err)
	}
	// determine params for queue
	var durable bool = false
	var autoDelete bool = false
	var exclusive bool = false
	var noWait bool = false
	argys := amqp.Table{}

	switch queueType {
	case 1:
		durable = true
	case 0:
		autoDelete = true
		exclusive = true
	default:
		log.Fatal("invalid queue type")
	}

	// declare queue
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, argys)
	if err != nil {
		return nil, queue, err
	}

	//bind the queue to the exchange
	err = ch.QueueBind(queueName, key, exchange, noWait, nil)
	if err != nil {
		return nil, queue, err
	}
	return ch, queue, nil

}
