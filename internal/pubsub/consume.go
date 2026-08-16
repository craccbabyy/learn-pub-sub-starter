package pubsub

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

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

	// configure queue parameters
	table := amqp.Table{}
	table["x-dead-letter-exchange"] = "peril_dlx"

	// declare queue
	queue, err := ch.QueueDeclare(
		queueName,
		queueType == Durable, //durable t/f
		queueType != Durable, // if durable true, autoDelete must be false
		queueType != Durable, // if durable true, exclusive must be false
		false,                // noWait
		table,                // args amqp.Table
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	//bind the queue to the exchange
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, queue, nil

}
