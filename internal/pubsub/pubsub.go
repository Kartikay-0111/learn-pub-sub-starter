package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"bytes"
	"encoding/gob"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)
	
	return err
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return err
	}
	
	err = ch.PublishWithContext(	
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		},
	)
	
	return err
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	qu, err := ch.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	err = ch.QueueBind(qu.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, qu, nil
}


func SubscribeJSON[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T) Acktype,
) error {
	ch, qu, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	channel, err := ch.Consume(qu.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for msg := range channel {
			var data T
			err := json.Unmarshal(msg.Body, &data)
			if err != nil {
				msg.Nack(false, false)
				continue
			}
			ackType := handler(data)
			switch ackType {
				case Ack:
					fmt.Println("ACK")
					msg.Ack(false)

				case NackRequeue:
					fmt.Println("NACK_REQUEUE")
					msg.Nack(false, true)

				case NackDiscard:
					fmt.Println("NACK_DISCARD")
					msg.Nack(false, false)
			}
		}
	}()
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	ch, qu, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	channel, err := ch.Consume(qu.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for msg := range channel {
			var data T
			buf := bytes.NewBuffer(msg.Body)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&data)
			if err != nil {
				msg.Nack(false, false)
				continue
			}
			ackType := handler(data)
			switch ackType {
				case Ack:
					fmt.Println("ACK")
					msg.Ack(false)

				case NackRequeue:
					fmt.Println("NACK_REQUEUE")
					msg.Nack(false, true)

				case NackDiscard:
					fmt.Println("NACK_DISCARD")
					msg.Nack(false, false)
			}
		}
	}()
	return nil
}