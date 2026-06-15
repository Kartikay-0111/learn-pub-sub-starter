package main

import (
	"fmt"
	"os"
	"os/signal"
    amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func main() {
	fmt.Println("Starting Peril server...")
	gamelogic.PrintServerHelp()

	connectionStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionStr)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %s\n", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ successfully!")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to create channel: %s\n", err)
		return
	}
	channel, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug,"game_logs.*", pubsub.Durable)
	if err != nil {
		fmt.Printf("Failed to declare and bind queue: %s\n", err)
		return
	}
	defer channel.Close()
	fmt.Printf("Declared and bound queue: %s\n", queue.Name)
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		exchange := routing.ExchangePerilDirect
		key := routing.PauseKey
		if input[0] == "pause" {
			err = pubsub.PublishJSON(ch, exchange, key, routing.PlayingState{IsPaused: true})
			if err != nil {
				fmt.Printf("Failed to publish pause message: %s\n", err)
				return
			}
		} else if input[0] == "resume" {
			err = pubsub.PublishJSON(ch, exchange, key, routing.PlayingState{IsPaused: false})
			if err != nil {
				fmt.Printf("Failed to publish resume message: %s\n", err)
				return
			}
		} else if input[0] == "quit" {
			fmt.Println("Quitting server...")
			break
		} else {
			fmt.Println("Unknown command.")
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
	fmt.Println("Received interrupt signal, shutting down...")
	conn.Close()
}