package main

import (
	"fmt"
	"os"
	"os/signal"
    amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionStr)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %s\n", err)
		return
	}
	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to create channel: %s\n", err)
		return
	}
	exchange := routing.ExchangePerilDirect
	key := routing.PauseKey
	err = pubsub.PublishJSON(ch, exchange, key, routing.PlayingState{IsPaused: true})
	if err != nil {
		fmt.Printf("Failed to publish message: %s\n", err)
		return
	}

	defer conn.Close()
	fmt.Println("Connected to RabbitMQ successfully!")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Shutting down Peril server...")
	conn.Close()
	fmt.Println("Peril server stopped gracefully.")
}
