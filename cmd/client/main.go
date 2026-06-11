package main

import (
	"fmt"
	"os"
	"os/signal"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionStr)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %s\n", err)
		return
	}
	defer conn.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	channel, _, err := pubsub.DeclareAndBind(conn,routing.ExchangePerilDirect,routing.PauseKey+"."+username,routing.PauseKey,"transient")
	if err != nil {
		fmt.Printf("Failed to declare and bind queue: %s\n", err)
		return
	}
	defer channel.Close()
	fmt.Printf("Client %s connected to RabbitMQ successfully!\n", username)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}