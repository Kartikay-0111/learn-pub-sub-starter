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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
    return func(move gamelogic.ArmyMove) {
        defer fmt.Print("> ")
        gs.HandleMove(move)
    }
}

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
	fmt.Printf("Client %s connected to RabbitMQ successfully!\n", username)

	gameState := gamelogic.NewGameState(username)

	ch ,_, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, "army_moves."+username, "army_moves.*", pubsub.SimpleQueueType("transient"))

	if err != nil {
		fmt.Printf("Failed to declare and bind queue: %s\n", err)
		return
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			err := gameState.CommandSpawn(input)
			if err != nil {
				fmt.Printf("%s\n", err)
			}
		case "move":
			armyMove, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Printf("%s\n", err)
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, "army_moves."+username, armyMove)
			fmt.Printf("successfully published, move to %s...\n", armyMove.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Client shutting down...")
}