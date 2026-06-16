package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			warMsg := gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+move.Player.Username, warMsg)
			if err != nil {
				fmt.Printf("Failed to publish war recognition: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}	

func publishGameLog(ch *amqp.Channel, username string, message string) pubsub.Acktype {
	gl := routing.GameLog{
		Username: username,
		Message:  message,
	}
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, gl)
	if err != nil {
		fmt.Printf("Failed to publish game log: %s\n", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}

func handleWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(war gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(war)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			return publishGameLog(ch, gs.Player.Username, msg)
		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			return publishGameLog(ch, gs.Player.Username, msg)
		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			return publishGameLog(ch, gs.Player.Username, msg)
		default:
			fmt.Println("error: unknown war outcome")
			return pubsub.NackDiscard
		}
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

	pubCh, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to create publish channel: %s\n", err)
		return
	}
	defer pubCh.Close()

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"army_moves."+username,
		"army_moves.*",
		pubsub.Transient,
		handlerMove(gameState, pubCh),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to army moves: %s\n", err)
		return
	}

	// Subscribe to pause messages
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		"pause."+username,
		username,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to pause messages: %s\n", err)
		return
	}
	// Use a durable queue with the war handler. The queue name should just be war. All clients will share this queue. Whenever war is declared, only one client will consume the message.
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handleWar(gameState, pubCh),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to war recognitions: %s\n", err)
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
			err = pubsub.PublishJSON(pubCh, routing.ExchangePerilTopic, "army_moves."+username, armyMove)
			fmt.Printf("successfully published, move to %s...\n", armyMove.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			count, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Printf("Invalid spam count: %s\n", input[1])
				continue
			}
			for i := 0; i < count; i++ {	
				maliciousLog := gamelogic.GetMaliciousLog()
				err := pubsub.PublishGob(pubCh, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, maliciousLog)
				if err != nil {
					fmt.Printf("Failed to publish malicious log: %s\n", err)
				} else {
					fmt.Printf("Successfully published malicious log: %s\n", maliciousLog)
				}
			}
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