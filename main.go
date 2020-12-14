package main

import (
	"errors"
	"log"
	"os"

	"github.com/streadway/amqp"
)

func main() {

	validateEnvVars()

	conn, err := amqp.Dial(os.Getenv("RABBIT_URL"))
	defer conn.Close()

	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		os.Getenv("RABBIT_QUERY_DATA_QUEUE"), "", true, // auto-ackc
		false, false, false, nil)

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}

func validateEnvVars() {
	vars := []string{"RABBIT_URL", "RABBIT_QUERY_DATA_QUEUE", "RABBIT_STORE_DATA_QUEUE"}
	for _, val := range vars {
		if len(val) == 0 {
			panic(errors.New("not found " + val + " environment variable"))
		}
	}
}
