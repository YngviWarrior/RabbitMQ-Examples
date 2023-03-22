package main

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Kline struct {
	Asset            uint64  `json:"asset"`
	AssetSymbol      string  `json:"asset_symbol"`
	AssetQuote       uint64  `json:"asset_quote"`
	AssetQuoteSymbol string  `json:"asset_quote_symbol"`
	Exchange         uint64  `json:"exchange"`
	Mts              uint64  `json:"mts"`
	Open             float64 `json:"open"`
	Close            float64 `json:"close"`
	High             float64 `json:"high"`
	Low              float64 `json:"low"`
	Volume           float64 `json:"volume"`
	Roc              float64 `json:"roc"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://rabbitmq:rabbitmq@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"cotation_fanout", // name
		"fanout",          // type
		false,             // durable
		true,              // auto-deleted
		false,             // internal
		true,              // no-wait
		nil,               // arguments
	)

	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"cotation_traderbot", // name
		false,                // durable
		true,                 // delete when unused
		false,                // exclusive
		true,                 // no-wait
		nil,                  // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,            // queue name
		"",                // routing key
		"cotation_fanout", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		true,   // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			var k Kline
			err := json.Unmarshal(d.Body, &k)

			if err != nil {
				log.Println(err)
			}

			log.Printf("Received a message: %v", k)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
