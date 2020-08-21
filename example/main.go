package main

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/jackc/pgconn"
	"github.com/mbict/watermill-pglogrep/pkg/src"
	"github.com/nats-io/stan.go"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "secret"
	dbname   = "postgres"
)

func main() {
	//logger := watermill.NewStdLogger(true, true)
	logger := watermill.NewStdLogger(false, false)

	//connect to database
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable replication=database", host, port, user, password, dbname)
	conn, err := pgconn.Connect(context.Background(), psqlInfo)
	if err != nil {
		logger.Error("failed to connect to PostgreSQL server", err, nil)
		return
	}
	defer conn.Close(context.Background())

	//create subscriber
	subscriber, err := src.NewSubscriber(conn, logger)
	msgCh, err := subscriber.Subscribe(context.Background(), "eventstore_slot.eventstore_cdc")
	if err != nil {
		logger.Error("unable to subscribe", err, nil)
		return
	}

	//create publisher
	publisher, err := nats.NewStreamingPublisher(
		nats.StreamingPublisherConfig{
			ClusterID: "test-cluster",
			ClientID:  "example-publisher",
			StanOptions: []stan.Option{
				stan.NatsURL("nats://localhost:4222"),
			},
			Marshaler: nats.GobMarshaler{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	//processing the messages
	for msg := range msgCh {
		if err := publisher.Publish("example.topic", msg); err != nil {
			fmt.Printf("%s\n", msg.Payload)
			logger.Error("unable to publish message to broker", err, nil)
			msg.Nack()
			continue
		}

		fmt.Print(".")
		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
