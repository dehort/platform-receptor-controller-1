package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/RedHatInsights/platform-receptor-controller/internal/controller/models"

	kafka "github.com/segmentio/kafka-go"
)

type MessageDispatcher struct {
	Database    *sql.DB
	KafkaWriter *kafka.Writer
}

func (md *MessageDispatcher) SendMessage(accountNumber string, nodeID string, msg Work) error {
	// find all connections to the customer
	connections, err := models.AllConnectionsPerAccount(md.Database, accountNumber)
	if err != nil {
		return err
	}
	log.Println("connections:", connections)

	// find the best route

	// send the message

	// dispatch job via kafka queue
	messageJSON, err := json.Marshal(msg)
	md.KafkaWriter.WriteMessages(context.Background(), // FIXME: context??!?!
		kafka.Message{
			Key:   []byte(fmt.Sprintf("%s:%s", accountNumber, nodeID)),
			Value: []byte(messageJSON),
		})

	return nil
}
