package mqtt

import (
	"context"
	"errors"
	"fmt"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

var (
	errUnableToSendMessage = errors.New("unable to send message")
)

type ReceptorMQTTProxy struct {
	ClientID string
	Client   MQTT.Client
}

func (rhp *ReceptorMQTTProxy) SendMessage(ctx context.Context, accountNumber string, recipient string, route []string, payload interface{}, directive string) (*uuid.UUID, error) {

	fmt.Println("Sending message to connected client")

	topic := fmt.Sprintf("redhat/insights/out/%s", rhp.ClientID)

	fmt.Println("topic: ", topic)

	t := rhp.Client.Publish(topic, byte(0), false, payload)
	go func() {
		_ = t.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
		if t.Error() != nil {
			fmt.Println("public error:", t.Error()) // Use your preferred logging technique (or just fmt.Printf)
		}
	}()

	messageID, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	return &messageID, nil
}

func (rhp *ReceptorMQTTProxy) Ping(ctx context.Context, accountNumber string, recipient string, route []string) (interface{}, error) {
	return struct{}{}, nil
}

func (rhp *ReceptorMQTTProxy) Close(ctx context.Context) error {
	return nil
}

func (rhp *ReceptorMQTTProxy) GetCapabilities(ctx context.Context) (interface{}, error) {
	return struct{}{}, nil
}
