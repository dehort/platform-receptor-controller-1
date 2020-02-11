package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/RedHatInsights/platform-receptor-controller/internal/platform/queue"

	"github.com/RedHatInsights/platform-receptor-controller/internal/receptor/protocol"
	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

type ResponseMessage struct {
	Account      string      `json:"account"`
	Sender       string      `json:"sender"`
	MessageID    string      `json:"message_id"`
	MessageType  string      `json:"message_type"`
	Payload      interface{} `json:"payload"`
	Code         int         `json:"code"`
	InResponseTo string      `json:"in_response_to"`
	Serial       int         `json:"serial"`
}

type ResponseNotification struct {
	MessageID       uuid.UUID // Asynchronous Completion Token (ACT)
	ResponseChannel chan ResponseMessage
}

type ChannelBasedResponseDispatcher struct {
	Ctx           context.Context
	Register      chan ResponseNotification
	Dispatch      chan ResponseMessage
	DispatchTable map[uuid.UUID]chan ResponseMessage
}

func (cbrd *ChannelBasedResponseDispatcher) Run() {
	for {
		log.Println("Channel Based Response Dispatcher - Waiting for something to process")

		select {
		/*
			case <-cbrd.Ctx.Done():
				log.Println("Channel Based Response Dispatcher...Context based done...leaving")

				// FIXME:  Loop through table closing channels, calling cancel??

				return
		*/
		case responseNotification := <-cbrd.Register:
			log.Println("Websocket writer needs to send msg:", responseNotification)

			// Add notifier to table

			cbrd.DispatchTable[responseNotification.MessageID] = responseNotification.ResponseChannel

		case responseMsgToDispatch := <-cbrd.Dispatch:
			log.Println("Websocket writer needs to send msg:", responseMsgToDispatch)

			// lookup message id...send Response down channel

			messageID, err := uuid.Parse(responseMsgToDispatch.MessageID)
			if err != nil {
				log.Printf("Unable to parse uuid (%s) from response: %s", responseMsgToDispatch.MessageID, err)
			}

			responseChannel, exists := cbrd.DispatchTable[messageID]
			if exists == false {
				log.Println("Unable to locate response channel for ", responseMsgToDispatch.MessageID)
				return
			}

			responseChannel <- responseMsgToDispatch
		}
	}
}

type ResponseDispatcherFactory struct {
	writer *kafka.Writer
}

func NewResponseDispatcherFactory(writer *kafka.Writer) *ResponseDispatcherFactory {
	return &ResponseDispatcherFactory{
		writer: writer,
	}
}

func (fact *ResponseDispatcherFactory) NewDispatcher(account, nodeID string) *ResponseDispatcher {
	log.Println("Creating a new response dispatcher")

	cbrd := &ChannelBasedResponseDispatcher{
		//Ctx:             req.Context(),  FIXME:
		Register:      make(chan ResponseNotification),
		Dispatch:      make(chan ResponseMessage),
		DispatchTable: make(map[uuid.UUID]chan ResponseMessage),
	}

	go cbrd.Run()

	return &ResponseDispatcher{
		account: account,
		nodeID:  nodeID,
		writer:  fact.writer,
		cbrd:    cbrd,
	}
}

type ResponseDispatcher struct {
	account string
	nodeID  string
	writer  *kafka.Writer
	cbrd    *ChannelBasedResponseDispatcher
}

func (rd *ResponseDispatcher) GetKey() string {
	return fmt.Sprintf("%s:%s", rd.account, rd.nodeID)
}

func (rd *ResponseDispatcher) Register(messageID uuid.UUID, notifyChan chan ResponseMessage) error {
	rd.cbrd.Register <- ResponseNotification{messageID, notifyChan}
	return nil
}

func (rd *ResponseDispatcher) DispatchResponse(ctx context.Context, m protocol.Message, receptorID string) error {
	if m.Type() != protocol.PayloadMessageType {
		log.Printf("Unable to dispatch message (type: %d): %s", m.Type(), m)
		return nil
	}

	payloadMessage, ok := m.(*protocol.PayloadMessage)
	if !ok {
		log.Println("Unable to convert message into PayloadMessage")
		return nil
	}

	// verify this message was meant for this receptor/peer (probably want a uuid here)
	if payloadMessage.RoutingInfo.Recipient != receptorID {
		log.Println("Recieved message that was not intended for this node")
		return nil
	}

	responseMessage := ResponseMessage{
		Account:      rd.account,
		Sender:       payloadMessage.RoutingInfo.Sender,
		MessageID:    payloadMessage.Data.MessageID,
		MessageType:  payloadMessage.Data.MessageType,
		Payload:      payloadMessage.Data.RawPayload,
		Code:         payloadMessage.Data.Code,
		InResponseTo: payloadMessage.Data.InResponseTo,
		Serial:       payloadMessage.Data.Serial,
	}

	log.Printf("Dispatching response:%+v", responseMessage)

	jsonResponseMessage, err := json.Marshal(responseMessage)
	if err != nil {
		log.Println("JSON marshal of ResponseMessage failed, err:", err)
		return nil
	}

	rd.cbrd.Dispatch <- responseMessage

	rd.writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(payloadMessage.Data.InResponseTo),
			Value: jsonResponseMessage,
		})

	return nil
}

type MessageDispatcherFactory struct {
	readerConfig *queue.ConsumerConfig
}

func NewMessageDispatcherFactory(cfg *queue.ConsumerConfig) *MessageDispatcherFactory {
	return &MessageDispatcherFactory{
		readerConfig: cfg,
	}
}

func (fact *MessageDispatcherFactory) NewDispatcher(account, nodeID string) *MessageDispatcher {
	log.Println("Creating a new work dispatcher")
	r := queue.StartConsumer(fact.readerConfig)
	return &MessageDispatcher{
		account: account,
		nodeID:  nodeID,
		reader:  r,
	}
}

type MessageDispatcher struct {
	account string
	nodeID  string
	reader  *kafka.Reader
}

func (md *MessageDispatcher) GetKey() string {
	return fmt.Sprintf("%s:%s", md.account, md.nodeID)
}

func (md *MessageDispatcher) StartDispatchingMessages(ctx context.Context, c chan<- Message) {
	defer func() {
		err := md.reader.Close()
		if err != nil {
			log.Println("Kafka job reader - error closing consumer: ", err)
			return
		}
		log.Println("Kafka job reader leaving...")
	}()

	for {
		log.Printf("Kafka job reader - waiting on a message from kafka...")
		m, err := md.reader.ReadMessage(ctx)
		if err != nil {
			// FIXME:  do we need to call cancel here??
			log.Println("Kafka job reader - error reading message: ", err)
			break
		}

		log.Printf("Kafka job reader - received message from %s-%d [%d]: %s: %s\n",
			m.Topic,
			m.Partition,
			m.Offset,
			string(m.Key),
			string(m.Value))

		if string(m.Key) == md.GetKey() {
			// FIXME:
			var w Message
			if err := json.Unmarshal(m.Value, &w); err != nil {
				log.Println("Unable to unmarshal message from kafka queue")
				continue
			}
			c <- w
		} else {
			log.Println("Kafka job reader - received message but did not send. Account number not found.")
		}
	}
}
