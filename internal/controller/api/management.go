package api

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/RedHatInsights/platform-receptor-controller/internal/controller"
	"github.com/RedHatInsights/platform-receptor-controller/internal/platform/queue"
	"github.com/redhatinsights/platform-go-middlewares/identity"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

const (
	CONNECTED_STATUS    = "connected"
	DISCONNECTED_STATUS = "disconnected"
)

type ManagementServer struct {
	connectionMgr     *controller.ConnectionManager
	router            *mux.Router
	messageDispatcher *controller.MessageDispatcher
}

func NewManagementServer(cm *controller.ConnectionManager, r *mux.Router, md *controller.MessageDispatcher) *ManagementServer {
	return &ManagementServer{
		connectionMgr:     cm,
		router:            r,
		messageDispatcher: md,
	}
}

func (s *ManagementServer) Routes() {
	securedSubRouter := s.router.PathPrefix("/connection").Subrouter()
	securedSubRouter.Use(identity.EnforceIdentity)
	securedSubRouter.HandleFunc("/disconnect", s.handleDisconnect())
	securedSubRouter.HandleFunc("/status", s.handleConnectionStatus())
	securedSubRouter.HandleFunc("/ping", s.handlePing())
}

type connectionID struct {
	Account string `json:"account"`
	NodeID  string `json:"node_id"`
}

func (s *ManagementServer) handleDisconnect() http.HandlerFunc {

	return func(w http.ResponseWriter, req *http.Request) {

		var connID connectionID

		body, err := ioutil.ReadAll(io.LimitReader(req.Body, 1048576))

		if err != nil {
			panic(err)
		}

		if err := req.Body.Close(); err != nil {
			panic(err)
		}

		if err := json.Unmarshal(body, &connID); err != nil {
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.WriteHeader(422) // unprocessable entity
			if err := json.NewEncoder(w).Encode(err); err != nil {
				panic(err)
			}
		}

		client := s.connectionMgr.GetConnection(connID.Account, connID.NodeID)
		if client == nil {
			w.WriteHeader(http.StatusNotFound)
			log.Printf("No connection to the customer (%+v)...\n", connID)
			return
		}

		client.DisconnectReceptorNetwork()

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(connID); err != nil {
			panic(err)
		}
	}
}

// FIXME: This might not belong here
func (s *ManagementServer) handleConnectionStatus() http.HandlerFunc {

	type connectionStatusResponse struct {
		Status string `json:"status"`
	}

	return func(w http.ResponseWriter, req *http.Request) {

		var connID connectionID

		body, err := ioutil.ReadAll(io.LimitReader(req.Body, 1048576))

		if err != nil {
			panic(err)
		}

		if err := req.Body.Close(); err != nil {
			panic(err)
		}

		if err := json.Unmarshal(body, &connID); err != nil {
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.WriteHeader(http.StatusUnprocessableEntity)
			if err := json.NewEncoder(w).Encode(err); err != nil {
				panic(err)
			}
		}

		log.Println(connID)

		var connectionStatus connectionStatusResponse

		client := s.connectionMgr.GetConnection(connID.Account, connID.NodeID)
		if client != nil {
			connectionStatus.Status = CONNECTED_STATUS
		} else {
			connectionStatus.Status = DISCONNECTED_STATUS
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(connectionStatus); err != nil {
			panic(err)
		}
	}
}

func (s *ManagementServer) handlePing() http.HandlerFunc {

	type connectionStatusResponse struct {
		Status string `json:"status"`
	}

	return func(w http.ResponseWriter, req *http.Request) {

		var connID connectionID

		body, err := ioutil.ReadAll(io.LimitReader(req.Body, 1048576))

		if err != nil {
			panic(err)
		}

		if err := req.Body.Close(); err != nil {
			panic(err)
		}

		if err := json.Unmarshal(body, &connID); err != nil {
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.WriteHeader(http.StatusUnprocessableEntity)
			if err := json.NewEncoder(w).Encode(err); err != nil {
				panic(err)
			}
		}

		log.Println(connID)

		ctx := req.Context()

		msgID, err := uuid.NewRandom()
		if err != nil {
			log.Println("Unable to generate UUID for routing the message...cannot proceed")
			return
		}

		workRequest := controller.Work{MessageID: msgID,
			Recipient: connID.NodeID,
			RouteList: []string{"node-b", "node-a"},
			Payload:   time.Now().String(),
			Directive: "receptor:ping"}

		log.Printf("work request: %+v", workRequest)

		// verify connection exists
		// route msg to connection
		err = s.messageDispatcher.SendMessage(connID.Account, connID.NodeID, workRequest)
		if err != nil {
			log.Println("ERROR!!   ", err)
			panic(err)
		}

		// retreive payload from kafka

		// return payload as http response

		responseChan := make(chan []byte)

		go consume(ctx, msgID, responseChan)

		log.Println("HTTP go routine - WAITING FOR A RESPONSE!!")
		log.Println("!!! FIXME!!! time out check here!!")
		response := <-responseChan

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		w.Write(response)
		/*
			if err := json.NewEncoder(w).Encode(connectionStatus); err != nil {
				panic(err)
			}
		*/
	}
}

func consume(ctx context.Context, msgID uuid.UUID, response chan []byte) {
	kafkaConsumerConfig := queue.GetConsumer()
	kafkaConsumerConfig.Topic = "platform.receptor-controller.responses"
	kafkaConsumerConfig.GroupID = "turd ferguson"
	r := queue.StartConsumer(kafkaConsumerConfig)

	defer func() {
		err := r.Close()
		if err != nil {
			log.Println("Kafka response reader - error closing consumer: ", err)
			return
		}
		log.Println("Kafka response reader leaving...")
	}()

	for {
		log.Printf("Kafka response reader - waiting on a message from kafka...")
		m, err := r.ReadMessage(ctx)
		if err != nil {
			// FIXME:  do we need to call cancel here??
			log.Println("Kafka response reader - error reading message: ", err)
			break
		}

		log.Printf("Kafka response reader - received message from %s-%d [%d]: %s: %s\n",
			m.Topic,
			m.Partition,
			m.Offset,
			string(m.Key),
			string(m.Value))

		targetKey := msgID.String()
		log.Println("FIXME!!!!!")
		log.Println("*** looking for key that matches: ", targetKey)
		log.Println("*** found key: ", string(m.Key))

		if string(m.Key) == targetKey {
			response <- m.Value
			break
		} else {
			log.Println("Kafka response reader - received message but did not send. Account number not found.")
		}
	}
}
