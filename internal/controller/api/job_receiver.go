package api

import (
	"github.com/gorilla/mux"

	//	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/google/uuid"

	"github.com/RedHatInsights/platform-receptor-controller/internal/controller"
	"github.com/redhatinsights/platform-go-middlewares/identity"
)

type JobReceiver struct {
	connectionMgr     *controller.ConnectionManager
	router            *mux.Router
	messageDispatcher *controller.MessageDispatcher
}

func NewJobReceiver(cm *controller.ConnectionManager, r *mux.Router, md *controller.MessageDispatcher) *JobReceiver {
	return &JobReceiver{
		connectionMgr:     cm,
		router:            r,
		messageDispatcher: md,
	}
}

func (jr *JobReceiver) Routes() {
	securedSubRouter := jr.router.PathPrefix("/").Subrouter()
	securedSubRouter.Use(identity.EnforceIdentity)
	securedSubRouter.HandleFunc("/job", jr.handleJob())
}

func (jr *JobReceiver) handleJob() http.HandlerFunc {

	type JobRequest struct {
		Account   string      `json:"account"`
		Recipient string      `json:"recipient"`
		Payload   interface{} `json:"payload"`
		Directive string      `json:"directive"`
	}

	type JobResponse struct {
		JobID string `json:"id"`
	}

	return func(w http.ResponseWriter, req *http.Request) {

		log.Println("Simulating JobReceiver producing a message")

		var jobRequest JobRequest

		body, err := ioutil.ReadAll(io.LimitReader(req.Body, 1048576))

		if err != nil {
			panic(err)
		}

		if err := req.Body.Close(); err != nil {
			panic(err)
		}

		if err := json.Unmarshal(body, &jobRequest); err != nil {
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.WriteHeader(http.StatusUnprocessableEntity)
			if err := json.NewEncoder(w).Encode(err); err != nil {
				panic(err)
			}
		}

		log.Println("jobRequest:", jobRequest)

		/*
			// dispatch job via client's sendwork
			// not using client's sendwork, but leaving this code in to verify connection?
			var client controller.Client
			client = jr.connectionMgr.GetConnection(jobRequest.Account, jobRequest.Recipient)
			if client == nil {
				// FIXME: the connection to the client was not available
				log.Println("No connection to the customer...")
				w.WriteHeader(http.StatusNotFound)
				return
			}
		*/

		jobID, err := uuid.NewRandom()
		if err != nil {
			log.Println("Unable to generate UUID for routing the job...cannot proceed")
			return
		}

		jobResponse := JobResponse{jobID.String()}

		log.Println("job request:", jobRequest)

		workRequest := controller.Work{MessageID: jobID,
			Recipient: jobRequest.Recipient,
			RouteList: []string{jobRequest.Recipient},
			Payload:   jobRequest.Payload,
			Directive: jobRequest.Directive}

		err = jr.messageDispatcher.SendMessage(jobRequest.Account, jobRequest.Recipient, workRequest)
		if err != nil {
			log.Println("ERROR!!   ", err)
			panic(err)
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(jobResponse); err != nil {
			panic(err)
		}
	}
}
