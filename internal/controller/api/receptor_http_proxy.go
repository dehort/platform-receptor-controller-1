package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/RedHatInsights/platform-receptor-controller/internal/platform/logger"

	"github.com/google/uuid"
)

type ReceptorHttpProxy struct {
	Url string
}

func (rhp *ReceptorHttpProxy) SendMessage(ctx context.Context, accountNumber string, recipient string, route []string, payload interface{}, directive string) (*uuid.UUID, error) {
	logger.Log.Printf("SendMessage")

	postPayload := jobRequest{accountNumber, recipient, payload, directive}
	jsonStr, err := json.Marshal(postPayload)
	logger.Log.Printf("jsonStr: %s", jsonStr)

	req, err := http.NewRequest(http.MethodPost, rhp.Url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "appliation/json")
	// FIXME:  this should be the PSK
	req.Header.Set("x-rh-identity", "eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAiMDAwMDAwMSIsICJpbnRlcm5hbCI6IHsib3JnX2lkIjogIjAwMDAwMSJ9fX0=")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	jobResponse := jobResponse{}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&jobResponse); err != nil {
		logger.Log.Error("Unable to read response from receptor-gateway")
		return nil, errors.New("Unable to read response from receptor-gateway")
	}

	messageID, err := uuid.Parse(jobResponse.JobID)
	if err != nil {
		logger.Log.Error("Unable to read message id from receptor-gateway")
		return nil, errors.New("Unable to read response from receptor-gateway")
	}

	return &messageID, nil
}

func (rhp *ReceptorHttpProxy) Ping(context.Context, string, string, []string) (interface{}, error) {
	logger.Log.Printf("Ping")
	return nil, nil
}

func (rhp *ReceptorHttpProxy) Close() {
	logger.Log.Printf("Close")
}

func (rhp *ReceptorHttpProxy) GetCapabilities() interface{} {
	logger.Log.Printf("GetCapabilities")
	return nil
}
