package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/RedHatInsights/platform-receptor-controller/internal/platform/logger"

	"github.com/gorilla/handlers"
	"github.com/sirupsen/logrus"
)

func accessLoggerMiddleware(next http.Handler) http.Handler {
	return handlers.CustomLoggingHandler(ioutil.Discard, next, logrusAccessLogAdapter)
}

func logrusAccessLogAdapter(w io.Writer, params handlers.LogFormatterParams) {
	request := fmt.Sprintf("%s %s %s", params.Request.Method, params.Request.URL, params.Request.Proto)
	requestID := getRequestIdFromRequest(params.Request)
	logger.Log.WithFields(logrus.Fields{
		"remote_addr": params.Request.RemoteAddr,
		"request":     request,
		"request_id":  requestID,
		"status":      params.StatusCode,
		"size":        params.Size},
	).Info("access")
}

func getRequestIdFromRequest(request *http.Request) *string {
	var requestID *string
	requestIDHeader := request.Header["X-Rh-Insights-Request-Id"]
	if len(requestIDHeader) > 0 {
		requestID = &requestIDHeader[0]
	}
	return requestID
}
