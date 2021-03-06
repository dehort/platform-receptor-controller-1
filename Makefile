.PHONY: test clean deps

GATEWAY_BINARY=receptor-controller-gateway


run: $(GATEWAY_BINARY)
	./$<

$(GATEWAY_BINARY): main.go ws_controller.go management.go job_receiver.go
	go build -o $@

deps:
	go get -u golang.org/x/lint/golint
	go get -u github.com/google/uuid
	go get -u github.com/spf13/viper
	go get -u github.com/segmentio/kafka-go

test:
	go test $(TEST_ARGS) ./...

fmt:
	go fmt ./...

lint:
	$(GOPATH)/bin/golint ./...

clean:
	rm -f $(GATEWAY_BINARY)
