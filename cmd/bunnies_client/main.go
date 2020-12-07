package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	//"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	Connector "github.com/RedHatInsights/platform-receptor-controller/internal/mqtt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func NewTLSConfig(certFile string, keyFile string) (*tls.Config, string) {
	// Import trusted certificates from CAfile.pem.
	// Alternatively, manually add CA certificates to
	// default openssl CA bundle.
	/*
	   certpool := x509.NewCertPool()
	   pemCerts, err := ioutil.ReadFile("samplecerts/CAfile.pem")
	   if err == nil {
	       certpool.AppendCertsFromPEM(pemCerts)
	   }
	*/

	// Import client certificate/key pair
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		panic(err)
	}

	// Just to print out the client certificate..
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		panic(err)
	}
	//fmt.Println(cert.Leaf)
	fmt.Println(cert.Leaf.Subject.CommonName)

	tlsConfig := &tls.Config{
		// RootCAs = certs used to verify server cert.
		//RootCAs: certpool,
		// ClientAuth = whether to request cert from server.
		// Since the server is set up for SSL, this happens
		// anyways.
		//ClientAuth: tls.NoClientCert,
		// ClientCAs = certs used to validate client cert.
		//ClientCAs: nil,
		// InsecureSkipVerify = verify that cert contents
		// match server. IP matches what is in cert etc.
		InsecureSkipVerify: true,
		// Certificates = list of certs client sends to server.
		Certificates: []tls.Certificate{cert},
	}

	return tlsConfig, cert.Leaf.Subject.CommonName
}

func main() {

	/*
	   logger := log.New(os.Stderr, "", log.LstdFlags)
	   MQTT.ERROR = logger
	   MQTT.CRITICAL = logger
	   MQTT.WARN = logger
	   MQTT.DEBUG = logger
	*/

	connectionCount := flag.Int("connection_count", 1, "number of connections to create")
	broker := flag.String("broker", "tcp://eclipse-mosquitto:1883", "hostname / port of broker")
	certFile := flag.String("cert", "cert.pem", "path to cert file")
	keyFile := flag.String("key", "key.pem", "path to key file")
	flag.Parse()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	for i := 0; i < *connectionCount; i++ {
		go startProducer(*certFile, *keyFile, *broker, i)
	}

	<-c

}

var m MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("default handler rec TOPIC: %s MSG:%s\n", msg.Topic(), msg.Payload())
}

func startProducer(certFile string, keyFile string, broker string, i int) {
	tlsconfig, clientID := NewTLSConfig(certFile, keyFile)

	readTopic := fmt.Sprintf("redhat/insights/%s/out", clientID)
	writeTopic := fmt.Sprintf("redhat/insights/%s/in", clientID)
	fmt.Println("consumer topic: ", readTopic)

	connOpts := MQTT.NewClientOptions()
	connOpts.AddBroker(broker)
	/*
	   hostname, err := os.Hostname()
	   if err != nil {
	       panic("Unable to determine hostname:" + err.Error())
	   }
	*/

	//username := fmt.Sprintf("client-%d", i)

	//clientID := fmt.Sprintf("client-%s-%d", hostname, i)

	connOpts.SetClientID(clientID).SetTLSConfig(tlsconfig)
	//connOpts.SetCleanSession(true)
	//connOpts.SetUsername(username)
	//connOpts.SetPassword(username)
	//connOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	connOpts.SetTLSConfig(tlsconfig)

	lastWillPayload, err := buildDisconnectMessage(clientID)
	connOpts.SetWill(writeTopic, string(lastWillPayload), byte(0), false)

	//    connOpts.SetDefaultPublishHandler(m)

	connOpts.OnConnect = func(c MQTT.Client) {
		fmt.Println("*** OnConnect - subscribing to topic:", readTopic)
		if token := c.Subscribe(readTopic, 0, onMessageReceived); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}

	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	fmt.Println("Connected to server ", broker)

	/* Verify that this client cannot publish to a different client's topic
	   topic = fmt.Sprintf("redhat/insights/%s/in", "client-10")
	   payload = fmt.Sprintf(`{"id": "%s"}`, "client-10")
	*/

	/* SPOOF the payload
	   spoofPayload := `{"id": "client-NO"}`
	   payload = spoofPayload
	*/

	connMsg := Connector.ConnectorMessage{
		MessageType: "host_handshake",
		MessageID:   "1234",
		ClientID:    clientID,
		Version:     1,
	}

	payload, err := json.Marshal(connMsg)

	if err != nil {
		fmt.Println("marshal of message failed, err:", err)
		panic(err)
	}

	fmt.Println("publishing to topic:", writeTopic)
	client.Publish(writeTopic, byte(0), false, payload)
	fmt.Printf("Published message %s... Sleeping...\n", payload)
	time.Sleep(time.Second * 10)
}

func onMessageReceived(client MQTT.Client, message MQTT.Message) {
	fmt.Printf("Received message on topic: %s\nMessage: %s\n", message.Topic(), message.Payload())
}

func buildDisconnectMessage(clientID string) ([]byte, error) {
	connMsg := Connector.ConnectorMessage{
		MessageType: "disconnect",
		MessageID:   "4321",
		ClientID:    clientID,
		Version:     1,
	}

	return json.Marshal(connMsg)
}
