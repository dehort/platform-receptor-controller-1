package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	//"os"
	"strings"

	MQTT "github.com/eclipse/paho.mqtt.golang"

	"github.com/RedHatInsights/platform-receptor-controller/internal/controller"
)

const TOPIC = "redhat/insights"

func NewTLSConfig() *tls.Config {
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
	cert, err := tls.LoadX509KeyPair("connector-service-cert.pem", "connector-service-key.pem")
	if err != nil {
		panic(err)
	}

	// Just to print out the client certificate..
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		panic(err)
	}
	fmt.Println(cert.Leaf)
	fmt.Println(cert.Leaf.Subject.ToRDNSequence())
	fmt.Println(cert.Leaf.Subject.CommonName)

	// Create tls.Config with desired tls properties
	return &tls.Config{
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
}

func NewConnectionRegistrar(connectionRegistrar controller.ConnectionRegistrar) {

	broker := "ssl://localhost:8883"
	//broker := "ssl://localhost:8883"
	//broker := "tcp://localhost:1883"

	startSubscriber(broker, connectionRegistrar)
}

var m MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("rec TOPIC: %s MSG:%s\n", msg.Topic(), msg.Payload())
}

func startSubscriber(broker string, connectionRegistrar controller.ConnectionRegistrar) {

	tlsconfig := NewTLSConfig()

	connOpts := MQTT.NewClientOptions()
	connOpts.AddBroker(broker)

	connOpts.SetTLSConfig(tlsconfig)

	//lastWill := fmt.Sprintf("{'client': '%s'}", clientID)
	//connOpts.SetWill(ACCOUNT_TOPIC+"/leaving", lastWill, 0, false)

	connOpts.SetDefaultPublishHandler(m)

	recordConnection := messageHandler(connectionRegistrar)

	connOpts.OnConnect = func(c MQTT.Client) {
		topic := fmt.Sprintf("%s/+/in", TOPIC)
		fmt.Println("subscribing to topic: ", topic)
		if token := c.Subscribe(topic, 0, recordConnection); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}

	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	fmt.Println("Connected to broker", broker)
}

func messageHandler(connectionRegistrar controller.ConnectionRegistrar) func(MQTT.Client, MQTT.Message) {
	return func(client MQTT.Client, message MQTT.Message) {
		fmt.Printf("Received message on topic: %s\nMessage: %s\n", message.Topic(), message.Payload())

		//verify the MQTT topic
		clientIDFromTopic, err := verifyTopic(message.Topic())
		if err != nil {
			log.Println(err)
			return
		}

		var connMsg ConnectorMessage

		if err := json.Unmarshal(message.Payload(), &connMsg); err != nil {
			fmt.Println("unmarshal of message failed, err:", err)
			panic(err)
		}

		fmt.Println("Got a connection:", connMsg)

		if clientIDFromTopic != connMsg.ClientID {
			fmt.Println("Potentially malicious connection attempt")
			return
		}

		switch connMsg.MessageType {
		case "handshake":
			handleHandshake(client, connMsg, connectionRegistrar)
		case "disconnect":
			handleDisconnect(client, connMsg, connectionRegistrar)
		case "processing_error":
			handleProcessingError(connMsg)
		default:
			fmt.Println("Invalid message type!")
		}
	}
}

func handleHandshake(client MQTT.Client, connMsg ConnectorMessage, connectionRegistrar controller.ConnectionRegistrar) {

	account, err := getAccountNumberFromBop(connMsg.ClientID)

	if err != nil {
		fmt.Println("Couldn't determine account number...ignoring connection")
		// FIXME: Disconnect client??  How??
		return
	}

	proxy := ReceptorMQTTProxy{ClientID: connMsg.ClientID, Client: client}

	connectionRegistrar.Register(context.Background(), account, connMsg.ClientID, &proxy)
	// FIXME: check for error, but ignore duplicate registration errors
}

func handleDisconnect(client MQTT.Client, connMsg ConnectorMessage, connectionRegistrar controller.ConnectionRegistrar) {

	account, err := getAccountNumberFromBop(connMsg.ClientID)

	if err != nil {
		fmt.Println("Couldn't determine account number...ignoring connection")
		// FIXME: Disconnect client??  How??
		return
	}

	connectionRegistrar.Unregister(context.Background(), account, connMsg.ClientID)
}

func handleProcessingError(connMsg ConnectorMessage) {
	fmt.Println("PROCESSING ERROR")
}

func getAccountNumberFromBop(clientID string) (string, error) {
	// FIXME: need to lookup the account number for the connected client
	fmt.Println("FIXME: looking up the connection's account number in BOP")
	return "010101", nil
}

func verifyTopic(topic string) (string, error) {
	items := strings.Split(topic, "/")
	if len(items) != 4 {
		return "", errors.New("MQTT topic requires 4 sections: redhat, insights, <clientID>, in")
	}

	if items[0] != "redhat" || items[1] != "insights" || items[3] != "in" {
		return "", errors.New("MQTT topic needs to be redhat/insights/<clientID>/in")
	}

	return items[2], nil
}
