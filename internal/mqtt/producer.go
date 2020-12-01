package mqtt

import (
	"flag"
	"fmt"
	//"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

const ACCOUNT_TOPIC = "redhat/000101/insights/"

//const ACCOUNT_TOPIC = "JmTQJP-7vpMpHZAJsAg-PzRIDLHoElVk/redhat/000101/insights/"
//const ACCOUNT_TOPIC = "aVTLMwRJKjxip-ZISixZOwsvEsyiuYF5" + "/" + "redhat/000101/insights/"

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
	flag.Parse()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	for i := 0; i < *connectionCount; i++ {
		go startProducer(*broker, i)
	}

	<-c

}

func startProducer(broker string, i int) {
	tlsconfig := NewTLSConfig()

	connOpts := MQTT.NewClientOptions()
	connOpts.AddBroker(broker)
	hostname, err := os.Hostname()
	if err != nil {
		panic("Unable to determine hostname:" + err.Error())
	}

	clientID := fmt.Sprintf("producer-%s-%d", hostname, i)
	connOpts.SetClientID(clientID).SetTLSConfig(tlsconfig)
	//connOpts.SetCleanSession(true)
	connOpts.SetUsername("connector")
	connOpts.SetPassword("fred")
	//connOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	connOpts.SetTLSConfig(tlsconfig)

	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	fmt.Println("Connected to server ", broker)

	topic := fmt.Sprintf("%s%d", ACCOUNT_TOPIC, i)
	payload := fmt.Sprintf(`{"device": "%d", "type": "%s","temperature": %.2f , "humidity": %.2f}`,
		i, "arm64", 72.2, 10.2)

	for {
		client.Publish(topic, byte(0), false, payload)
		fmt.Printf("Published message %s... Sleeping...\n", payload)
		time.Sleep(time.Second * 5)
	}
}
