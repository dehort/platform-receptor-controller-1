package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	//"time"
	//"encoding/base64"
	"syscall"

	"github.com/RedHatInsights/platform-receptor-controller/internal/receptor/protocol"
	"github.com/gorilla/websocket"
)

func readSocket(c *websocket.Conn, mt protocol.NetworkMessageType) protocol.Message {
	for {
		mtype, r, err := c.NextReader()
		fmt.Println("mtype:", mtype)
		if err != nil {
			fmt.Println("NextReader err:", err)
			return nil
		}
		//Expect(mtype).Should(Equal(websocket.BinaryMessage))

		fmt.Println("TestClient reading response from receptor-controller")
		m, err := protocol.ReadMessage(r)
		if err != nil {
			fmt.Println("ReadMessage err:", err)
			return nil
		}

		fmt.Println("m:", m)
		//Expect(m.Type()).Should(Equal(mt))
	}
	return nil
}

func writeSocket(c *websocket.Conn, message protocol.Message) {
	w, err := c.NextWriter(websocket.BinaryMessage)
	fmt.Println("err:", err)
	//Expect(err).NotTo(HaveOccurred())

	fmt.Println("TestClient writing to receptor-controller")
	err = protocol.WriteMessage(w, message)
	fmt.Println("err:", err)
	//Expect(err).NotTo(HaveOccurred())
	w.Close()
}

var targetUrl = flag.String("url", "ws://localhost:8080/wss/receptor-controller/gateway", "http service address")

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var headerFlags arrayFlags

var identity = `{ "identity": {"account_number": "01", "type": "User", "internal": { "org_id": "1979710" } } }`
var headers = map[string][]string{
	//	"x-rh-identity": {base64.StdEncoding.EncodeToString([]byte(identity))},
}

func main() {

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	flag.Var(&headerFlags, "header", "header name:value")
	connectionCount := flag.Int("connection_count", 1, "number of connections to create")
	flag.Parse()

	u, err := url.Parse(*targetUrl)
	if err != nil {
		log.Fatal("unable to parse url:", err)
	}
	log.Printf("connecting to %s\n", u.String())

	for _, s := range headerFlags {
		nameValuePair := strings.Split(s, ":")
		headers[nameValuePair[0]] = []string{nameValuePair[1]}
	}

	// FIXME: don't ignore ctx
	_, cancel := context.WithCancel(context.Background())

	for i := 0; i < *connectionCount; i++ {
		go func(i int) {
			c, _, err := websocket.DefaultDialer.Dial(u.String(), headers)
			log.Println("connected")
			if err != nil {
				log.Fatal("dial:", err)
				return
			}
			defer c.Close()

			nodeID := fmt.Sprintf("node-%d", i)

			hiMessage := protocol.HiMessage{Command: "HI", ID: nodeID}

			writeSocket(c, &hiMessage)

			_ = readSocket(c, 1)
		}(i)
	}

	<-c
	cancel()

}
