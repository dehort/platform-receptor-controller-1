package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	//"time"
	//"encoding/base64"

	"github.com/RedHatInsights/platform-receptor-controller/internal/receptor/protocol"
	"github.com/gorilla/websocket"
)

func readSocket(c *websocket.Conn, mt protocol.NetworkMessageType) protocol.Message {
	mtype, r, _ := c.NextReader()
	fmt.Println("mtype:", mtype)
	//Expect(mtype).Should(Equal(websocket.BinaryMessage))

	fmt.Println("TestClient reading response from receptor-controller")
	m, _ := protocol.ReadMessage(r)
	fmt.Println("m:", m)
	//Expect(m.Type()).Should(Equal(mt))

	return m
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
	flag.Var(&headerFlags, "header", "header name:value")
	flag.Parse()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u, err := url.Parse(*targetUrl)
	if err != nil {
		log.Fatal("unable to parse url:", err)
	}
	log.Printf("connecting to %s\n", u.String())

	for _, s := range headerFlags {
		nameValuePair := strings.Split(s, ":")
		headers[nameValuePair[0]] = []string{nameValuePair[1]}
	}

	c, _, err := websocket.DefaultDialer.Dial(u.String(), headers)
	log.Println("connected")
	if err != nil {
		log.Fatal("dial:", err)
		return
	}
	defer c.Close()

	hiMessage := protocol.HiMessage{Command: "HI", ID: "fred"}

	writeSocket(c, &hiMessage)

	_ = readSocket(c, 1)

}
