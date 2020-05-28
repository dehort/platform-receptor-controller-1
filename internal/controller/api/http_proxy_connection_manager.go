package api

import (
	"github.com/RedHatInsights/platform-receptor-controller/internal/controller"
	"github.com/RedHatInsights/platform-receptor-controller/internal/platform/logger"
	"os"
)

type ConnectionManagerHttpProxy struct {
}

func (cm *ConnectionManagerHttpProxy) Register(account string, node_id string, client controller.Receptor) error {
	logger.Log.Printf("FIXME: (NO-OP) Registered a connection (%s, %s)\n", account, node_id)
	return nil
}

func (cm *ConnectionManagerHttpProxy) Unregister(account string, node_id string) {
	logger.Log.Printf("FIXME: (NO-OP) Unregistered a connection (%s, %s)\n", account, node_id)
}

func (cm *ConnectionManagerHttpProxy) GetConnection(account string, node_id string) controller.Receptor {
	var conn controller.Receptor

	var url string
	url = os.Getenv("GATEWAY_URL")
	if len(url) == 0 {
		logger.Log.Printf("GATEWAY_URL env var is not set\n")
	}
	logger.Log.Printf("GATEWAY_URL: %s\n", url)

	conn = &ReceptorHttpProxy{Url: url, AccountNumber: account, NodeID: node_id}

	return conn
}

func (cm *ConnectionManagerHttpProxy) GetConnectionsByAccount(account string) map[string]controller.Receptor {

	connectionsPerAccount := make(map[string]controller.Receptor)

	nodeId := "fred"
	connectionsPerAccount[nodeId] = cm.GetConnection("0000001", nodeId)

	return connectionsPerAccount
}

func (cm *ConnectionManagerHttpProxy) GetAllConnections() map[string]map[string]controller.Receptor {

	connectionMap := make(map[string]map[string]controller.Receptor)

	accountNumber := "0000001"

	connectionMap[accountNumber] = cm.GetConnectionsByAccount(accountNumber)

	return connectionMap
}
