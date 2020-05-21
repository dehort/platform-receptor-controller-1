package controller

import (
	"github.com/RedHatInsights/platform-receptor-controller/internal/platform/logger"
)

type ConnectionManagerHttpProxy struct {
}

func (cm *ConnectionManagerHttpProxy) Register(account string, node_id string, client Receptor) error {
	logger.Log.Printf("FIXME: (NO-OP) Registered a connection (%s, %s)", account, node_id)
	return nil
}

func (cm *ConnectionManagerHttpProxy) Unregister(account string, node_id string) {
	logger.Log.Printf("FIXME: (NO-OP) Unregistered a connection (%s, %s)", account, node_id)
}

func (cm *ConnectionManagerHttpProxy) GetConnection(account string, node_id string) Receptor {
	var conn Receptor

	conn = &ReceptorHttpProxy{Url: "http://localhost:9090/job"}

	return conn
}

func (cm *ConnectionManagerHttpProxy) GetConnectionsByAccount(account string) map[string]Receptor {

	connectionsPerAccount := make(map[string]Receptor)

	nodeId := "fred"
	connectionsPerAccount[nodeId] = cm.GetConnection("0000001", nodeId)

	return connectionsPerAccount
}

func (cm *ConnectionManagerHttpProxy) GetAllConnections() map[string]map[string]Receptor {

	connectionMap := make(map[string]map[string]Receptor)

	accountNumber := "0000001"

	connectionMap[accountNumber] = cm.GetConnectionsByAccount(accountNumber)

	return connectionMap
}
