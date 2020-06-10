package api

import (
	"fmt"

	"github.com/RedHatInsights/platform-receptor-controller/internal/config"
	"github.com/RedHatInsights/platform-receptor-controller/internal/controller"
	"github.com/RedHatInsights/platform-receptor-controller/internal/platform/logger"
	"github.com/go-redis/redis"
)

type RedisConnectionLocator struct {
	Client *redis.Client
	Cfg    *config.Config
}

func (rcl *RedisConnectionLocator) GetConnection(account string, node_id string) controller.Receptor {
	var conn controller.Receptor
	var podName string
	var err error

	if podName, err = controller.GetRedisConnection(rcl.Client, account, node_id); err != nil {
		// FIXME: log error, return an error
		return nil
	}

	if podName == "" {
		return nil
	}

	url := fmt.Sprintf("%s://%s:%s", rcl.Cfg.ReceptorProxyScheme, podName, rcl.Cfg.ReceptorProxyPort)

	conn = &ReceptorHttpProxy{
		Url:           url,
		AccountNumber: account,
		NodeID:        node_id,
		Config:        rcl.Cfg,
	}

	return conn
}

func (rcl *RedisConnectionLocator) GetConnectionsByAccount(account string) map[string]controller.Receptor {

	connectionsPerAccount := make(map[string]controller.Receptor)

	accountConnections, err := controller.GetRedisConnectionsByAccount(rcl.Client, account)
	if err != nil {
		// FIXME: Update connectionlocator interface methods to return error
		logger.Log.Warnf("Error during lookup for account: %s", account)
		return nil
	}

	for nodeID, _ := range accountConnections {
		proxy := rcl.GetConnection(account, nodeID)
		connectionsPerAccount[nodeID] = proxy
	}

	return connectionsPerAccount
}

func (rcl *RedisConnectionLocator) GetAllConnections() map[string]map[string]controller.Receptor {

	connectionMap := make(map[string]map[string]controller.Receptor)

	connections, err := controller.GetAllRedisConnections(rcl.Client)
	if err != nil {
		// FIXME: Update connectionlocator interface methods to return error
		logger.Log.Warn("Error during lookup for all connections")
		return nil
	}

	for account, conn := range connections {
		if _, exists := connectionMap[account]; !exists {
			connectionMap[account] = make(map[string]controller.Receptor)
		}
		for node, _ := range conn {
			proxy := rcl.GetConnection(account, node)
			connectionMap[account][node] = proxy
		}
	}

	return connectionMap
}