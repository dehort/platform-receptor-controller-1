package controller

import (
	"errors"
	"github.com/RedHatInsights/platform-receptor-controller/internal/controller/models"
)

var (
	errNoRoute = errors.New("no route to target node")
)

type Route struct {
	FirstHop string
}

func CalculateRoute(connections []*models.Connection, recipient string) (*Route, error) {
	if connections == nil || len(connections) == 0 {
		return nil, errNoRoute
	}

	for _, conn := range connections {
		if recipient == conn.NodeID {
			return &Route{conn.NodeID}, nil
		}
	}

	return nil, errNoRoute
}
