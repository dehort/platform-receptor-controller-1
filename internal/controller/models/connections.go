package models

import (
	"database/sql"
	"log"
)

type Connection struct {
	AccountNumber string
	NodeID        string
	RouteTable    interface{}
	PodName       string
}

func AllConnectionsPerAccount(db *sql.DB, accountNumber string) ([]*Connection, error) {
	rows, err := db.Query("SELECT * FROM connections ") // WHERE AccountNumber = ?", accountNumber)
	if err != nil {
		log.Println("Query error! ", err)
		return nil, err
	}
	defer rows.Close()

	connections := make([]*Connection, 0)
	for rows.Next() {
		conn := new(Connection)
		var id int
		err := rows.Scan(&id, &conn.AccountNumber, &conn.NodeID, &conn.PodName, &conn.RouteTable)
		if err != nil {
			return nil, err
		}
		connections = append(connections, conn)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return connections, nil
}

func SaveConnection(db *sql.DB, conn Connection) error {
	// FIXME: remember to add route_table
	query, err := db.Prepare("INSERT INTO connections (account_number, node_id, route_table, pod_name) VALUES ($1, $2, $3, $4)")
	if err != nil {
		return err
	}

	_, err = query.Exec(conn.AccountNumber, conn.NodeID, conn.RouteTable, conn.PodName)
	if err != nil {
		return err
	}

	defer query.Close()
	return nil
}
