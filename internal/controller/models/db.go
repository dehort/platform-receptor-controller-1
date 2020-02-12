package models

import (
	"database/sql"
	_ "github.com/lib/pq"
)

func NewDB(dataSourceName string) (*sql.DB, error) {
	//dataSourceName = "postgres://user:pass@localhost:5432/fifi"
	dataSourceName = "host=localhost port=5432 user=fifi password=fifi dbname=fifi sslmode=disable"
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}
