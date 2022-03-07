package data

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const retryAttempts = 10

func NewDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)

	if err != nil {
		return nil, fmt.Errorf("unable to connect to the database: %w", err)
	}

	tries := retryAttempts
	for {
		err := db.Ping()
		if err == nil {
			break
		}

		time.Sleep(time.Second * 1)
		tries--

		if tries == 0 {
			return nil, fmt.Errorf("database did not become available within %d connection attempts", retryAttempts)
		}
	}

	return db, nil
}
