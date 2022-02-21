package data

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/inviqa/kafka-consumer-go/log"
)

const retryAttempts = 10

func NewDB(dsn string, logger log.Logger) (*sql.DB, error) {
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
		logger.Infof("database is not available (err: %s), retrying %d more time(s)", err, tries)

		if tries == 0 {
			return nil, fmt.Errorf("database did not become available within %d connection attempts", retryAttempts)
		}
	}

	return db, nil
}
