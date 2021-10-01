CREATE TABLE IF NOT EXISTS kafka_consumer_retries(
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    topic VARCHAR (255) NOT NULL,
    batch_id CHAR(36) NULL,
    retry_started_at timestamp NULL,
    retry_completed_at timestamp NULL,
    payload_json JSON NOT NULL,
    payload_headers JSON NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_partition INT NOT NULL,
    payload_key VARCHAR(255) NOT NULL,
    attempts TINYINT NOT NULL DEFAULT 1,
    deadlettered BOOLEAN NOT NULL DEFAULT false,
    successful BOOLEAN NOT NULL DEFAULT false,
    last_error VARCHAR (255) NOT NULL DEFAULT '',
    errored BOOLEAN NOT NULL DEFAULT false,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
