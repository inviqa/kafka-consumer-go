CREATE INDEX IF NOT EXISTS topic_attempts_idx ON kafka_consumer_retries (topic, attempts);
CREATE INDEX IF NOT EXISTS batch_id_idx ON kafka_consumer_retries (batch_id);