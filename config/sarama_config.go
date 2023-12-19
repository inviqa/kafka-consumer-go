package config

import (
	"crypto/tls"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

func NewSaramaConfig(tlsEnable bool, tlsSkipVerify bool) *sarama.Config {
	cfg := sarama.NewConfig()

	host, _ := os.Hostname()

	cfg.ClientID = host
	cfg.Version = sarama.V2_4_0_0
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit = struct {
		Enable   bool
		Interval time.Duration
	}{Enable: false}

	cfg.Producer.Return.Successes = true

	if tlsEnable {
		cfg.Net.TLS.Enable = true
		// #nosec G402
		cfg.Net.TLS.Config = &tls.Config{InsecureSkipVerify: tlsSkipVerify}
	}

	return cfg
}
