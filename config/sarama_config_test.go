package config

import (
	"os"
	"testing"
)

func TestNewSaramaConfig(t *testing.T) {
	cfg := NewSaramaConfig(true, false)
	if nil == cfg {
		t.Fatal("expected config, but got nil")
	}

	expClient, _ := os.Hostname()
	if cfg.ClientID != expClient {
		t.Error("clientId not set on Sarama config")
	}
}
