#!/usr/bin/env bash

docker-compose down && docker-compose up -d
echo "waiting for stack..."
sleep 5

LOG_LEVEL=error go test -timeout=100s -count=1 -v --tags=integration ./integration/
