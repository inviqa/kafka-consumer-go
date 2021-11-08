package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func defaultTopicNameGenerator(group, topic, prefix string) string {
	return fmt.Sprintf("%s.%s.%s", prefix, group, topic)
}

func envVarAsBool(name string) bool {
	truthy := map[string]bool{
		"true": true,
		"1":    true,
		"on":   true,
	}

	return truthy[strings.ToLower(os.Getenv(name))]
}

func envVarAsIntSlice(name string) ([]int, error) {
	envVal := os.Getenv(name)
	if envVal == "" {
		return []int{}, nil
	}

	var i []int
	for _, pt := range strings.Split(envVal, ",") {
		val, err := strconv.Atoi(strings.TrimSpace(pt))
		if err != nil {
			return nil, err
		}
		i = append(i, val)
	}
	return i, nil
}

func envVarAsStringSlice(name string) []string {
	envVal := os.Getenv(name)
	if envVal == "" {
		return []string{}
	}

	return strings.Split(envVal, ",")
}

func envVarAsInt(name string) int {
	val, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil {
		return 0
	}
	return val
}
