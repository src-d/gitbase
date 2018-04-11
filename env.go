package gitbase

import (
	"os"
	"strconv"
)

func getIntEnv(key string, defaultValue int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	v, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}
	return v
}

func getBoolEnv(key string, defaultValue bool) bool {
	_, ok := os.LookupEnv(key)
	if ok {
		return true
	}

	return defaultValue
}

func getStringEnv(key string, defaultValue string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		return defaultValue
	}
	return v
}
