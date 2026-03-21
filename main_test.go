package main

import (
	"encoding/json"
	"os"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	// Clear any existing env vars for isolation
	os.Clearenv()

	config := LoadConfig()

	if config.RabbitMQURL != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("Expected default RabbitMQURL, got %s", config.RabbitMQURL)
	}
	if config.RabbitMQQueue != "smoker_temps_queue" {
		t.Errorf("Expected default RabbitMQQueue, got %s", config.RabbitMQQueue)
	}
	if config.RabbitMQExchange != "smoker_exchange" {
		t.Errorf("Expected default RabbitMQExchange, got %s", config.RabbitMQExchange)
	}
	if config.RabbitMQRoutingKey != "smoker.temps" {
		t.Errorf("Expected default RabbitMQRoutingKey, got %s", config.RabbitMQRoutingKey)
	}
	if config.Port != "8080" {
		t.Errorf("Expected default Port, got %s", config.Port)
	}
	if config.DBURL != "postgres://postgres:postgres@localhost:5432/smoker?sslmode=disable" {
		t.Errorf("Expected default DBURL, got %s", config.DBURL)
	}
}

func TestLoadConfigEnvVars(t *testing.T) {
	os.Setenv("RABBITMQ_URL", "amqp://user:pass@remote:5672/")
	os.Setenv("RABBITMQ_QUEUE", "custom_queue")
	os.Setenv("RABBITMQ_EXCHANGE", "custom_exchange")
	os.Setenv("RABBITMQ_ROUTING_KEY", "custom.key")
	os.Setenv("PORT", "9090")
	os.Setenv("DB_URL", "postgres://user:pass@remote:5432/db?sslmode=require")
	defer os.Clearenv()

	config := LoadConfig()

	if config.RabbitMQURL != "amqp://user:pass@remote:5672/" {
		t.Errorf("Expected overridden RabbitMQURL, got %s", config.RabbitMQURL)
	}
	if config.RabbitMQQueue != "custom_queue" {
		t.Errorf("Expected overridden RabbitMQQueue, got %s", config.RabbitMQQueue)
	}
	if config.RabbitMQExchange != "custom_exchange" {
		t.Errorf("Expected overridden RabbitMQExchange, got %s", config.RabbitMQExchange)
	}
	if config.RabbitMQRoutingKey != "custom.key" {
		t.Errorf("Expected overridden RabbitMQRoutingKey, got %s", config.RabbitMQRoutingKey)
	}
	if config.Port != "9090" {
		t.Errorf("Expected overridden Port, got %s", config.Port)
	}
	if config.DBURL != "postgres://user:pass@remote:5432/db?sslmode=require" {
		t.Errorf("Expected overridden DBURL, got %s", config.DBURL)
	}
}

func TestSmokerPayloadDecoding(t *testing.T) {
	jsonPayload := []byte(`{
		"device": "smoker-01",
		"ts": 1710345600,
		"data": [
			{"id": 1, "t": 22.5},
			{"id": 2, "t": 23.1},
			{"id": 3, "t": null},
			{"id": 4, "t": 21.0}
		]
	}`)

	var payload SmokerPayload
	err := json.Unmarshal(jsonPayload, &payload)
	if err != nil {
		t.Fatalf("Failed to decode payload: %v", err)
	}

	if payload.Device != "smoker-01" {
		t.Errorf("Expected device 'smoker-01', got %s", payload.Device)
	}

	if payload.TS != 1710345600 {
		t.Errorf("Expected ts 1710345600, got %d", payload.TS)
	}

	if len(payload.Data) != 4 {
		t.Fatalf("Expected 4 data points, got %d", len(payload.Data))
	}

	if *payload.Data[0].T != 22.5 {
		t.Errorf("Expected probe 1 temp 22.5, got %v", *payload.Data[0].T)
	}

	if payload.Data[2].T != nil {
		t.Errorf("Expected probe 3 temp to be nil, got %v", *payload.Data[2].T)
	}
}
