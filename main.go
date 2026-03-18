package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Config holds the application configuration
type Config struct {
	RabbitMQURL        string
	RabbitMQQueue      string
	RabbitMQExchange   string
	RabbitMQRoutingKey string
	Port               string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	config := &Config{
		RabbitMQURL:        os.Getenv("RABBITMQ_URL"),
		RabbitMQQueue:      os.Getenv("RABBITMQ_QUEUE"),
		RabbitMQExchange:   os.Getenv("RABBITMQ_EXCHANGE"),
		RabbitMQRoutingKey: os.Getenv("RABBITMQ_ROUTING_KEY"),
		Port:               os.Getenv("PORT"),
	}

	// Set defaults if not provided
	if config.RabbitMQURL == "" {
		config.RabbitMQURL = "amqp://guest:guest@localhost:5672/"
	}
	if config.RabbitMQQueue == "" {
		config.RabbitMQQueue = "smoker_temps_queue"
	}
	if config.RabbitMQExchange == "" {
		config.RabbitMQExchange = "smoker_exchange"
	}
	if config.RabbitMQRoutingKey == "" {
		config.RabbitMQRoutingKey = "smoker.temps"
	}
	if config.Port == "" {
		config.Port = "8080"
	}

	return config
}

// ProbeData represents an individual temperature probe reading
type ProbeData struct {
	ID int      `json:"id"`
	T  *float64 `json:"t"`
}

// SmokerPayload represents the main telemetry packet
type SmokerPayload struct {
	Device string      `json:"device"`
	Data   []ProbeData `json:"data"`
}

// Broker manages connected clients and broadcasts messages
type Broker struct {
	// Events are pushed to this channel by the main events-gathering routine
	Notifier chan []byte

	// New client connections
	newClients chan chan []byte

	// Closed client connections
	closingClients chan chan []byte

	// Client connections registry
	clients map[chan []byte]bool

	// Mutex for synchronizing access to clients map
	mu sync.Mutex
}

func NewBroker() *Broker {
	broker := &Broker{
		Notifier:       make(chan []byte, 1),
		newClients:     make(chan chan []byte),
		closingClients: make(chan chan []byte),
		clients:        make(map[chan []byte]bool),
	}

	// Set it running - listening and broadcasting events
	go broker.listen()

	return broker
}

func (broker *Broker) listen() {
	for {
		select {
		case s := <-broker.newClients:
			// A new client has connected.
			// Register their message channel
			broker.mu.Lock()
			broker.clients[s] = true
			broker.mu.Unlock()
			log.Printf("Client added. %d registered clients", len(broker.clients))
		case s := <-broker.closingClients:
			// A client has detached and we want to stop sending them messages.
			broker.mu.Lock()
			delete(broker.clients, s)
			broker.mu.Unlock()
			log.Printf("Removed client. %d registered clients", len(broker.clients))
		case event := <-broker.Notifier:
			// We got a new event from the outside!
			// Send event to all connected clients
			broker.mu.Lock()
			for clientMessageChan := range broker.clients {
				select {
				case clientMessageChan <- event:
				default:
					// if we can't send immediately, drop the message for this client
					log.Printf("Failed to send message to client, dropping it")
				}
			}
			broker.mu.Unlock()
		}
	}
}

func (broker *Broker) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	// Make sure that the writer supports flushing.
	flusher, ok := rw.(http.Flusher)
	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	// Each connection registers its own message channel with the Broker's connections registry
	messageChan := make(chan []byte, 10)

	// Signal the broker that we have a new connection
	broker.newClients <- messageChan

	// Listen to connection close and un-register messageChan
	notify := req.Context().Done()

	// block waiting for messages broadcast on this connection's messageChan
	for {
		select {
		case <-notify:
			broker.closingClients <- messageChan
			return
		case msg := <-messageChan:
			// Write to the ResponseWriter
			// Server Sent Events compatible
			fmt.Fprintf(rw, "data: %s\n\n", msg)

			// Flush the data immediately instead of buffering it for later.
			flusher.Flush()
		}
	}
}

const htmlPage = `
<!DOCTYPE html>
<html>
<head>
    <title>ESP32 Smoker Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: sans-serif; margin: 20px; }
        canvas { max-width: 800px; max-height: 400px; }
        .container { display: flex; flex-direction: column; align-items: center; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Masterbuilt Smoker Monitor</h1>
        <p>Device: <span id="deviceId">Waiting for data...</span></p>
        <canvas id="tempChart"></canvas>
    </div>

    <script>
        const ctx = document.getElementById('tempChart').getContext('2d');
        const maxDataPoints = 60; // Keep last 60 points

        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    { label: 'Probe 1', data: [], borderColor: 'red', fill: false },
                    { label: 'Probe 2', data: [], borderColor: 'blue', fill: false },
                    { label: 'Probe 3', data: [], borderColor: 'green', fill: false },
                    { label: 'Probe 4', data: [], borderColor: 'orange', fill: false }
                ]
            },
            options: {
                responsive: true,
                animation: { duration: 0 },
                scales: {
                    x: { display: true, title: { display: true, text: 'Time' } },
                    y: { display: true, title: { display: true, text: 'Temperature (°C)' } }
                }
            }
        });

        const evtSource = new EventSource("/events");
        evtSource.onmessage = function(event) {
            const payload = JSON.parse(event.data);

            document.getElementById('deviceId').innerText = payload.device;

            const now = new Date();
            const timeStr = now.getHours().toString().padStart(2, '0') + ':' +
                            now.getMinutes().toString().padStart(2, '0') + ':' +
                            now.getSeconds().toString().padStart(2, '0');

            chart.data.labels.push(timeStr);
            if (chart.data.labels.length > maxDataPoints) {
                chart.data.labels.shift();
            }

            payload.data.forEach(probe => {
                if (probe.id >= 1 && probe.id <= 4) {
                    const dataset = chart.data.datasets[probe.id - 1];
                    const temp = probe.t !== null ? probe.t : null;
                    dataset.data.push(temp);
                    if (dataset.data.length > maxDataPoints) {
                        dataset.data.shift();
                    }
                }
            });

            chart.update();
        };

        evtSource.onerror = function() {
            console.error("EventSource failed.");
        };
    </script>
</body>
</html>
`

func serveIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(htmlPage))
}

func main() {
	config := LoadConfig()

	// Setup SSE Broker
	broker := NewBroker()

	log.Printf("Connecting to RabbitMQ at %s...", config.RabbitMQURL)
	conn, err := amqp.Dial(config.RabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Declare Exchange
	err = ch.ExchangeDeclare(
		config.RabbitMQExchange, // name
		"topic",                 // type
		true,                    // durable
		false,                   // auto-deleted
		false,                   // internal
		false,                   // no-wait
		nil,                     // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare an exchange: %v", err)
	}

	// Declare Queue
	q, err := ch.QueueDeclare(
		config.RabbitMQQueue, // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	// Bind Queue
	err = ch.QueueBind(
		q.Name,                     // queue name
		config.RabbitMQRoutingKey,  // routing key
		config.RabbitMQExchange,    // exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind a queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	log.Printf("Waiting for messages on queue %s. To exit press CTRL+C", q.Name)

	go func() {
		for d := range msgs {
			var payload SmokerPayload
			if err := json.Unmarshal(d.Body, &payload); err != nil {
				log.Printf("Error decoding JSON: %v", err)
				continue
			}

			// Broadcast the JSON via SSE
			broker.Notifier <- d.Body

			log.Printf("Forwarded payload from device %s", payload.Device)
		}
	}()

	// Setup HTTP server
	http.HandleFunc("/", serveIndex)
	http.Handle("/events", broker)

	log.Printf("Starting HTTP server on port %s...", config.Port)
	if err := http.ListenAndServe(":"+config.Port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
