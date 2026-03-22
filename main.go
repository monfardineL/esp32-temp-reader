package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Config holds the application configuration
type Config struct {
	RabbitMQURL   string
	RabbitMQQueue string
	Port          string
	DBURL         string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	config := &Config{
		RabbitMQURL:   os.Getenv("RABBITMQ_URL"),
		RabbitMQQueue: os.Getenv("RABBITMQ_QUEUE"),
		Port:          os.Getenv("PORT"),
		DBURL:         os.Getenv("DB_URL"),
	}

	// Set defaults if not provided
	if config.RabbitMQURL == "" {
		config.RabbitMQURL = "amqp://guest:guest@localhost:5672/"
	}
	if config.RabbitMQQueue == "" {
		config.RabbitMQQueue = "smoker_temps_queue"
	}
	if config.Port == "" {
		config.Port = "8080"
	}
	if config.DBURL == "" {
		config.DBURL = "postgres://postgres:postgres@localhost:5432/smoker?sslmode=disable"
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
	TS     int64       `json:"ts"`
	Data   []ProbeData `json:"data"`
}

func initDB(dbURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, err
	}

	// Test the connection
	if err = db.Ping(); err != nil {
		return nil, err
	}

	// Create table
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS smoker_data (
		time TIMESTAMPTZ NOT NULL,
		device TEXT NOT NULL,
		probe1 DOUBLE PRECISION,
		probe2 DOUBLE PRECISION,
		probe3 DOUBLE PRECISION,
		probe4 DOUBLE PRECISION
	);
	`
	if _, err := db.Exec(createTableQuery); err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	// Convert to TimescaleDB hypertable if not already
	hypertableQuery := `
	SELECT create_hypertable('smoker_data', by_range('time'), if_not_exists => TRUE);
	`
	if _, err := db.Exec(hypertableQuery); err != nil {
		// Log the error but don't fail, in case we're testing without TimescaleDB extension
		log.Printf("Note: failed to create hypertable (is TimescaleDB installed?): %v", err)
	}

	return db, nil
}

func insertPayload(db *sql.DB, payload SmokerPayload) error {
	var p1, p2, p3, p4 *float64
	for _, p := range payload.Data {
		switch p.ID {
		case 1:
			p1 = p.T
		case 2:
			p2 = p.T
		case 3:
			p3 = p.T
		case 4:
			p4 = p.T
		}
	}

	ts := time.Unix(payload.TS, 0)
	query := `
		INSERT INTO smoker_data (time, device, probe1, probe2, probe3, probe4)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := db.Exec(query, ts, payload.Device, p1, p2, p3, p4)
	return err
}

type CachedDataPoint struct {
	TS     int64    `json:"ts"`
	Device string   `json:"device"`
	Probe1 *float64 `json:"probe1"`
	Probe2 *float64 `json:"probe2"`
	Probe3 *float64 `json:"probe3"`
	Probe4 *float64 `json:"probe4"`
}

type DataCache struct {
	sync.RWMutex
	Points []CachedDataPoint
}

func NewDataCache() *DataCache {
	return &DataCache{
		Points: make([]CachedDataPoint, 0),
	}
}

func (c *DataCache) AddAndEvict(p CachedDataPoint, duration time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.Points = append(c.Points, p)

	// Evict older data
	cutoff := time.Now().Add(-duration).Unix()
	var newPoints []CachedDataPoint
	for _, pt := range c.Points {
		if pt.TS >= cutoff {
			newPoints = append(newPoints, pt)
		}
	}
	c.Points = newPoints
}

func (c *DataCache) GetPoints() []CachedDataPoint {
	c.RLock()
	defer c.RUnlock()

	// Return a copy to avoid race conditions
	cpy := make([]CachedDataPoint, len(c.Points))
	copy(cpy, c.Points)
	return cpy
}

func loadCacheFromDB(db *sql.DB, cache *DataCache, duration time.Duration) error {
	cutoff := time.Now().Add(-duration)

	query := `
		SELECT time, device, probe1, probe2, probe3, probe4
		FROM smoker_data
		WHERE time >= $1
		ORDER BY time ASC
	`

	rows, err := db.Query(query, cutoff)
	if err != nil {
		return err
	}
	defer rows.Close()

	cache.Lock()
	defer cache.Unlock()
	cache.Points = make([]CachedDataPoint, 0)

	for rows.Next() {
		var t time.Time
		var d string
		var p1, p2, p3, p4 *float64
		if err := rows.Scan(&t, &d, &p1, &p2, &p3, &p4); err != nil {
			return err
		}

		cache.Points = append(cache.Points, CachedDataPoint{
			TS:     t.Unix(),
			Device: d,
			Probe1: p1,
			Probe2: p2,
			Probe3: p3,
			Probe4: p4,
		})
	}

	return rows.Err()
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

        async function fetchData() {
            try {
                const response = await fetch('/api/temps');
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                const data = await response.json();

                if (data.length > 0) {
                    document.getElementById('deviceId').innerText = data[data.length - 1].device;
                }

                const labels = [];
                const d1 = [], d2 = [], d3 = [], d4 = [];

                data.forEach(pt => {
                    const date = new Date(pt.ts * 1000);
                    const timeStr = date.getHours().toString().padStart(2, '0') + ':' +
                                    date.getMinutes().toString().padStart(2, '0') + ':' +
                                    date.getSeconds().toString().padStart(2, '0');
                    labels.push(timeStr);
                    d1.push(pt.probe1);
                    d2.push(pt.probe2);
                    d3.push(pt.probe3);
                    d4.push(pt.probe4);
                });

                chart.data.labels = labels;
                chart.data.datasets[0].data = d1;
                chart.data.datasets[1].data = d2;
                chart.data.datasets[2].data = d3;
                chart.data.datasets[3].data = d4;

                chart.update();
            } catch (error) {
                console.error("Failed to fetch data:", error);
            }
        }

        // Fetch immediately, then every 10 seconds
        fetchData();
        setInterval(fetchData, 10000);
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

	// Initialize Database
	log.Printf("Connecting to Database at %s...", config.DBURL)
	db, err := initDB(config.DBURL)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Initialize Data Cache (1 hour)
	cacheDuration := 1 * time.Hour
	cache := NewDataCache()
	log.Printf("Loading last %v of data from database...", cacheDuration)
	if err := loadCacheFromDB(db, cache, cacheDuration); err != nil {
		log.Printf("Error loading cache from DB: %v", err)
	} else {
		log.Printf("Loaded %d data points into cache", len(cache.GetPoints()))
	}

	// Start RabbitMQ worker in the background
	go runRabbitMQWorker(config, db, cache, cacheDuration)

	// Setup HTTP server
	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/api/temps", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(cache.GetPoints())
	})

	log.Printf("Starting HTTP server on port %s...", config.Port)
	if err := http.ListenAndServe(":"+config.Port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}

func runRabbitMQWorker(config *Config, db *sql.DB, cache *DataCache, cacheDuration time.Duration) {
	for {
		log.Printf("Connecting to RabbitMQ at %s...", config.RabbitMQURL)
		conn, err := amqp.Dial(config.RabbitMQURL)
		if err != nil {
			log.Printf("Failed to connect to RabbitMQ: %v. Retrying in 10s...", err)
			time.Sleep(10 * time.Second)
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			log.Printf("Failed to open a channel: %v. Retrying in 10s...", err)
			conn.Close()
			time.Sleep(10 * time.Second)
			continue
		}

		// Declare Queue
		q, err := ch.QueueDeclare(
			config.RabbitMQQueue, true, false, false, false, nil,
		)
		if err != nil {
			log.Printf("Failed to declare queue: %v. Retrying in 10s...", err)
			ch.Close()
			conn.Close()
			time.Sleep(10 * time.Second)
			continue
		}

		msgs, err := ch.Consume(
			q.Name, "", true, false, false, false, nil,
		)
		if err != nil {
			log.Printf("Failed to register consumer: %v. Retrying in 10s...", err)
			ch.Close()
			conn.Close()
			time.Sleep(10 * time.Second)
			continue
		}

		log.Printf("RabbitMQ Connected. Waiting for messages on queue %s.", q.Name)

		// Monitor connection health
		closeChan := conn.NotifyClose(make(chan *amqp.Error))

		processLoop:
		for {
			select {
			case err := <-closeChan:
				if err != nil {
					log.Printf("RabbitMQ connection closed: %v", err)
				}
				break processLoop
			case d, ok := <-msgs:
				if !ok {
					log.Print("RabbitMQ message channel closed")
					break processLoop
				}

				var payload SmokerPayload
				if err := json.Unmarshal(d.Body, &payload); err != nil {
					log.Printf("Error decoding JSON: %v", err)
					continue
				}

				// Insert into DB
				if err := insertPayload(db, payload); err != nil {
					log.Printf("Failed to insert payload into DB: %v", err)
				}

				// Add to Cache
				var p1, p2, p3, p4 *float64
				for _, p := range payload.Data {
					switch p.ID {
					case 1: p1 = p.T
					case 2: p2 = p.T
					case 3: p3 = p.T
					case 4: p4 = p.T
					}
				}
				cache.AddAndEvict(CachedDataPoint{
					TS:     payload.TS,
					Device: payload.Device,
					Probe1: p1,
					Probe2: p2,
					Probe3: p3,
					Probe4: p4,
				}, cacheDuration)

				log.Printf("Processed payload from device %s at %d", payload.Device, payload.TS)
			}
		}

		ch.Close()
		conn.Close()
		log.Print("RabbitMQ worker disconnected. Reconnecting in 10s...")
		time.Sleep(10 * time.Second)
	}
}
