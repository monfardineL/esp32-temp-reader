# ESP32 Temperature Reader

This is a Go application that connects to a RabbitMQ broker, reads temperature probe data sent by an ESP32 microcontroller, stores it persistently in a TimescaleDB (PostgreSQL) database, and provides a near real-time web dashboard.

Data is stored efficiently into the database while the dashboard is powered by an in-memory 1-hour rolling cache. Web clients automatically poll the local cache every 10 seconds via a REST API, ensuring the database is never overloaded by high web traffic.

The application expects JSON payloads matching the schema defined in `OpenAPISpec.yaml`.

## Configuration

The application is configured entirely through environment variables.

| Variable | Description | Default |
| --- | --- | --- |
| `RABBITMQ_URL` | The AMQP connection string to connect to RabbitMQ | `amqp://guest:guest@localhost:5672/` |
| `RABBITMQ_QUEUE` | The queue name to declare and consume from | `smoker_temps_queue` |
| `RABBITMQ_EXCHANGE` | The exchange name to declare and bind the queue to | `smoker_exchange` |
| `RABBITMQ_ROUTING_KEY` | The routing key used to bind the queue to the exchange | `smoker.temps` |
| `PORT` | The HTTP port the web dashboard will listen on | `8080` |
| `DB_URL` | PostgreSQL connection string | `postgres://postgres:postgres@localhost:5432/smoker?sslmode=disable` |

## Running with Docker Compose (Recommended)

The easiest way to run the entire stack (Application, RabbitMQ, and TimescaleDB) is using `docker-compose`.

```bash
docker-compose up -d
```

This will automatically create a PostgreSQL/TimescaleDB container, a RabbitMQ container, and the Go application, wiring them together automatically.

You can then access:
- **Web Dashboard**: `http://localhost:8080`
- **RabbitMQ Management UI**: `http://localhost:15672` (login: `guest` / `guest`)
- **Database**: `localhost:5432` (user `smoker` / pass `smokerpass`)

## Running Locally

1. Ensure you have Go 1.24+ installed.
2. Ensure you have a RabbitMQ and PostgreSQL instances running.
3. Install dependencies:
   ```bash
   go mod download
   ```
4. Run the application:
   ```bash
   RABBITMQ_URL="amqp://user:pass@localhost:5672/" DB_URL="postgres://user:pass@localhost:5432/db" PORT=8080 go run main.go
   ```
5. Open your browser and navigate to `http://localhost:8080` to view the live temperature chart.

## Running the Application Docker Container

You can also run this application standalone via Docker. The image is automatically built and pushed to the GitHub Container Registry.

Run the Docker container using the pre-built image, passing the required environment variables:
```bash
docker run -d -p 8080:8080 \
  -e RABBITMQ_URL="amqp://user:pass@your-rabbitmq-host:5672/" \
  -e DB_URL="postgres://user:pass@your-db-host:5432/db" \
  -e PORT="8080" \
  ghcr.io/yourusername/esp32-temp-reader:main
```
*(Replace `yourusername` with the repository owner).*

## Development

- `main.go`: Contains the core application logic including configuration loading, database connections, caching mechanism, RabbitMQ consumption, and the HTTP server serving the embedded HTML dashboard and REST API.
- `OpenAPISpec.yaml`: The schema definition for the messages sent by the ESP32.
- `.github/workflows/build.yml`: CI workflow for building and testing the application and Docker image.
