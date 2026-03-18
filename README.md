# ESP32 Temperature Reader

This is a Go application that connects to a RabbitMQ broker, reads temperature probe data sent by an ESP32 microcontroller, and provides a real-time web dashboard using Server-Sent Events (SSE) and Chart.js to plot the temperatures.

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

## Running Locally

1. Ensure you have Go 1.24+ installed.
2. Ensure you have a RabbitMQ instance running.
3. Install dependencies:
   ```bash
   go mod download
   ```
4. Run the application:
   ```bash
   RABBITMQ_URL="amqp://user:pass@localhost:5672/" PORT=8080 go run main.go
   ```
5. Open your browser and navigate to `http://localhost:8080` to view the live temperature chart.

## Running with Docker

You can also run this application via Docker. A multi-stage `Dockerfile` is provided that builds a minimal image from scratch.

1. Build the Docker image:
   ```bash
   docker build -t esp32-temp-reader .
   ```
2. Run the Docker container, passing the required environment variables:
   ```bash
   docker run -d -p 8080:8080 \
     -e RABBITMQ_URL="amqp://user:pass@your-rabbitmq-host:5672/" \
     -e PORT="8080" \
     esp32-temp-reader
   ```

## Development

- `main.go`: Contains the core application logic including configuration loading, RabbitMQ connection and message consumption, the SSE broker, and the HTTP server serving the embedded HTML dashboard.
- `OpenAPISpec.yaml`: The schema definition for the messages sent by the ESP32.
- `.github/workflows/build.yml`: CI workflow for building and testing the application and Docker image.
