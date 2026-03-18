# Build stage
FROM golang:1.24.3-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the Go modules and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o esp32-temp-reader .

# Final minimal stage
FROM scratch

# Copy the binary from the builder stage
COPY --from=builder /app/esp32-temp-reader /esp32-temp-reader

# Expose the default port (can be overridden by environment variable)
EXPOSE 8080

# Command to run the executable
ENTRYPOINT ["/esp32-temp-reader"]
