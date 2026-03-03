# GMQ - Go Message Queue

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.24-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

GMQ is a unified message queue library for Go that provides a consistent interface for multiple message queue backends including NATS (JetStream), Redis Streams, and RabbitMQ.

## Features

- **Unified Interface**: Single API for multiple message queue backends
- **Auto-Reconnection**: Automatic reconnection with exponential backoff
- **Retry Logic**: Built-in retry mechanism for publish and subscribe operations
- **Delayed Messages**: Support for delayed message delivery (NATS & RabbitMQ)
- **Connection Management**: Centralized connection lifecycle management
- **Structured Logging**: Built-in structured logging with slog

## Supported Message Queues

| Message Queue | Publish | Subscribe | Delayed Message | Persistence |
|--------------|---------|-----------|-----------------|-------------|
| NATS (JetStream) | ✅ | ✅ | ✅ | File/Memory |
| Redis Streams | ✅ | ✅ | ❌ | Memory |
| RabbitMQ | ✅ | ✅ | ✅* | Disk |

*RabbitMQ delayed messages require the [x-delayed-message plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)

## Installation

```bash
go get github.com/bjang03/gmq
```

## Quick Start

### 1. Configuration

Create a `config.yml` file:

```yaml
gmq:
  nats:
    default:
      addr: "localhost"
      port: "4222"
      username: "nats"
      password: "nats"
  
  redis:
    default:
      addr: "localhost"
      port: "6379"
      db: 0
      password: ""
  
  rabbitmq:
    default:
      addr: "localhost"
      port: "5672"
      username: "guest"
      password: "guest"
      vhost: "/"
```

### 2. Initialize GMQ

```go
package main

import (
    "context"
    "log"
    
    gmq "github.com/bjang03/gmq/core/gmq"
)

func main() {
    // Initialize GMQ with configuration
    gmq.Init("config.yml")
    defer gmq.Shutdown(context.Background())
    
    // Your application code here
}
```

### 3. Publish Messages

#### NATS
```go
import (
    "github.com/bjang03/gmq/mq"
    "github.com/bjang03/gmq/types"
)

func publishMessage(ctx context.Context) {
    client := gmq.GetGmq("default")
    
    msg := &mq.NatsPubMessage{
        PubMessage: types.PubMessage{
            Topic: "orders.created",
            Data: map[string]interface{}{
                "order_id": "12345",
                "amount":   99.99,
            },
        },
        Durable: true, // Persist to disk
    }
    
    if err := client.GmqPublish(ctx, msg); err != nil {
        log.Printf("Failed to publish: %v", err)
    }
}
```

#### Redis
```go
func publishToRedis(ctx context.Context) {
    client := gmq.GetGmq("default")
    
    msg := &mq.RedisPubMessage{
        PubMessage: types.PubMessage{
            Topic: "notifications",
            Data: map[string]interface{}{
                "user_id": "user123",
                "message": "Hello, World!",
            },
        },
    }
    
    if err := client.GmqPublish(ctx, msg); err != nil {
        log.Printf("Failed to publish: %v", err)
    }
}
```

### 4. Subscribe to Messages

```go
func subscribeToMessages(ctx context.Context) {
    client := gmq.GetGmq("default")
    
    subMsg := &mq.NatsSubMessage{
        SubMessage: types.SubMessage{
            Topic:        "orders.created",
            ConsumerName: "order-processor",
            AutoAck:      false, // Manual acknowledgment
            FetchCount:   10,
            HandleFunc: func(ctx context.Context, message any) error {
                // Process message
                log.Printf("Received: %v", message)
                return nil // Return nil to acknowledge, error to reject
            },
        },
        Durable: true,
    }
    
    if err := client.GmqSubscribe(ctx, subMsg); err != nil {
        log.Printf("Failed to subscribe: %v", err)
    }
}
```

### 5. Delayed Messages

```go
func publishDelayedMessage(ctx context.Context) {
    client := gmq.GetGmq("default")
    
    msg := &mq.NatsPubDelayMessage{
        PubDelayMessage: types.PubDelayMessage{
            PubMessage: types.PubMessage{
                Topic: "reminders",
                Data: map[string]interface{}{
                    "task": "Send email",
                },
            },
            DelaySeconds: 300, // 5 minutes
        },
        Durable: true,
    }
    
    if err := client.GmqPublishDelay(ctx, msg); err != nil {
        log.Printf("Failed to publish delayed message: %v", err)
    }
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Application                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                    GMQ Proxy Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Retry Logic  │  │   Metrics    │  │   Logger     │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
┌───────▼──────┐ ┌────▼─────┐ ┌──────▼───────┐
│    NATS      │ │  Redis   │ │   RabbitMQ   │
│  (JetStream) │ │ (Streams)│ │              │
└──────────────┘ └──────────┘ └──────────────┘
```

## Configuration Options

### NATS
```yaml
nats:
  default:
    addr: "localhost"     # Server address
    port: "4222"          # Server port
    username: ""          # Authentication username
    password: ""          # Authentication password
```

### Redis
```yaml
redis:
  default:
    addr: "localhost"     # Server address
    port: "6379"          # Server port
    db: 0                 # Database number
    username: ""          # Authentication username
    password: ""          # Authentication password
    pool_size: 10         # Connection pool size
    min_idle_conns: 2     # Minimum idle connections
    max_retries: 3        # Maximum retry attempts
```

### RabbitMQ
```yaml
rabbitmq:
  default:
    addr: "localhost"     # Server address
    port: "5672"          # Server port
    username: "guest"     # Authentication username
    password: "guest"     # Authentication password
    vhost: "/"            # Virtual host
```

## Advanced Usage

### Multiple Instances

```yaml
gmq:
  nats:
    primary:
      addr: "nats1.example.com"
      port: "4222"
    secondary:
      addr: "nats2.example.com"
      port: "4222"
```

```go
primary := gmq.GetGmq("primary")
secondary := gmq.GetGmq("secondary")
```

### Manual Acknowledgment

```go
subMsg := &mq.NatsSubMessage{
    SubMessage: types.SubMessage{
        Topic:        "orders",
        ConsumerName: "processor",
        AutoAck:      false, // Manual acknowledgment
        HandleFunc: func(ctx context.Context, message any) error {
            // Process message
            if err := processOrder(message); err != nil {
                return err // Message will be rejected and requeued
            }
            return nil // Message will be acknowledged
        },
    },
}
```

### Connection Health Check

```go
client := gmq.GetGmq("default")
if client.GmqPing(ctx) {
    log.Println("Connection is healthy")
} else {
    log.Println("Connection is down")
}
```

## Error Handling

GMQ provides predefined errors for common scenarios:

```go
import "github.com/bjang03/gmq/types"

err := client.GmqPublish(ctx, msg)
if err != nil {
    if errors.Is(err, types.ErrTopicRequired) {
        log.Println("Topic is required")
    } else if errors.Is(err, types.ErrConnectionNil) {
        log.Println("Not connected to message queue")
    }
}
```

## Testing

Run tests for specific message queue:

```bash
# NATS tests
go test -v ./examples/... -run TestNats

# Redis tests
go test -v ./examples/... -run TestRedis

# RabbitMQ tests
go test -v ./examples/... -run TestRabbitMQ
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [NATS](https://nats.io/) - Cloud-native messaging system
- [Redis](https://redis.io/) - In-memory data structure store
- [RabbitMQ](https://www.rabbitmq.com/) - Feature-rich message broker
