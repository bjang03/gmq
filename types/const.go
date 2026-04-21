// Package types provides unified type definitions for the GMQ message queue system.
// It includes configuration structures, message types, constants, and error definitions
// used across all message queue implementations (NATS, Redis, RabbitMQ).
package types

import "time"

// Default reconnection configuration constants
const (
	BaseReconnectDelay = 5 * time.Second  // Base delay before first reconnection attempt
	MaxReconnectDelay  = 60 * time.Second // Maximum delay between reconnection attempts (caps exponential backoff)
	ConnectTimeout     = 30 * time.Second // Timeout for establishing initial connection
)

// Default retry configuration constants for message operations
const (
	MsgRetryDeliver = 30              // Maximum retry attempts for publish/subscribe operations
	MsgRetryDelay   = 3 * time.Second // Base delay between retry attempts (exponential backoff applied)
)
