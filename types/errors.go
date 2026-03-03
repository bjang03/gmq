// Package types provides unified type definitions for the GMQ message queue system.
// It includes configuration structures, message types, constants, and error definitions
// used across all message queue implementations (NATS, Redis, RabbitMQ).
package types

import "errors"

// ============================================
// GMQ Plugin Unified Error Specification
// ============================================
// Error format: [plugin]: [operation]: [details]
// Example: "nats: connect: failed to connect to server"
// ============================================

// Configuration errors: errors related to configuration validation
var (
	// ErrConfigAddrRequired indicates that address configuration is required
	ErrConfigAddrRequired = errors.New("config: addr is required")
	// ErrConfigPortRequired indicates that port configuration is required
	ErrConfigPortRequired = errors.New("config: port is required")
	// ErrConfigUsernameRequired indicates that username configuration is required
	ErrConfigUsernameRequired = errors.New("config: username is required")
	// ErrConfigPasswordRequired indicates that password configuration is required
	ErrConfigPasswordRequired = errors.New("config: password is required")
)

// Connection errors: errors related to connection state
var (
	// ErrConnectionNil indicates that connection is nil
	ErrConnectionNil = errors.New("connection is nil")
	// ErrConnectionClosed indicates that connection is closed
	ErrConnectionClosed = errors.New("connection is closed")
)

// Message errors: errors related to message validation and processing
var (
	// ErrInvalidMessageType indicates that the message type is invalid
	ErrInvalidMessageType = errors.New("invalid message type")
	// ErrTopicRequired indicates that topic is required
	ErrTopicRequired = errors.New("topic is required")
	// ErrDataRequired indicates that data is required
	ErrDataRequired = errors.New("data is required")
	// ErrDelaySecondsRequired indicates that delay seconds must be greater than 0
	ErrDelaySecondsRequired = errors.New("delay seconds must be greater than 0")
	// ErrConsumerNameRequired indicates that consumer name is required
	ErrConsumerNameRequired = errors.New("consumer name is required")
	// ErrFetchCountRequired indicates that fetch count must be greater than 0
	ErrFetchCountRequired = errors.New("fetch count must be greater than 0")
	// ErrHandleFuncRequired indicates that handle function is required
	ErrHandleFuncRequired = errors.New("handle func is required")
)

// Feature not supported errors: errors for unsupported features in specific implementations
var (
	// ErrDelayMessageNotSupported indicates that delay message is not supported
	ErrDelayMessageNotSupported = errors.New("delay message not supported")
)

// Subscription errors: errors related to subscription management
var (
	// ErrSubscriptionNotFound indicates that subscription was not found
	ErrSubscriptionNotFound = errors.New("subscription not found")
	// ErrSubscriptionAlreadyExists indicates that subscription already exists
	ErrSubscriptionAlreadyExists = errors.New("subscription already exists")
)

// Type conversion errors: errors related to type conversion utilities
var (
	// ErrTargetMustBeStructPtr indicates that target must be a struct pointer (e.g. &config{})
	ErrTargetMustBeStructPtr = errors.New("target must be struct pointer (e.g. &config{})")
	// ErrUnsupportedFieldType indicates that the field type is not supported
	ErrUnsupportedFieldType = errors.New("unsupported field type")
)
