// Package core provides the core functionality for the GMQ message queue system.
// It includes the unified Gmq interface, proxy wrapper, and plugin registry.
// The proxy layer adds monitoring, retry logic, and connection management.
package core

import (
	"context"
	"github.com/bjang03/gmq/types"
)

// Gmq unified interface definition for message queue operations.
// All message queue implementations must implement these methods to work with the GMQ proxy layer.
//
// Interface Methods:
//   - Connection: GmqConnect, GmqPing, GmqClose, GmqGetConn
//   - Publish: GmqPublish, GmqPublishDelay (for delayed messages)
//   - Subscribe: GmqSubscribe (with consumer group support)
//   - Acknowledgment: GmqAck (success), GmqNak (failure)
//   - State: GmqSetStateCallback (for connection state changes)
//
// Context Usage:
//   - All methods accept a context parameter for timeout/cancellation control
//   - Use context.WithTimeout to set operation timeouts
//   - Use context.WithCancel for manual cancellation
type Gmq interface {
	GmqGetConn(ctx context.Context) any                          // Get the underlying connection object for direct access
	GmqConnect(ctx context.Context, cfg map[string]any) error    // Establish connection to the message queue server
	GmqPublish(ctx context.Context, msg types.Publish) error     // Publish a message to the specified topic
	GmqSubscribe(ctx context.Context, msg types.Subscribe) error // Subscribe to messages from a topic with a consumer
	GmqPing(ctx context.Context) bool                            // Check if the connection is alive and working
	GmqClose(ctx context.Context) error                          // Close the connection and cleanup resources
	GmqAck(ctx context.Context, msg *types.AckMessage) error     // Acknowledge successful message processing
}

type GmqUnique interface {
	GmqPublishDelay(ctx context.Context, msg types.PublishDelay) error // Publish a delayed message with specified delay time
	GmqNak(ctx context.Context, msg *types.AckMessage) error           // Negatively acknowledge message (processing failed)
}
