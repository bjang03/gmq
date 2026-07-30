// Package core provides the core functionality for the GMQ message queue system.
// It includes the unified Gmq interface, proxy wrapper, and plugin registry.
package core

// Subscription represents a message queue subscription handle.
// It allows callers to unsubscribe or query subscription information.
type Subscription struct {
	topic        string
	consumerName string
	subKey       string
	proxy        *GmqProxy
}

// Topic returns the subscribed topic name.
func (s *Subscription) Topic() string {
	return s.topic
}

// ConsumerName returns the consumer name used for this subscription.
func (s *Subscription) ConsumerName() string {
	return s.consumerName
}

// Drain cancels the subscription and drains all associated resources.
// It performs two levels of cleanup:
//  1. Proxy level: cancels the subscription goroutine and removes internal tracking
//  2. MQ level:  cancels/drains the server-side subscription (e.g. NATS consumer, RabbitMQ consumer, Redis consumer group)
//
// After calling Drain, the message handler will no longer receive messages.
// It is safe to call Drain multiple times.
func (s *Subscription) Drain() {
	if s.proxy != nil {
		s.proxy.cleanupSubscribe(s.subKey)
	}
}
