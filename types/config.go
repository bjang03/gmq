// Package types provides unified type definitions for the GMQ message queue system.
// It includes configuration structures, message types, constants, and error definitions
// used across all message queue implementations (NATS, Redis, RabbitMQ).
package types

// GMQConfig defines a flexible nested structure to adapt to various YAML configurations.
// The structure is: gmq -> {redis,nats,rabbitmq} -> {instance_name} -> {config_items}
// This allows multiple instances of the same message queue type with different configurations.
// Example YAML structure:
//   gmq:
//     nats:
//       primary:
//         addr: "localhost"
//         port: "4222"
//       secondary:
//         addr: "192.168.1.100"
//         port: "4222"
type GMQConfig struct {
	GMQ map[string]map[string]interface{} `yaml:"gmq"` // Nested map: MQ type -> instance name -> configuration
}
