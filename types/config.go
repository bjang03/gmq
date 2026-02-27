package types

import "fmt"

// Config 配置文件结构
type Config struct {
	Gmq struct {
		Redis    []RedisConfig    `yaml:"redis"`
		Nats     []NatsConfig     `yaml:"nats"`
		RabbitMQ []RabbitMQConfig `yaml:"rabbitmq"`
	} `yaml:"gmq"`
}

// RedisConfig Redis 配置项
type RedisConfig struct {
	Name            string `yaml:"name"`
	Url             string `yaml:"url"`
	Port            string `yaml:"port"`
	Db              int    `yaml:"db"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	PoolSize        int    `yaml:"poolSize"`
	MinIdleConns    int    `yaml:"minIdleConns"`
	MaxActiveConns  int    `yaml:"maxActiveConns"`
	MaxRetries      int    `yaml:"maxRetries"`
	DialTimeout     int    `yaml:"dialTimeout"`
	ReadTimeout     int    `yaml:"readTimeout"`
	WriteTimeout    int    `yaml:"writeTimeout"`
	PoolTimeout     int    `yaml:"poolTimeout"`
	ConnMaxIdleTime int    `yaml:"connMaxIdleTime"`
	ConnMaxLifetime int    `yaml:"connMaxLifetime"`
}

// Validate 验证 Redis 配置是否有效（包含 Name 验证，用于配置加载阶段）
func (c RedisConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("redis config name is empty")
	}
	return c.ValidateConn()
}

// ValidateConn 验证 Redis 连接配置是否有效（不包含 Name，用于连接阶段）
func (c RedisConfig) ValidateConn() error {
	if c.Url == "" {
		return fmt.Errorf("redis config url is empty")
	}
	if c.Port == "" {
		return fmt.Errorf("redis config port is empty")
	}
	return nil
}

// NatsConfig NATS 配置项
type NatsConfig struct {
	Name     string `yaml:"name"`
	Url      string `yaml:"url"`
	Port     string `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// Validate 验证 NATS 配置是否有效（包含 Name 验证，用于配置加载阶段）
func (c NatsConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("nats config name is empty")
	}
	return c.ValidateConn()
}

// ValidateConn 验证 NATS 连接配置是否有效（不包含 Name，用于连接阶段）
func (c NatsConfig) ValidateConn() error {
	if c.Url == "" {
		return fmt.Errorf("nats config url is empty")
	}
	if c.Port == "" {
		return fmt.Errorf("nats config port is empty")
	}
	return nil
}

// RabbitMQConfig RabbitMQ 配置项
type RabbitMQConfig struct {
	Name     string `yaml:"name"`
	Url      string `yaml:"url"`
	Port     string `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	VHost    string `yaml:"vhost"`
}

// Validate 验证 RabbitMQ 配置是否有效（包含 Name 验证，用于配置加载阶段）
func (c RabbitMQConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("rabbitmq config name is empty")
	}
	return c.ValidateConn()
}

// ValidateConn 验证 RabbitMQ 连接配置是否有效（不包含 Name，用于连接阶段）
func (c RabbitMQConfig) ValidateConn() error {
	if c.Url == "" {
		return fmt.Errorf("rabbitmq config url is empty")
	}
	if c.Port == "" {
		return fmt.Errorf("rabbitmq config port is empty")
	}
	if c.Username == "" {
		return fmt.Errorf("rabbitmq config username is empty")
	}
	if c.Password == "" {
		return fmt.Errorf("rabbitmq config password is empty")
	}
	return nil
}
