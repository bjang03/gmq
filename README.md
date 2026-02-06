# GMQ - 轻量级消息队列管理系统

<div align="center">

![Go Version](https://img.shields.io/badge/Go-1.25.5+-00ADD8?style=flat&logo=go)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Status](https://img.shields.io/badge/status-active-brightgreen.svg)

一个基于 Go 语言开发的轻量级消息队列管理系统，采用插件化架构设计，支持多种消息队列，提供 Web 可视化监控界面。

</div>

---

## ✨ 特性

- 🚀 **插件化架构** - 统一接口抽象，易于扩展新的消息队列
- 📊 **实时监控** - WebSocket 实时推送消息队列指标
- 🔄 **自动重连** - 断线自动重连，订阅关系自动恢复
- 🎯 **重试机制** - 指数退避重试策略，提高消息可靠性
- 🛡️ **高可用** - 支持多消息队列备份，自动故障转移
- 🌐 **Web UI** - 可视化监控界面，操作简便
- ⚡ **高性能** - 无锁并发设计，低延迟高吞吐
- 🔌 **热插拔** - 支持动态注册新的消息队列组件

## 📦 支持的消息队列

| 消息队列 | 状态 | 说明 |
|---------|------|------|
| NATS | ✅ 已支持 | 高性能消息队列 |
| Redis Stream | 🚧 计划中 | Redis 流式数据结构 |
| RabbitMQ | 🚧 计划中 | 企业级消息代理 |

## 🏗️ 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                        GMQ System                           │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Web UI     │  │   HTTP API   │  │   WebSocket  │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                 │                 │               │
│  ┌──────▼─────────────────▼─────────────────▼───────┐      │
│  │              Web Layer (Gin)                      │      │
│  └──────┬────────────────────────────────────────────┘      │
│         │                                                   │
│  ┌──────▼─────────────────────────────────────────────┐    │
│  │              Core Layer                             │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐    │    │
│  │  │ Registry │  │ Pipeline │  │   Metrics    │    │    │
│  │  └──────────┘  └──────────┘  └──────────────┘    │    │
│  └──────┬────────────────────────────────────────────┘    │
│         │                                                   │
│  ┌──────▼─────────────────────────────────────────────┐    │
│  │              Components Layer                        │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐    │    │
│  │  │  NATS    │  │  Redis   │  │  RabbitMQ    │    │    │
│  │  └──────────┘  └──────────┘  └──────────────┘    │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 快速开始

### 环境要求

- Go 1.25.5+
- NATS Server 2.12.4+ （可选，如果使用 NATS）

### 安装

```bash
# 克隆项目
git clone https://github.com/bjang03/gmq.git
cd gmq

# 安装依赖
go mod download
```

### 配置

编辑 `config.yml` 文件：

```yaml
# 服务器配置
server:
  port: 8080

# NATS 配置
nats:
  url: "nats://localhost:4222"
  timeout: 5
  reconnect: true
  max_reconnect: 5
  ping_interval: 10
```

### 运行

```bash
# 启动服务
go run main.go
```

服务启动后，访问 http://localhost:8080 查看监控界面。

## 📖 使用指南

### 发布消息

#### 普通 API 调用

```go
import (
    "context"
    "github.com/bjang03/gmq/core"
    "github.com/bjang03/gmq/web/dto"
)

// 发布普通消息
func publishMessage() error {
    msg := dto.NormalMessage{
        Subject: "test.subject",
        Data:    map[string]interface{}{
            "message": "Hello GMQ",
            "time":    time.Now(),
        },
    }

    return core.GmqPublish(context.Background(), "nats", msg)
}
```

#### HTTP API

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "plugin": "nats",
    "type": "normal",
    "subject": "test.subject",
    "data": {"message": "Hello GMQ"}
  }'
```

### 订阅消息

```go
import (
    "context"
    "github.com/bjang03/gmq/core"
    "github.com/bjang03/gmq/web/dto"
)

// 订阅消息
func subscribeMessage() (interface{}, error) {
    sub := dto.SubscribeNormal{
        Subject: "test.subject",
        Queue:   "worker.group",
    }

    return core.GmqSubscribe(context.Background(), "nats", sub)
}
```

#### HTTP API

```bash
curl -X GET "http://localhost:8080/subscribe?plugin=nats&subject=test.subject"
```

### 使用 WebSocket 监控

```javascript
// 连接 WebSocket
const ws = new WebSocket('ws://localhost:8080/ws/metrics');

// 接收实时指标
ws.onmessage = (event) => {
    const metrics = JSON.parse(event.data);
    console.log('Published:', metrics.publishedCount);
    console.log('Subscribed:', metrics.subscribedCount);
    console.log('Errors:', metrics.errorCount);
};
```

## 🔌 开发自定义插件

GMQ 提供了简单的插件扩展机制，只需实现 `Gmq` 接口即可。

### 插件接口定义

```go
type Gmq interface {
    GmqConnect(ctx context.Context) error
    GmqPublish(ctx context.Context, msg Publish) error
    GmqSubscribe(ctx context.Context, msg any) (interface{}, error)
    GmqPing(ctx context.Context) bool
    GmqClose(ctx context.Context) error
    GetMetrics(ctx context.Context) *Metrics
}
```

### 示例：实现 Redis Stream 插件

```go
package components

import (
    "context"
    "github.com/bjang03/gmq/core"
    "github.com/go-redis/redis/v8"
)

type redisMsg struct {
    client *redis.Client
}

// 连接 Redis
func (r *redisMsg) GmqConnect(ctx context.Context) error {
    r.client = redis.NewClient(&redis.Options{
        Addr:     config.GlobalConfig.Redis.URL,
        Password: config.GlobalConfig.Redis.Password,
        DB:       config.GlobalConfig.Redis.DB,
    })

    return r.client.Ping(ctx).Err()
}

// 发布消息
func (r *redisMsg) GmqPublish(ctx context.Context, msg core.Publish) error {
    data, _ := json.Marshal(msg.GetData())
    return r.client.XAdd(ctx, &redis.XAddArgs{
        Stream: msg.GetSubject(),
        Values: map[string]interface{}{"data": string(data)},
    }).Err()
}

// 订阅消息
func (r *redisMsg) GmqSubscribe(ctx context.Context, msg any) (interface{}, error) {
    // 实现订阅逻辑
    return nil, nil
}

// Ping 检测
func (r *redisMsg) GmqPing(ctx context.Context) bool {
    return r.client.Ping(ctx).Err() == nil
}

// 关闭连接
func (r *redisMsg) GmqClose(ctx context.Context) error {
    return r.client.Close()
}

// 获取指标
func (r *redisMsg) GetMetrics(ctx context.Context) *core.Metrics {
    return &core.Metrics{}
}

// 注册插件
func init() {
    _ = core.GmqRegister("redis", &redisMsg{})
}
```

## 📊 监控指标

GMQ 提供丰富的监控指标：

| 指标名称 | 说明 |
|---------|------|
| `publishedCount` | 发布消息总数 |
| `subscribedCount` | 订阅消息总数 |
| `errorCount` | 错误次数 |
| `lastErrorTime` | 最后一次错误时间 |
| `connected` | 连接状态 |
| `connectTime` | 连接时长 |
| `subjectCount` | 当前订阅的 Subject 数量 |

## 🔧 高级功能

### 高可用策略

配置多个消息队列作为备份，自动故障转移：

```yaml
# 高可用配置
ha:
  enabled: true
  plugins:
    - name: "nats"
      weight: 80
    - name: "redis"
      weight: 20
```

### 延迟消息

```go
msg := dto.DelayedMessage{
    Subject: "delayed.subject",
    Data:    "Hello Delayed",
    Delay:   30 * time.Second, // 延迟 30 秒
}
```

### 事务消息

```go
msg := dto.TransactionalMessage{
    Subject: "transaction.subject",
    Data:    "Hello Transaction",
    TxID:    "tx-123456",
}
```

## 🛠️ 配置说明

完整配置文件示例：

```yaml
# 服务器配置
server:
  port: 8080
  read_timeout: 30
  write_timeout: 30

# NATS 配置
nats:
  url: "nats://localhost:4222"
  timeout: 5
  reconnect: true
  max_reconnect: 5
  reconnect_delay: 2
  ping_interval: 10

# Redis 配置
redis:
  url: "localhost:6379"
  password: ""
  db: 0
  pool_size: 10
  timeout: 5

# 高可用配置
ha:
  enabled: false
  max_retries: 3
  retry_delay: 1

# 监控配置
monitoring:
  metrics_interval: 2
  websocket_ping_interval: 30
```

## 🧪 测试

```bash
# 运行所有测试
go test ./...

# 运行 NATS 组件测试
go test ./components -v

# 查看测试覆盖率
go test -cover ./...
```

## 📁 项目结构

```
gmq/
├── main.go                 # 程序入口
├── config/                 # 配置管理
│   └── config.go
├── core/                   # 核心功能
│   ├── types.go           # 类型定义
│   ├── pipeline.go        # 管道封装
│   ├── registry.go        # 插件注册
│   └── metrics.go         # 监控指标
├── components/             # 消息队列组件
│   ├── nats.go
│   └── nats_test.go
├── web/                    # Web 服务
│   ├── http.go
│   ├── controller/
│   ├── middleware/
│   ├── dto/
│   └── ui/
├── config.yml              # 配置文件
├── go.mod
├── go.sum
└── README.md
```

## 🤝 贡献指南

欢迎贡献代码！请遵循以下步骤：

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request

## 📄 开源协议

本项目采用 MIT 协议 - 详见 [LICENSE](LICENSE) 文件

## 🔗 相关链接

- [NATS 官方文档](https://docs.nats.io/)
- [Gin Web 框架](https://gin-gonic.com/)
- [Go 语言官方文档](https://golang.org/doc/)

## 💬 联系方式

- Issue Tracker: [GitHub Issues](https://github.com/bjang03/gmq/issues)
- 邮箱: bjang03@example.com

## 🙏 致谢

感谢所有为本项目做出贡献的开发者！

---

<div align="center">

**如果这个项目对你有帮助，请给个 Star ⭐**

Made with ❤️ by GMQ Team

</div>
