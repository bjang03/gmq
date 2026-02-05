const API_BASE = window.location.origin;

// 指标配置定义
const METRIC_CONFIG = {
    // 基础信息
    basic: [
        { key: 'type', label: '类型', icon: '📋', format: 'string' },
        { key: 'serverAddr', label: '服务器地址', icon: '🌐', format: 'string' },
        { key: 'status', label: '状态', icon: '●', format: 'status' },
        { key: 'uptimeSeconds', label: '运行时间', icon: '⏱️', format: 'duration' },
        { key: 'connectedAt', label: '连接时间', icon: '🔗', format: 'string' }
    ],
    // 服务端消息统计 (从MQ服务器获取)
    serverMessages: [
        { key: 'messageCount', label: '服务端消息总数', icon: '📊', format: 'number' },
        { key: 'msgsIn', label: '流入消息', icon: '⬇️', format: 'number' },
        { key: 'msgsOut', label: '流出消息', icon: '⬆️', format: 'number' },
        { key: 'bytesIn', label: '流入字节', icon: '📥', format: 'bytes' },
        { key: 'bytesOut', label: '流出字节', icon: '📤', format: 'bytes' },
        { key: 'pendingMessages', label: '待处理消息', icon: '⏳', format: 'number' }
    ],
    // 客户端消息统计 (本地累加)
    clientMessages: [
        { key: 'publishCount', label: '客户端发布数', icon: '📤', format: 'number' },
        { key: 'subscribeCount', label: '客户端订阅数', icon: '📥', format: 'number' },
        { key: 'publishFailed', label: '发布失败', icon: '❌', format: 'number' },
        { key: 'subscribeFailed', label: '订阅失败', icon: '❌', format: 'number' },
        { key: 'pendingAckCount', label: '待确认消息', icon: '✓', format: 'number' }
    ],
    // 延迟指标 (客户端本地测量)
    latency: [
        { key: 'averageLatency', label: '平均延迟', icon: '⚡', format: 'ms' },
        { key: 'lastPingLatency', label: 'Ping延迟', icon: '📡', format: 'ms' },
        { key: 'maxLatency', label: '最大延迟', icon: '📈', format: 'ms' },
        { key: 'minLatency', label: '最小延迟', icon: '📉', format: 'ms' }
    ],
    // 吞吐量 (客户端计算)
    throughput: [
        { key: 'throughputPerSec', label: '总吞吐量', icon: '🚀', format: 'perSec' },
        { key: 'publishPerSec', label: '发布吞吐', icon: '📤', format: 'perSec' },
        { key: 'subscribePerSec', label: '订阅吞吐', icon: '📥', format: 'perSec' },
        { key: 'errorRate', label: '错误率', icon: '⚠️', format: 'percent' },
        { key: 'reconnectCount', label: '重连次数', icon: '🔄', format: 'number' }
    ]
};

// 类型标签映射
const TYPE_LABELS = {
    'nats': { name: 'NATS', color: '#4f46e5', icon: '🚀' },
    'redis': { name: 'Redis Stream', color: '#dc2626', icon: '🔴' },
    'rabbitmq': { name: 'RabbitMQ', color: '#ea580c', icon: '🐰' },
    'kafka': { name: 'Kafka', color: '#0891b2', icon: '📊' }
};

async function fetchAllMetrics() {
    try {
        const response = await fetch(`${API_BASE}/metrics/all`);
        const result = await response.json();

        if (result.code === 200) {
            return result.data;
        } else {
            throw new Error(result.msg || '获取监控数据失败');
        }
    } catch (error) {
        console.error('获取监控数据失败:', error);
        throw error;
    }
}

function formatValue(value, format) {
    if (value === undefined || value === null || value === '' || value === 0) return '-';

    switch (format) {
        case 'number':
            return new Intl.NumberFormat().format(value);
        case 'ms':
            return (typeof value === 'number' ? value.toFixed(2) : value) + ' ms';
        case 'perSec':
            return (typeof value === 'number' ? value.toFixed(2) : value) + ' /s';
        case 'percent':
            return (typeof value === 'number' ? value.toFixed(2) : value) + '%';
        case 'bytes':
            return formatBytes(value);
        case 'duration':
            return formatDuration(value);
        case 'status':
            return value;
        default:
            return String(value);
    }
}

function formatBytes(bytes) {
    if (bytes === 0 || !bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDuration(seconds) {
    if (!seconds || seconds === 0) return '-';
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);

    if (days > 0) return `${days}天${hours}小时`;
    if (hours > 0) return `${hours}小时${mins}分钟`;
    if (mins > 0) return `${mins}分钟${secs}秒`;
    return `${secs}秒`;
}

function getMetricValue(metric, config) {
    if (config.parent) {
        return metric[config.parent]?.[config.key];
    }
    return metric[config.key];
}

function renderMetrics(metrics) {
    const container = document.getElementById('metrics-container');
    container.innerHTML = '';

    let totalMessages = 0;
    let totalPublish = 0;
    let totalSubscribe = 0;
    let totalLatency = 0;
    let latencyCount = 0;
    let totalConnections = 0;
    let nodeCount = 0;

    for (const [name, metric] of Object.entries(metrics)) {
        // 汇总统计 - 优先使用服务端数据
        totalMessages += metric.messageCount || metric.msgsIn + metric.msgsOut || 0;
        totalPublish += metric.publishCount || 0;
        totalSubscribe += metric.subscribeCount || 0;
        totalConnections += metric.serverMetrics?.activeConnections || 0;
        nodeCount++;

        if (metric.averageLatency > 0) {
            totalLatency += metric.averageLatency;
            latencyCount++;
        }

        const card = createMetricCard(name, metric);
        container.appendChild(card);
    }

    // 更新概览
    document.getElementById('total-messages').textContent = formatNumber(totalMessages);
    document.getElementById('total-publish').textContent = formatNumber(totalPublish);
    document.getElementById('total-subscribe').textContent = formatNumber(totalSubscribe);
    document.getElementById('avg-latency').textContent =
        latencyCount > 0 ? (totalLatency / latencyCount).toFixed(2) + ' ms' : '0 ms';
    document.getElementById('active-connections').textContent = formatNumber(totalConnections);
    document.getElementById('server-nodes').textContent = formatNumber(nodeCount);

    document.getElementById('last-update').textContent = new Date().toLocaleString('zh-CN');
}

function createMetricCard(name, metric) {
    const card = document.createElement('div');
    card.className = 'metric-card';

    const typeInfo = TYPE_LABELS[metric.type] || { name: metric.type || 'Unknown', color: '#6b7280', icon: '?' };
    const statusClass = metric.status === 'connected' ? 'connected' : 'disconnected';

    card.innerHTML = `
        <div class="metric-header">
            <div class="metric-title">
                <span class="type-badge" style="background: ${typeInfo.color}20; color: ${typeInfo.color}; border: 1px solid ${typeInfo.color}40;">
                    ${typeInfo.icon} ${typeInfo.name}
                </span>
                <span class="metric-name">${name}</span>
            </div>
            <div class="metric-status ${statusClass}">${metric.status}</div>
        </div>
        <div class="metric-body">
            ${createMetricSection('基本信息', METRIC_CONFIG.basic, metric)}
            ${createMetricSection('📊 服务端消息统计', METRIC_CONFIG.serverMessages, metric)}
            ${createMetricSection('💻 客户端消息统计', METRIC_CONFIG.clientMessages, metric)}
            ${createMetricSection('⏱️ 延迟指标(客户端)', METRIC_CONFIG.latency, metric)}
            ${createMetricSection('📈 吞吐量(客户端)', METRIC_CONFIG.throughput, metric)}
            ${hasServerMetrics(metric) ? createServerMetricsSection(metric.serverMetrics) : ''}
            ${createExtensionsSection(metric.extensions)}
        </div>
    `;

    return card;
}

function createMetricSection(title, configs, metric) {
    const items = configs
        .map(config => {
            const value = getMetricValue(metric, config);
            if (value === undefined || value === null || value === '' || value === 0 || value === '-') return null;
            return { config, value };
        })
        .filter(item => item !== null);

    if (items.length === 0) return '';

    return `
        <div class="metric-section">
            <div class="section-title">${title}</div>
            <div class="metric-grid">
                ${items.map(({ config, value }) => `
                    <div class="metric-item">
                        <div class="metric-item-icon">${config.icon}</div>
                        <div class="metric-item-content">
                            <div class="metric-item-label">${config.label}</div>
                            <div class="metric-item-value ${config.format === 'status' ? (value === 'connected' ? 'text-success' : 'text-error') : ''}">
                                ${formatValue(value, config.format)}
                            </div>
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

function hasServerMetrics(metric) {
    return metric.serverMetrics && Object.keys(metric.serverMetrics).some(k => metric.serverMetrics[k] !== 0 && metric.serverMetrics[k] !== '');
}

function createServerMetricsSection(serverMetrics) {
    const fields = [
        { key: 'serverVersion', label: '版本', icon: '🏷️' },
        { key: 'serverId', label: '服务器ID', icon: '🆔', format: 'shortId' },
        { key: 'totalConnections', label: '总连接数', icon: '👥', format: 'number' },
        { key: 'activeConnections', label: '活跃连接', icon: '✅', format: 'number' },
        { key: 'slowConsumers', label: '慢消费者', icon: '🐌', format: 'number' },
        { key: 'totalConsumers', label: '消费者数', icon: '👤', format: 'number' },
        { key: 'totalChannels', label: '通道数', icon: '📡', format: 'number' },
        { key: 'totalSubjects', label: '主题数', icon: '📋', format: 'number' },
        { key: 'memoryUsed', label: '内存使用', icon: '💾', format: 'bytes' },
        { key: 'memoryLimit', label: '内存限制', icon: '📊', format: 'bytes' },
        { key: 'cpuUsage', label: 'CPU使用', icon: '💻', format: 'percent' }
    ];

    const items = fields
        .map(field => {
            const value = serverMetrics[field.key];
            if (!value || value === 0 || value === '') return null;
            let displayValue = value;
            if (field.format === 'number') displayValue = formatNumber(value);
            else if (field.format === 'bytes') displayValue = formatBytes(value);
            else if (field.format === 'percent') displayValue = value + '%';
            else if (field.format === 'shortId') displayValue = String(value).substring(0, 8) + '...';
            else displayValue = String(value);
            return { ...field, displayValue };
        })
        .filter(item => item !== null);

    if (items.length === 0) return '';

    return `
        <div class="metric-section">
            <div class="section-title">🖥️ 服务端详细信息</div>
            <div class="metric-grid">
                ${items.map(item => `
                    <div class="metric-item">
                        <div class="metric-item-icon">${item.icon}</div>
                        <div class="metric-item-content">
                            <div class="metric-item-label">${item.label}</div>
                            <div class="metric-item-value">${item.displayValue}</div>
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

function createExtensionsSection(extensions) {
    if (!extensions || Object.keys(extensions).length === 0) return '';

    const items = Object.entries(extensions)
        .map(([key, value]) => {
            if (value === undefined || value === null || value === '' || (typeof value === 'number' && value === 0)) return null;
            return { key, value };
        })
        .filter(item => item !== null);

    if (items.length === 0) return '';

    return `
        <div class="metric-section">
            <div class="section-title">🔧 扩展指标</div>
            <div class="metric-grid">
                ${items.map(({ key, value }) => `
                    <div class="metric-item">
                        <div class="metric-item-icon">🔧</div>
                        <div class="metric-item-content">
                            <div class="metric-item-label">${key}</div>
                            <div class="metric-item-value">${typeof value === 'object' ? JSON.stringify(value) : String(value)}</div>
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

function formatNumber(num) {
    return new Intl.NumberFormat().format(num || 0);
}

async function refreshMetrics() {
    const container = document.getElementById('metrics-container');
    container.innerHTML = '<div class="loading">加载中...</div>';

    try {
        const metrics = await fetchAllMetrics();
        renderMetrics(metrics);
    } catch (error) {
        container.innerHTML = `
            <div class="error">
                <div>❌ 加载失败</div>
                <div style="margin-top: 10px;">${error.message}</div>
            </div>
        `;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    refreshMetrics();
    setInterval(refreshMetrics, 5000);
});
