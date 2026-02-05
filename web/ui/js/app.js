// WebSocket连接
let ws = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_DELAY = 3000;

// 列表展示的完整指标配置（所有指标分类型）
const LIST_METRICS_CONFIG = {
    basic: [
        { key: 'serverAddr', label: '服务器地址' },
        { key: 'uptimeSeconds', label: '运行时间', format: 'duration' },
        { key: 'connectedAt', label: '连接时间' }
    ],
    serverMessages: [
        { key: 'messageCount', label: '服务端消息总数' },
        { key: 'msgsIn', label: '流入消息', format: 'number' },
        { key: 'msgsOut', label: '流出消息', format: 'number' },
        { key: 'bytesIn', label: '流入字节', format: 'bytes' },
        { key: 'bytesOut', label: '流出字节', format: 'bytes' },
        { key: 'pendingMessages', label: '待处理消息', format: 'number' }
    ],
    clientMessages: [
        { key: 'publishCount', label: '客户端发布数', format: 'number' },
        { key: 'subscribeCount', label: '客户端订阅数', format: 'number' },
        { key: 'publishFailed', label: '发布失败', format: 'number' },
        { key: 'subscribeFailed', label: '订阅失败', format: 'number' },
        { key: 'pendingAckCount', label: '待确认消息', format: 'number' }
    ],
    serverMetrics: [
        { key: 'serverMetrics.activeConnections', label: '活跃连接', format: 'number', isNested: true },
        { key: 'serverMetrics.totalConnections', label: '总连接数', format: 'number', isNested: true },
        { key: 'serverMetrics.serverVersion', label: '服务器版本', isNested: true },
        { key: 'serverMetrics.serverId', label: '服务器ID', isNested: true, format: 'shortId' }
    ],
    latency: [
        { key: 'averageLatency', label: '平均延迟', format: 'ms' },
        { key: 'lastPingLatency', label: 'Ping延迟', format: 'ms' },
        { key: 'maxLatency', label: '最大延迟', format: 'ms' },
        { key: 'minLatency', label: '最小延迟', format: 'ms' }
    ],
    throughput: [
        { key: 'throughputPerSec', label: '总吞吐量', format: 'perSec' },
        { key: 'publishPerSec', label: '发布吞吐', format: 'perSec' },
        { key: 'subscribePerSec', label: '订阅吞吐', format: 'perSec' },
        { key: 'errorRate', label: '错误率', format: 'percent' },
        { key: 'reconnectCount', label: '重连次数', format: 'number' }
    ]
};

// 详情面板的完整指标配置（保留用于详情展开）
const DETAIL_METRIC_CONFIG = {
    basic: [
        { key: 'type', label: '类型', icon: '📋' },
        { key: 'serverAddr', label: '服务器地址', icon: '🌐' },
        { key: 'status', label: '状态', icon: '●', isStatus: true },
        { key: 'uptimeSeconds', label: '运行时间', icon: '⏱️', format: 'duration' },
        { key: 'connectedAt', label: '连接时间', icon: '🔗' }
    ],
    serverMessages: [
        { key: 'messageCount', label: '服务端消息总数', icon: '📊' },
        { key: 'msgsIn', label: '流入消息', icon: '⬇️' },
        { key: 'msgsOut', label: '流出消息', icon: '⬆️' },
        { key: 'bytesIn', label: '流入字节', icon: '📥', format: 'bytes' },
        { key: 'bytesOut', label: '流出字节', icon: '📤', format: 'bytes' },
        { key: 'pendingMessages', label: '待处理消息', icon: '⏳' }
    ],
    clientMessages: [
        { key: 'publishCount', label: '客户端发布数', icon: '📤' },
        { key: 'subscribeCount', label: '客户端订阅数', icon: '📥' },
        { key: 'publishFailed', label: '发布失败', icon: '❌' },
        { key: 'subscribeFailed', label: '订阅失败', icon: '❌' },
        { key: 'pendingAckCount', label: '待确认消息', icon: '✓' }
    ],
    latency: [
        { key: 'averageLatency', label: '平均延迟', icon: '⚡', format: 'ms' },
        { key: 'lastPingLatency', label: 'Ping延迟', icon: '📡', format: 'ms' },
        { key: 'maxLatency', label: '最大延迟', icon: '📈', format: 'ms' },
        { key: 'minLatency', label: '最小延迟', icon: '📉', format: 'ms' }
    ],
    throughput: [
        { key: 'throughputPerSec', label: '总吞吐量', icon: '🚀', format: 'perSec' },
        { key: 'publishPerSec', label: '发布吞吐', icon: '📤', format: 'perSec' },
        { key: 'subscribePerSec', label: '订阅吞吐', icon: '📥', format: 'perSec' },
        { key: 'errorRate', label: '错误率', icon: '⚠️', format: 'percent' },
        { key: 'reconnectCount', label: '重连次数', icon: '🔄' }
    ]
};

const TYPE_LABELS = {
    'nats': { name: 'NATS', color: '#4f46e5', icon: '🚀' },
    'redis': { name: 'Redis Stream', color: '#dc2626', icon: '🔴' },
    'rabbitmq': { name: 'RabbitMQ', color: '#ea580c', icon: '🐰' },
    'kafka': { name: 'Kafka', color: '#0891b2', icon: '📊' }
};

// 缓存 DOM 元素引用
const domCache = {
    overview: {},
    cards: {}
};

// 初始化WebSocket连接
function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/metrics`;
    
    ws = new WebSocket(wsUrl);
    
    ws.onopen = function(event) {
        console.log('WebSocket连接已建立');
        reconnectAttempts = 0;
        updateConnectionStatus('已连接', true);
    };
    
    ws.onmessage = function(event) {
        try {
            const data = JSON.parse(event.data);
            if (data.type === 'metrics') {
                renderMetrics(data.payload);
            }
        } catch (error) {
            console.error('解析WebSocket消息失败:', error);
        }
    };
    
    ws.onclose = function(event) {
        console.log('WebSocket连接已关闭');
        updateConnectionStatus('连接断开', false);
        
        // 尝试重连
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
            reconnectAttempts++;
            console.log(`尝试重连... (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
            setTimeout(initWebSocket, RECONNECT_DELAY);
        }
    };
    
    ws.onerror = function(error) {
        console.error('WebSocket错误:', error);
        updateConnectionStatus('连接错误', false);
    };
}

function formatValue(value, format) {
    if (value === undefined || value === null || value === '') return '-';

    switch (format) {
        case 'number': return new Intl.NumberFormat().format(value);
        case 'ms': return Number(value).toFixed(2) + ' ms';
        case 'perSec': return Number(value).toFixed(2) + ' /s';
        case 'percent': return Number(value).toFixed(2) + '%';
        case 'shortId': {
            const strValue = String(value);
            return strValue.length > 8 ? strValue.substring(0, 8) + '...' : strValue;
        }
        case 'bytes': {
            const numValue = Number(value);
            if (numValue === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
            const i = Math.min(Math.floor(Math.log(numValue) / Math.log(k)), sizes.length - 1);
            return parseFloat((numValue / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
        case 'duration': {
            const numValue = Number(value);
            if (!numValue) return '-';
            const days = Math.floor(numValue / 86400);
            const hours = Math.floor((numValue % 86400) / 3600);
            const mins = Math.floor((numValue % 3600) / 60);
            const secs = Math.floor(numValue % 60);
            if (days > 0) return `${days}天${hours}小时`;
            if (hours > 0) return `${hours}小时${mins}分钟`;
            if (mins > 0) return `${mins}分钟${secs}秒`;
            return `${secs}秒`;
        }
        default: return String(value);
    }
}

function formatNumber(num) {
    return new Intl.NumberFormat().format(num || 0);
}

// 更新连接状态显示
function updateConnectionStatus(status, isConnected) {
    const statusEl = document.getElementById('connection-status');
    if (statusEl) {
        statusEl.textContent = status;
        statusEl.className = `connection-status ${isConnected ? 'connected' : 'disconnected'}`;
    }
}

function getSectionTitle(sectionKey) {
    const titles = {
        basic: '基本信息',
        serverMessages: '服务端消息统计',
        clientMessages: '客户端消息统计',
        serverMetrics: '服务端详细信息',
        latency: '延迟指标',
        throughput: '吞吐量'
    };
    return titles[sectionKey] || sectionKey;
}

// 初始化概览统计的 DOM 引用
function initOverviewCache() {
    domCache.overview = {
        totalMessages: document.getElementById('total-messages'),
        totalPublish: document.getElementById('total-publish'),
        totalSubscribe: document.getElementById('total-subscribe'),
        avgLatency: document.getElementById('avg-latency'),
        activeConnections: document.getElementById('active-connections'),
        serverNodes: document.getElementById('server-nodes')
    };
}

// 更新概览统计 - 只更新 textContent
function updateOverview(metrics) {
    let totalMessages = 0, totalPublish = 0, totalSubscribe = 0;
    let totalLatency = 0, latencyCount = 0;
    let totalConnections = 0, nodeCount = 0;

    for (const metric of Object.values(metrics)) {
        totalMessages += metric.messageCount || (metric.msgsIn + metric.msgsOut) || 0;
        totalPublish += metric.publishCount || 0;
        totalSubscribe += metric.subscribeCount || 0;
        totalConnections += metric.serverMetrics?.activeConnections || 0;
        nodeCount++;

        if (metric.averageLatency > 0) {
            totalLatency += metric.averageLatency;
            latencyCount++;
        }
    }

    domCache.overview.totalMessages.textContent = formatNumber(totalMessages);
    domCache.overview.totalPublish.textContent = formatNumber(totalPublish);
    domCache.overview.totalSubscribe.textContent = formatNumber(totalSubscribe);
    domCache.overview.avgLatency.textContent = latencyCount > 0 ? (totalLatency / latencyCount).toFixed(2) + ' ms' : '0 ms';
    domCache.overview.activeConnections.textContent = formatNumber(totalConnections);
    domCache.overview.serverNodes.textContent = formatNumber(nodeCount);

// 创建列表项结构（多行展示，类型跨行合并）
    function createListItem(name, metric) {
        const typeInfo = TYPE_LABELS[metric.type] || {name: metric.type || 'Unknown', color: '#6b7280', icon: '?'};

        const item = document.createElement('div');
        item.className = 'metric-list-item';
        item.dataset.itemName = name;

        // 左侧信息区（跨所有行）
        const infoDiv = document.createElement('div');
        infoDiv.className = 'metric-info';
        infoDiv.innerHTML = `
        <span class="type-badge" style="background: ${typeInfo.color}20; color: ${typeInfo.color}; border: 1px solid ${typeInfo.color}40;">
            ${typeInfo.icon} ${typeInfo.name}
        </span>
        <span class="metric-name">${name}</span>
        <span class="metric-status ${metric.status === 'connected' ? 'connected' : 'disconnected'}" data-status>
            ${metric.status === 'connected' ? '已连接' : '未连接'}
        </span>
    `;
        item.appendChild(infoDiv);

        // 右侧指标区（多行展示）
        const contentDiv = document.createElement('div');
        contentDiv.className = 'metric-content';

        // 按类型分组创建多行
        for (const [sectionKey, configs] of Object.entries(LIST_METRICS_CONFIG)) {
            const sectionDiv = document.createElement('div');
            sectionDiv.className = 'metric-section-row';
            sectionDiv.dataset.section = sectionKey;

            const titleDiv = document.createElement('div');
            titleDiv.className = 'section-row-title';
            titleDiv.textContent = getSectionTitle(sectionKey);
            sectionDiv.appendChild(titleDiv);

            const metricsGrid = document.createElement('div');
            metricsGrid.className = 'section-row-metrics';

            for (const config of configs) {
                const metricItem = document.createElement('div');
                metricItem.className = 'metric-content-item';
                metricItem.innerHTML = `
                <span class="metric-content-label">${config.label}</span>
                <span class="metric-content-value" data-value-key="${config.key}">-</span>
            `;
                metricsGrid.appendChild(metricItem);
            }

            sectionDiv.appendChild(metricsGrid);
            contentDiv.appendChild(sectionDiv);
        }

        item.appendChild(contentDiv);
        return item;
    }

// 更新列表项的值
    function updateListItemValues(name, metric) {
        const item = document.querySelector(`.metric-list-item[data-item-name="${name}"]`);
        if (!item) return;

        // 更新状态
        const statusEl = item.querySelector('[data-status]');
        if (statusEl) {
            const statusText = metric.status === 'connected' ? '已连接' : '未连接';
            if (statusEl.textContent !== statusText) {
                statusEl.textContent = statusText;
                statusEl.className = `metric-status ${metric.status === 'connected' ? 'connected' : 'disconnected'}`;
            }
        }

        // 更新各分区的值
        for (const [sectionKey, configs] of Object.entries(LIST_METRICS_CONFIG)) {
            const sectionEl = item.querySelector(`[data-section="${sectionKey}"]`);
            if (!sectionEl) continue;

            let hasVisibleData = false;

            for (const config of configs) {
                const valueEl = sectionEl.querySelector(`[data-value-key="${config.key}"]`);
                if (!valueEl) continue;

                // 支持嵌套属性（如 serverMetrics.activeConnections）
                let value;
                if (config.isNested) {
                    const keys = config.key.split('.');
                    value = metric;
                    for (const k of keys) {
                        value = value?.[k];
                        if (value === undefined) break;
                    }
                } else {
                    value = metric[config.key];
                }

                const isNumericField = config.format !== undefined;

                // 显示所有有值的字段，数值0也显示
                if (value === undefined || value === null || value === '') {
                    valueEl.textContent = '-';
                    valueEl.parentElement.style.display = 'none';
                } else {
                    let formatted = config.format ? formatValue(value, config.format) : String(value);

                    // 特殊处理 shortId 格式
                    if (config.format === 'shortId' && typeof value === 'string') {
                        formatted = value.substring(0, 8) + '...';
                    }

                    if (valueEl.textContent !== formatted) {
                        valueEl.textContent = formatted;
                    }
                    valueEl.parentElement.style.display = 'flex';
                    hasVisibleData = true;
                }
            }

            sectionEl.style.display = hasVisibleData ? 'flex' : 'none';
        }
    }

// 主渲染函数
    function renderMetrics(metrics) {
        const container = document.getElementById('metrics-container');

        // 更新或创建列表项
        for (const [name, metric] of Object.entries(metrics)) {
            let item = document.querySelector(`.metric-list-item[data-item-name="${name}"]`);

            if (!item) {
                // 首次渲染创建结构
                item = createListItem(name, metric);
                container.appendChild(item);
            }

            // 更新值（不重新创建元素）
            updateListItemValues(name, metric);
        }

        // 删除已经不存在的列表项
        const existingItems = container.querySelectorAll('.metric-list-item');
        for (const item of existingItems) {
            const itemName = item.dataset.itemName;
            if (!metrics[itemName]) {
                item.remove();
            }
        }

        // 更新概览
        updateOverview(metrics);
    }


// 显示错误
    function showError(error) {
        const container = document.getElementById('metrics-container');
        container.innerHTML = `
        <div class="error">
            <div>❌ 加载失败</div>
            <div style="margin-top: 10px;">${error.message}</div>
        </div>
    `;
    }

// 初始化
    document.addEventListener('DOMContentLoaded', () => {
        initOverviewCache();
        initWebSocket();
    });
}
