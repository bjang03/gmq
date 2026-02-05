const API_BASE = window.location.origin;

// 指标配置定义
const METRIC_CONFIG = {
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

async function fetchAllMetrics() {
    const response = await fetch(`${API_BASE}/metrics/all`);
    const result = await response.json();
    if (result.code !== 200) {
        throw new Error(result.msg || '获取监控数据失败');
    }
    return result.data;
}

function formatValue(value, format) {
    if (value === undefined || value === null || value === '' || value === 0) return '-';
    
    switch (format) {
        case 'number': return new Intl.NumberFormat().format(value);
        case 'ms': return value.toFixed(2) + ' ms';
        case 'perSec': return value.toFixed(2) + ' /s';
        case 'percent': return value.toFixed(2) + '%';
        case 'bytes': {
            if (value === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(value) / Math.log(k));
            return parseFloat((value / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
        case 'duration': {
            if (!value) return '-';
            const days = Math.floor(value / 86400);
            const hours = Math.floor((value % 86400) / 3600);
            const mins = Math.floor((value % 3600) / 60);
            const secs = Math.floor(value % 60);
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

// 初始化概览统计的 DOM 引用
function initOverviewCache() {
    domCache.overview = {
        totalMessages: document.getElementById('total-messages'),
        totalPublish: document.getElementById('total-publish'),
        totalSubscribe: document.getElementById('total-subscribe'),
        avgLatency: document.getElementById('avg-latency'),
        activeConnections: document.getElementById('active-connections'),
        serverNodes: document.getElementById('server-nodes'),
        lastUpdate: document.getElementById('last-update')
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
    domCache.overview.lastUpdate.textContent = new Date().toLocaleString('zh-CN');
}

// 创建指标项元素
function createMetricItem(config) {
    const item = document.createElement('div');
    item.className = 'metric-item';
    item.dataset.metricKey = config.key;
    
    item.innerHTML = `
        <div class="metric-item-icon">${config.icon}</div>
        <div class="metric-item-content">
            <div class="metric-item-label">${config.label}</div>
            <div class="metric-item-value" data-value-key="${config.key}">-</div>
        </div>
    `;
    return item;
}

// 创建卡片结构（首次渲染）
function createCardStructure(name, metric) {
    const typeInfo = TYPE_LABELS[metric.type] || { name: metric.type || 'Unknown', color: '#6b7280', icon: '?' };
    
    const card = document.createElement('div');
    card.className = 'metric-card';
    card.dataset.cardName = name;
    
    // Header
    const header = document.createElement('div');
    header.className = 'metric-header';
    header.innerHTML = `
        <div class="metric-title">
            <span class="type-badge" style="background: ${typeInfo.color}20; color: ${typeInfo.color}; border: 1px solid ${typeInfo.color}40;">
                ${typeInfo.icon} ${typeInfo.name}
            </span>
            <span class="metric-name">${name}</span>
        </div>
        <div class="metric-status ${metric.status === 'connected' ? 'connected' : 'disconnected'}" data-status>
            ${metric.status}
        </div>
    `;
    card.appendChild(header);
    
    // Body
    const body = document.createElement('div');
    body.className = 'metric-body';
    
    // 创建各分区
    const sections = [
        { title: '基本信息', configs: METRIC_CONFIG.basic },
        { title: '📊 服务端消息统计', configs: METRIC_CONFIG.serverMessages },
        { title: '💻 客户端消息统计', configs: METRIC_CONFIG.clientMessages },
        { title: '⏱️ 延迟指标(客户端)', configs: METRIC_CONFIG.latency },
        { title: '📈 吞吐量(客户端)', configs: METRIC_CONFIG.throughput }
    ];
    
    for (const section of sections) {
        const sectionEl = document.createElement('div');
        sectionEl.className = 'metric-section';
        sectionEl.dataset.section = section.title;
        sectionEl.style.display = 'none'; // 初始隐藏，有数据时再显示
        
        const titleEl = document.createElement('div');
        titleEl.className = 'section-title';
        titleEl.textContent = section.title;
        sectionEl.appendChild(titleEl);
        
        const grid = document.createElement('div');
        grid.className = 'metric-grid';
        
        for (const config of section.configs) {
            grid.appendChild(createMetricItem(config));
        }
        
        sectionEl.appendChild(grid);
        body.appendChild(sectionEl);
    }
    
    // 服务端详细信息区
    const serverSection = document.createElement('div');
    serverSection.className = 'metric-section';
    serverSection.dataset.section = 'serverMetrics';
    serverSection.style.display = 'none';
    serverSection.innerHTML = '<div class="section-title">🖥️ 服务端详细信息</div>';
    const serverGrid = document.createElement('div');
    serverGrid.className = 'metric-grid';
    serverGrid.dataset.serverGrid = 'true';
    serverSection.appendChild(serverGrid);
    body.appendChild(serverSection);
    
    // 扩展指标区
    const extSection = document.createElement('div');
    extSection.className = 'metric-section';
    extSection.dataset.section = 'extensions';
    extSection.style.display = 'none';
    extSection.innerHTML = '<div class="section-title">🔧 扩展指标</div>';
    const extGrid = document.createElement('div');
    extGrid.className = 'metric-grid';
    extGrid.dataset.extGrid = 'true';
    extSection.appendChild(extGrid);
    body.appendChild(extSection);
    
    card.appendChild(body);
    return card;
}

// 更新单个卡片的值
function updateCardValues(name, metric) {
    const card = document.querySelector(`.metric-card[data-card-name="${name}"]`);
    if (!card) return;
    
    // 更新状态
    const statusEl = card.querySelector('[data-status]');
    if (statusEl && statusEl.textContent !== metric.status) {
        statusEl.textContent = metric.status;
        statusEl.className = `metric-status ${metric.status === 'connected' ? 'connected' : 'disconnected'}`;
    }
    
    // 更新各分区的值
    for (const [sectionName, configs] of Object.entries(METRIC_CONFIG)) {
        const sectionEl = card.querySelector(`[data-section="${getSectionTitle(sectionName)}"]`);
        if (!sectionEl) continue;
        
        let hasVisibleData = false;
        
        for (const config of configs) {
            const valueEl = sectionEl.querySelector(`[data-value-key="${config.key}"]`);
            if (!valueEl) continue;
            
            let value = metric[config.key];
            if (value === undefined || value === null || value === '' || value === 0) {
                valueEl.textContent = '-';
                valueEl.parentElement.parentElement.style.display = 'none';
            } else {
                const formatted = config.format ? formatValue(value, config.format) : formatNumber(value);
                if (valueEl.textContent !== formatted) {
                    valueEl.textContent = formatted;
                }
                valueEl.parentElement.parentElement.style.display = 'flex';
                hasVisibleData = true;
                
                // 状态特殊样式
                if (config.isStatus) {
                    valueEl.className = 'metric-item-value ' + (value === 'connected' ? 'text-success' : 'text-error');
                }
            }
        }
        
        sectionEl.style.display = hasVisibleData ? 'block' : 'none';
    }
    
    // 更新服务端详细信息
    updateServerMetrics(card, metric.serverMetrics);
    
    // 更新扩展指标
    updateExtensions(card, metric.extensions);
}

function getSectionTitle(sectionName) {
    const titles = {
        basic: '基本信息',
        serverMessages: '📊 服务端消息统计',
        clientMessages: '💻 客户端消息统计',
        latency: '⏱️ 延迟指标(客户端)',
        throughput: '📈 吞吐量(客户端)'
    };
    return titles[sectionName] || sectionName;
}

// 更新服务端指标
function updateServerMetrics(card, serverMetrics) {
    const section = card.querySelector('[data-section="serverMetrics"]');
    const grid = section.querySelector('[data-server-grid]');
    
    if (!serverMetrics || Object.keys(serverMetrics).length === 0) {
        section.style.display = 'none';
        return;
    }
    
    const fields = [
        { key: 'serverVersion', label: '版本', icon: '🏷️' },
        { key: 'serverId', label: '服务器ID', icon: '🆔', format: 'shortId' },
        { key: 'totalConnections', label: '总连接数', icon: '👥' },
        { key: 'activeConnections', label: '活跃连接', icon: '✅' },
        { key: 'slowConsumers', label: '慢消费者', icon: '🐌' },
        { key: 'totalConsumers', label: '消费者数', icon: '👤' },
        { key: 'memoryUsed', label: '内存使用', icon: '💾', format: 'bytes' },
        { key: 'cpuUsage', label: 'CPU使用', icon: '💻', format: 'percent' }
    ];
    
    let hasData = false;
    
    for (const field of fields) {
        const value = serverMetrics[field.key];
        let item = grid.querySelector(`[data-server-key="${field.key}"]`);
        
        if (!value || value === 0 || value === '') {
            if (item) item.style.display = 'none';
            continue;
        }
        
        hasData = true;
        
        if (!item) {
            item = createMetricItem({ key: field.key, label: field.label, icon: field.icon });
            item.dataset.serverKey = field.key;
            grid.appendChild(item);
        }
        item.style.display = 'flex';
        
        const valueEl = item.querySelector(`[data-value-key="${field.key}"]`);
        let displayValue = value;
        if (field.format === 'number') displayValue = formatNumber(value);
        else if (field.format === 'bytes') displayValue = formatValue(value, 'bytes');
        else if (field.format === 'percent') displayValue = value + '%';
        else if (field.format === 'shortId') displayValue = String(value).substring(0, 8) + '...';
        
        if (valueEl.textContent !== displayValue) {
            valueEl.textContent = displayValue;
        }
    }
    
    section.style.display = hasData ? 'block' : 'none';
}

// 更新扩展指标
function updateExtensions(card, extensions) {
    const section = card.querySelector('[data-section="extensions"]');
    const grid = section.querySelector('[data-ext-grid]');
    
    if (!extensions || Object.keys(extensions).length === 0) {
        section.style.display = 'none';
        return;
    }
    
    let hasData = false;
    
    for (const [key, value] of Object.entries(extensions)) {
        if (!value || (typeof value === 'number' && value === 0)) continue;
        
        hasData = true;
        
        let item = grid.querySelector(`[data-ext-key="${key}"]`);
        if (!item) {
            item = createMetricItem({ key, label: key, icon: '🔧' });
            item.dataset.extKey = key;
            grid.appendChild(item);
        }
        item.style.display = 'flex';
        
        const valueEl = item.querySelector(`[data-value-key="${key}"]`);
        const displayValue = typeof value === 'object' ? JSON.stringify(value) : String(value);
        if (valueEl.textContent !== displayValue) {
            valueEl.textContent = displayValue;
        }
    }
    
    section.style.display = hasData ? 'block' : 'none';
}

// 主渲染函数
function renderMetrics(metrics) {
    const container = document.getElementById('metrics-container');
    
    // 更新或创建卡片
    for (const [name, metric] of Object.entries(metrics)) {
        let card = document.querySelector(`.metric-card[data-card-name="${name}"]`);
        
        if (!card) {
            // 首次渲染创建结构
            card = createCardStructure(name, metric);
            container.appendChild(card);
        }
        
        // 更新值（不重新创建元素）
        updateCardValues(name, metric);
    }
    
    // 删除已经不存在的卡片
    const existingCards = container.querySelectorAll('.metric-card');
    for (const card of existingCards) {
        const cardName = card.dataset.cardName;
        if (!metrics[cardName]) {
            card.remove();
        }
    }
    
    // 更新概览
    updateOverview(metrics);
}

// 显示加载状态
function showLoading() {
    const container = document.getElementById('metrics-container');
    if (container.children.length === 0) {
        container.innerHTML = '<div class="loading">加载中...</div>';
    }
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

// 刷新指标
async function refreshMetrics() {
    try {
        const metrics = await fetchAllMetrics();
        renderMetrics(metrics);
    } catch (error) {
        console.error('刷新指标失败:', error);
        showError(error);
    }
}

// 初始化
document.addEventListener('DOMContentLoaded', () => {
    initOverviewCache();
    showLoading();
    refreshMetrics();
    setInterval(refreshMetrics, 5000);
});
