// WebSocket连接
let ws = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_DELAY = 3000;

// 列表展示的完整指标配置（所有指标分类型）
const LIST_METRICS_CONFIG = {
    basic: [
        { key: 'name', label: '连接名称' },
        { key: 'status', label: '连接状态' },
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
        serverNodes: document.getElementById('server-nodes'),
        deadLetterCount: document.getElementById('dead-letter-count')
    };
}

// 更新概览统计
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
}

// 创建列表项结构（多行展示，类型跨行合并）
function createListItem(name, metric) {
    const typeInfo = TYPE_LABELS[metric.type] || {name: metric.type || 'Unknown', color: '#6b7280', icon: '?'};

    const item = document.createElement('div');
    item.className = 'metric-list-item';
    item.dataset.itemName = name;

    // 左侧信息区（跨所有行）- 只显示类型
    const infoDiv = document.createElement('div');
    infoDiv.className = 'metric-info';
    infoDiv.innerHTML = `
        <span class="type-badge" style="background: ${typeInfo.color}20; color: ${typeInfo.color}; border: 1px solid ${typeInfo.color}40;">
            ${typeInfo.icon} ${typeInfo.name}
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

                // 特殊处理连接状态，添加颜色样式
                if (config.key === 'status') {
                    const isConnected = value === 'connected';
                    const statusText = isConnected ? '已连接' : '未连接';
                    const statusClass = isConnected ? 'status-connected' : 'status-disconnected';
                    valueEl.innerHTML = `<span class="status-badge ${statusClass}">${statusText}</span>`;
                } else {
                    if (valueEl.textContent !== formatted) {
                        valueEl.textContent = formatted;
                    }
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
    if (!container) return;

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

    // 更新死信队列数量
    updateDeadLetterCount(metrics);
}

// 显示错误
function showError(error) {
    const container = document.getElementById('metrics-container');
    if (container) {
        container.innerHTML = `
            <div class="error">
                <div>❌ 加载失败</div>
                <div style="margin-top: 10px;">${error.message || error}</div>
            </div>
        `;
    }
}

// 初始化
document.addEventListener('DOMContentLoaded', () => {
    initOverviewCache();
    initWebSocket();

    // 初始化生成 mock 数据并更新死信队列数量
    currentDeadLetterMessages = generateMockDeadLetterMessages();
    currentMqName = 'redis';
    currentQueueName = 'default-queue';
    updateDeadLetterCountFromMock();
});

// ==================== 死信队列管理 ====================

let currentDeadLetterMessages = [];
let editingDeadLetterMessage = null;
let currentMqName = '';
let currentQueueName = '';
let currentPage = 1;
let pageSize = 20;

// Mock 死信消息数据
function generateMockDeadLetterMessages() {
    const messages = [];
    const queues = ['user-order', 'payment-queue', 'notification-queue', 'email-queue', 'log-queue'];
    const deadReasons = [
        '消息消费超时',
        '重试次数超过阈值',
        '业务逻辑处理失败',
        '数据格式错误',
        '下游服务不可用',
        '连接中断'
    ];
    const mqTypes = ['redis', 'rabbitmq', 'nats'];
    const servers = [
        '192.168.1.100:6379',
        '192.168.1.101:5672',
        '192.168.1.102:4222',
        '192.168.1.103:6379',
        '192.168.1.104:5672'
    ];

    for (let i = 1; i <= 50; i++) {
        messages.push({
            message_id: `msg_${Date.now()}_${i}`,
            queue_name: queues[Math.floor(Math.random() * queues.length)],
            queue_type: mqTypes[Math.floor(Math.random() * mqTypes.length)],
            server_addr: servers[Math.floor(Math.random() * servers.length)],
            timestamp: new Date(Date.now() - Math.random() * 86400000).toISOString(),
            delivery_tag: Math.floor(Math.random() * 1000000),
            dead_reason: deadReasons[Math.floor(Math.random() * deadReasons.length)],
            body: JSON.stringify({
                event: `event_${i}`,
                data: {
                    userId: 1000 + i,
                    orderId: `ORD${Date.now()}${i}`,
                    amount: (Math.random() * 1000).toFixed(2),
                    status: 'failed'
                },
                timestamp: Date.now()
            }),
            headers: {
                'Content-Type': 'application/json',
                'Retry-Count': (Math.floor(Math.random() * 5) + 1).toString(),
                'Original-Queue': queues[Math.floor(Math.random() * queues.length)],
                'Error-Code': 'E' + (Math.floor(Math.random() * 900) + 100)
            }
        });
    }

    return messages;
}

// 更新死信队列数量
function updateDeadLetterCount(metrics) {
    // 使用 mock 数据的数量
    if (domCache.overview.deadLetterCount) {
        domCache.overview.deadLetterCount.textContent = formatNumber(currentDeadLetterMessages.length);
    }
}

// 从 mock 数据更新死信队列数量
function updateDeadLetterCountFromMock() {
    if (domCache.overview.deadLetterCount) {
        domCache.overview.deadLetterCount.textContent = formatNumber(currentDeadLetterMessages.length);
    }
}

// 显示死信队列模态框
function showDeadLetterModal() {
    document.getElementById('deadletter-modal').classList.add('active');

    // 设置默认值
    document.getElementById('dlq-mq-select').value = currentMqName;
    document.getElementById('dlq-queue-input').value = currentQueueName;

    // 渲染数据
    currentPage = 1;
    renderDeadLetterMessages();
}

// 关闭死信队列模态框
function closeDeadLetterModal() {
    document.getElementById('deadletter-modal').classList.remove('active');
}

// 加载死信消息
async function loadDeadLetterMessages() {
    currentMqName = document.getElementById('dlq-mq-select').value;
    currentQueueName = document.getElementById('dlq-queue-input').value.trim();

    console.log('loadDeadLetterMessages 调用, MQ:', currentMqName, '队列:', currentQueueName);

    if (!currentMqName) {
        showToast('请选择 MQ', 'error');
        return;
    }

    if (!currentQueueName) {
        showToast('请输入队列名称', 'error');
        return;
    }

    const container = document.getElementById('dead-letter-list');
    container.innerHTML = `
        <div class="loading-state">
            <div class="loading-spinner"></div>
            <div>正在加载死信消息...</div>
        </div>
    `;

    // 直接使用 mock 数据
    console.log('使用 mock 数据');
    currentDeadLetterMessages = generateMockDeadLetterMessages();
    console.log('生成 mock 数据数量:', currentDeadLetterMessages.length);
    currentPage = 1;
    renderDeadLetterMessages();
    showToast('已加载 mock 数据', 'success');
}

// 刷新死信消息
async function refreshDeadLetterMessages() {
    if (currentMqName && currentQueueName) {
        await loadDeadLetterMessages();
        showToast('刷新成功', 'success');
    } else {
        showToast('请先选择 MQ 并输入队列名称', 'error');
    }
}

// 渲染死信消息列表
function renderDeadLetterMessages() {
    const wrapper = document.querySelector('.dead-letter-list-wrapper');
    const container = document.getElementById('dead-letter-list');

    console.log('renderDeadLetterMessages 调用，数据量:', currentDeadLetterMessages.length);

    if (!currentDeadLetterMessages || currentDeadLetterMessages.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">✓</div>
                <div class="empty-text">当前队列没有死信消息</div>
            </div>
        `;
        return;
    }

    // 计算分页
    const totalItems = currentDeadLetterMessages.length;
    const totalPages = Math.ceil(totalItems / pageSize);
    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const pageMessages = currentDeadLetterMessages.slice(startIndex, endIndex);

    console.log('当前页:', currentPage, '总页数:', totalPages, '当前页数据量:', pageMessages.length);

    let html = `
        <table class="dead-letter-table">
            <thead>
                <tr>
                    <th class="message-id">消息ID</th>
                    <th class="queue-name">队列名</th>
                    <th class="queue-type">队列类型</th>
                    <th class="server-addr">服务器地址</th>
                    <th class="timestamp">时间</th>
                    <th class="dead-reason">死信原因</th>
                    <th class="actions">操作</th>
                </tr>
            </thead>
            <tbody>
    `;

    pageMessages.forEach(msg => {
        html += `
            <tr data-message-id="${encodeURIComponent(msg.message_id || '')}">
                <td class="message-id">${escapeHtml(msg.message_id || 'N/A')}</td>
                <td class="queue-name">${escapeHtml(msg.queue_name || 'N/A')}</td>
                <td class="queue-type">${escapeHtml(msg.queue_type || 'N/A')}</td>
                <td class="server-addr">${escapeHtml(msg.server_addr || 'N/A')}</td>
                <td class="timestamp">${escapeHtml(msg.timestamp || 'N/A')}</td>
                <td class="dead-reason">${escapeHtml(msg.dead_reason || 'N/A')}</td>
                <td class="actions">
                    <button class="btn btn-primary btn-sm" onclick="retryDeadLetterMessage('${escapeHtml(msg.message_id || '')}')" title="重新执行">
                        🔄
                    </button>
                    <button class="btn btn-warning btn-sm" onclick="editDeadLetterMessage('${escapeHtml(msg.message_id || '')}')" title="编辑">
                        ✏️
                    </button>
                    <button class="btn btn-danger btn-sm" onclick="discardDeadLetterMessage('${escapeHtml(msg.message_id || '')}')" title="丢弃">
                        🗑️
                    </button>
                </td>
            </tr>
        `;
    });

    html += `
            </tbody>
        </table>
    `;

    container.innerHTML = html;

    // 添加分页控件到 wrapper 的底部
    const paginationHtml = `
        <div class="pagination">
            <button onclick="goToPage(1)" ${currentPage === 1 ? 'disabled' : ''}>首页</button>
            <button onclick="goToPage(${currentPage - 1})" ${currentPage === 1 ? 'disabled' : ''}>上一页</button>
            ${generatePageNumbers(currentPage, totalPages)}
            <button onclick="goToPage(${currentPage + 1})" ${currentPage === totalPages ? 'disabled' : ''}>下一页</button>
            <button onclick="goToPage(${totalPages})" ${currentPage === totalPages ? 'disabled' : ''}>末页</button>
            <span class="pagination-info">共 ${totalItems} 条，第 ${currentPage} / ${totalPages} 页</span>
        </div>
    `;

    // 先移除旧的分页
    const oldPagination = wrapper.querySelector('.pagination');
    if (oldPagination) {
        oldPagination.remove();
    }

    // 添加新的分页
    wrapper.insertAdjacentHTML('beforeend', paginationHtml);

    console.log('渲染完成');
}

// 生成页码按钮
function generatePageNumbers(current, total) {
    let html = '';
    const maxVisible = 5;

    if (total <= maxVisible) {
        for (let i = 1; i <= total; i++) {
            html += `<button onclick="goToPage(${i})" class="${i === current ? 'active' : ''}">${i}</button>`;
        }
    } else {
        if (current <= 3) {
            for (let i = 1; i <= 4; i++) {
                html += `<button onclick="goToPage(${i})" class="${i === current ? 'active' : ''}">${i}</button>`;
            }
            html += `<button disabled>...</button>`;
            html += `<button onclick="goToPage(${total})">${total}</button>`;
        } else if (current >= total - 2) {
            html += `<button onclick="goToPage(1)">1</button>`;
            html += `<button disabled>...</button>`;
            for (let i = total - 3; i <= total; i++) {
                html += `<button onclick="goToPage(${i})" class="${i === current ? 'active' : ''}">${i}</button>`;
            }
        } else {
            html += `<button onclick="goToPage(1)">1</button>`;
            html += `<button disabled>...</button>`;
            html += `<button onclick="goToPage(${current - 1})">${current - 1}</button>`;
            html += `<button onclick="goToPage(${current})" class="active">${current}</button>`;
            html += `<button onclick="goToPage(${current + 1})">${current + 1}</button>`;
            html += `<button disabled>...</button>`;
            html += `<button onclick="goToPage(${total})">${total}</button>`;
        }
    }

    return html;
}

// 跳转到指定页
function goToPage(page) {
    const totalPages = Math.ceil(currentDeadLetterMessages.length / pageSize);
    if (page < 1 || page > totalPages) return;

    currentPage = page;
    renderDeadLetterMessages();
    const container = document.getElementById('dead-letter-list');
    if (container) {
        container.scrollTop = 0;
    }
}

// 重新执行死信消息
async function retryDeadLetterMessage(messageId) {
    const message = currentDeadLetterMessages.find(m => m.message_id === messageId);
    if (!message) {
        showToast('消息不存在', 'error');
        return;
    }

    if (!confirm(`确定要重新执行消息 "${messageId}" 吗？`)) {
        return;
    }

    try {
        const response = await fetch('/api/deadletter/retry', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                mqName: currentMqName,
                queueName: currentQueueName,
                messageId: messageId
            })
        });

        const result = await response.json();

        if (result.code === 200) {
            showToast('消息已重新执行', 'success');
            await refreshDeadLetterMessages();
        } else {
            throw new Error(result.msg || '操作失败');
        }
    } catch (error) {
        console.error('重新执行失败:', error);
        showToast('重新执行失败: ' + error.message, 'error');
    }
}

// 编辑死信消息
function editDeadLetterMessage(messageId) {
    const message = currentDeadLetterMessages.find(m => m.message_id === messageId);
    if (!message) {
        showToast('消息不存在', 'error');
        return;
    }

    editingDeadLetterMessage = message;
    document.getElementById('edit-message-body').value = message.body || '';
    document.getElementById('edit-modal').classList.add('active');
}

// 关闭编辑模态框
function closeEditModal() {
    document.getElementById('edit-modal').classList.remove('active');
    editingDeadLetterMessage = null;
    document.getElementById('edit-message-body').value = '';
}

// 保存死信消息
async function saveDeadLetterMessage() {
    if (!editingDeadLetterMessage) {
        showToast('没有正在编辑的消息', 'error');
        return;
    }

    const newBody = document.getElementById('edit-message-body').value.trim();

    if (!newBody) {
        showToast('消息内容不能为空', 'error');
        return;
    }

    // 验证 JSON 格式（如果是 JSON）
    if (newBody.startsWith('{') || newBody.startsWith('[')) {
        try {
            JSON.parse(newBody);
        } catch (e) {
            if (!confirm('消息格式不是有效的 JSON，确定要保存吗？')) {
                return;
            }
        }
    }

    try {
        const response = await fetch('/api/deadletter/update', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                mqName: currentMqName,
                queueName: currentQueueName,
                messageId: editingDeadLetterMessage.message_id,
                newBody: newBody
            })
        });

        const result = await response.json();

        if (result.code === 200) {
            showToast('消息已更新', 'success');
            closeEditModal();
            await refreshDeadLetterMessages();
        } else {
            throw new Error(result.msg || '更新失败');
        }
    } catch (error) {
        console.error('更新失败:', error);
        showToast('更新失败: ' + error.message, 'error');
    }
}

// 丢弃死信消息
async function discardDeadLetterMessage(messageId) {
    const message = currentDeadLetterMessages.find(m => m.message_id === messageId);
    if (!message) {
        showToast('消息不存在', 'error');
        return;
    }

    if (!confirm(`确定要丢弃消息 "${messageId}" 吗？此操作不可恢复！`)) {
        return;
    }

    try {
        const response = await fetch('/api/deadletter/discard', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                mqName: currentMqName,
                queueName: currentQueueName,
                messageId: messageId
            })
        });

        const result = await response.json();

        if (result.code === 200) {
            showToast('消息已丢弃', 'success');
            await refreshDeadLetterMessages();
        } else {
            throw new Error(result.msg || '操作失败');
        }
    } catch (error) {
        console.error('丢弃失败:', error);
        showToast('丢弃失败: ' + error.message, 'error');
    }
}

// 显示 Toast 提示
function showToast(message, type = 'success') {
    const existingToasts = document.querySelectorAll('.toast');
    existingToasts.forEach(toast => toast.remove());

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    document.body.appendChild(toast);

    setTimeout(() => {
        toast.style.animation = 'slideInRight 0.3s ease reverse';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// HTML 转义
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// 点击模态框外部关闭
document.addEventListener('click', (e) => {
    const deadletterModal = document.getElementById('deadletter-modal');
    const editModal = document.getElementById('edit-modal');

    if (e.target === deadletterModal) {
        closeDeadLetterModal();
    }
    if (e.target === editModal) {
        closeEditModal();
    }
});

// ESC 键关闭模态框
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeDeadLetterModal();
        closeEditModal();
    }
});

