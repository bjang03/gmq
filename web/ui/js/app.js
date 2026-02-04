const API_BASE = window.location.origin;

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

function renderMetrics(metrics) {
    const container = document.getElementById('metrics-container');
    container.innerHTML = '';

    let totalMessages = 0;
    let totalPublish = 0;
    let totalSubscribe = 0;
    let totalLatency = 0;
    let latencyCount = 0;

    for (const [name, metric] of Object.entries(metrics)) {
        totalMessages += metric.MessageCount || 0;
        totalPublish += metric.PublishCount || 0;
        totalSubscribe += metric.SubscribeCount || 0;
        if (metric.AverageLatency > 0) {
            totalLatency += metric.AverageLatency;
            latencyCount++;
        }

        const card = createMetricCard(name, metric);
        container.appendChild(card);
    }

    document.getElementById('total-messages').textContent = formatNumber(totalMessages);
    document.getElementById('total-publish').textContent = formatNumber(totalPublish);
    document.getElementById('total-subscribe').textContent = formatNumber(totalSubscribe);
    document.getElementById('avg-latency').textContent =
        latencyCount > 0 ? totalLatency.toFixed(2) + ' ms' : '0 ms';

    document.getElementById('last-update').textContent = new Date().toLocaleString('zh-CN');
}

function createMetricCard(name, metric) {
    const card = document.createElement('div');
    card.className = 'metric-card';

    const statusClass = metric.Status === 'connected' ? 'connected' : 'disconnected';

    card.innerHTML = `
        <div class="metric-header">
            <div class="metric-name">${name}</div>
            <div class="metric-status ${statusClass}">${metric.Status}</div>
        </div>
        <div class="metric-body">
            <div class="metric-grid">
                <div class="metric-item">
                    <div class="metric-item-label">消息总数</div>
                    <div class="metric-item-value">${formatNumber(metric.MessageCount)}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-item-label">发布数</div>
                    <div class="metric-item-value">${formatNumber(metric.PublishCount)}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-item-label">订阅数</div>
                    <div class="metric-item-value">${formatNumber(metric.SubscribeCount)}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-item-label">待处理</div>
                    <div class="metric-item-value">${formatNumber(metric.PendingMessages)}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-item-label">发布失败</div>
                    <div class="metric-item-value">${formatNumber(metric.PublishFailed)}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-item-label">订阅失败</div>
                    <div class="metric-item-value">${formatNumber(metric.SubscribeFailed)}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-item-label">平均延迟</div>
                    <div class="metric-item-value">${metric.AverageLatency.toFixed(2)} ms</div>
                </div>
                <div class="metric-item">
                    <div class="metric-item-label">Ping延迟</div>
                    <div class="metric-item-value">${metric.LastPingLatency.toFixed(2)} ms</div>
                </div>
            </div>
        </div>
        <div class="metric-footer">
            <div>连接时间: ${metric.ConnectedAt || '-'}</div>
            <div>吞吐量: ${metric.ThroughputPerSec.toFixed(2)} msg/s</div>
        </div>
    `;

    return card;
}

function formatNumber(num) {
    return new Intl.NumberFormat().format(num);
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
