// 死信队列页面脚本

let currentDeadLetterMessages = [];
let editingDeadLetterMessage = null;
let currentMqName = '';
let currentPipelineName = '';
let currentQueueName = '';

// 页面加载时初始化
document.addEventListener('DOMContentLoaded', () => {
    const { mqName, pipelineName } = getMQInfo();

    if (!mqName) {
        // 没有 MQ 信息，让用户选择
        showMQSelector();
        return;
    }

    currentMqName = mqName;
    currentPipelineName = pipelineName;

    // 显示 MQ 信息
    const typeInfo = getTypeInfo(mqName);
    document.getElementById('mq-info').innerHTML = `
        <span class="type-badge" style="background: ${typeInfo.color}20; color: ${typeInfo.color}; border: 1px solid ${typeInfo.color}40;">
            ${typeInfo.icon} ${typeInfo.name} - ${pipelineName}
        </span>
    `;
});

// 显示 MQ 选择器
function showMQSelector() {
    document.getElementById('mq-info').innerHTML = `
        <select id="mq-select" class="form-select" onchange="selectMQ()">
            <option value="">选择 MQ</option>
            <option value="redis">Redis Stream</option>
            <option value="rabbitmq">RabbitMQ</option>
            <option value="nats">NATS</option>
        </select>
    `;
}

// 选择 MQ
function selectMQ() {
    const mqSelect = document.getElementById('mq-select');
    const mqName = mqSelect.value;

    if (!mqName) {
        return;
    }

    currentMqName = mqName;
    currentPipelineName = mqName;

    // 保存到 sessionStorage
    sessionStorage.setItem('dlq-mq-name', mqName);
    sessionStorage.setItem('dlq-pipeline-name', mqName);

    // 更新显示
    const typeInfo = getTypeInfo(mqName);
    document.getElementById('mq-info').innerHTML = `
        <span class="type-badge" style="background: ${typeInfo.color}20; color: ${typeInfo.color}; border: 1px solid ${typeInfo.color}40;">
            ${typeInfo.icon} ${typeInfo.name}
        </span>
    `;
}

// 返回上一页
function goBack() {
    window.location.href = '/ui/html/index.html';
}

// 从 sessionStorage 获取 MQ 信息
function getMQInfo() {
    return {
        mqName: sessionStorage.getItem('dlq-mq-name') || '',
        pipelineName: sessionStorage.getItem('dlq-pipeline-name') || ''
    };
}

// 获取类型信息
function getTypeInfo(mqType) {
    const TYPE_LABELS = {
        'nats': { name: 'NATS', color: '#4f46e5', icon: '🚀' },
        'redis': { name: 'Redis Stream', color: '#dc2626', icon: '🔴' },
        'rabbitmq': { name: 'RabbitMQ', color: '#ea580c', icon: '🐰' },
        'kafka': { name: 'Kafka', color: '#0891b2', icon: '📊' }
    };
    return TYPE_LABELS[mqType] || { name: mqType || 'Unknown', color: '#6b7280', icon: '?' };
}

// 加载死信消息
async function loadDeadLetterMessages() {
    currentQueueName = document.getElementById('dlq-queue-input').value.trim();

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

    try {
        const response = await fetch(`/api/deadletter?mqName=${encodeURIComponent(currentMqName)}&queueName=${encodeURIComponent(currentQueueName)}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const result = await response.json();

        if (result.code === 200) {
            currentDeadLetterMessages = result.data || [];
            renderDeadLetterMessages(currentDeadLetterMessages);
        } else {
            throw new Error(result.msg || '加载失败');
        }
    } catch (error) {
        console.error('加载死信消息失败:', error);
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">❌</div>
                <div class="empty-text">加载失败: ${error.message}</div>
            </div>
        `;
        showToast('加载死信消息失败: ' + error.message, 'error');
    }
}

// 刷新死信消息
async function refreshDeadLetterMessages() {
    if (currentQueueName) {
        await loadDeadLetterMessages();
        showToast('刷新成功', 'success');
    } else {
        showToast('请先输入队列名称', 'error');
    }
}

// 渲染死信消息列表
function renderDeadLetterMessages(messages) {
    const container = document.getElementById('dead-letter-list');

    if (!messages || messages.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">✓</div>
                <div class="empty-text">当前队列没有死信消息</div>
            </div>
        `;
        return;
    }

    container.innerHTML = messages.map(msg => `
        <div class="dead-letter-item" data-message-id="${encodeURIComponent(msg.message_id || '')}">
            <div class="dead-letter-header">
                <div class="dead-letter-info">
                    <div class="dead-letter-id">消息ID: ${escapeHtml(msg.message_id || 'N/A')}</div>
                    <div class="dead-letter-meta">
                        <span>📦 队列: ${escapeHtml(msg.queue_name || 'N/A')}</span>
                        <span>🕐 时间: ${escapeHtml(msg.timestamp || 'N/A')}</span>
                        ${msg.delivery_tag ? `<span>🏷️ 标签: ${msg.delivery_tag}</span>` : ''}
                    </div>
                </div>
                <div class="dead-letter-actions">
                    <button class="btn btn-primary btn-sm" onclick="retryDeadLetterMessage('${escapeHtml(msg.message_id || '')}')" title="重新执行">
                        🔄 重新执行
                    </button>
                    <button class="btn btn-warning btn-sm" onclick="editDeadLetterMessage('${escapeHtml(msg.message_id || '')}')" title="编辑">
                        ✏️ 编辑
                    </button>
                    <button class="btn btn-danger btn-sm" onclick="discardDeadLetterMessage('${escapeHtml(msg.message_id || '')}')" title="丢弃">
                        🗑️ 丢弃
                    </button>
                </div>
            </div>
            ${msg.dead_reason ? `<span class="dead-reason">☠️ ${escapeHtml(msg.dead_reason)}</span>` : ''}
            <div class="dead-letter-body">${escapeHtml(msg.body || 'N/A')}</div>
            ${msg.headers && Object.keys(msg.headers).length > 0 ? `
                <div class="dead-letter-headers">
                    <div class="dead-letter-headers-title">消息头</div>
                    ${Object.entries(msg.headers).map(([key, value]) => `
                        <div class="dead-letter-header-item">
                            <span class="dead-letter-header-key">${escapeHtml(key)}:</span>
                            <span class="dead-letter-header-value">${escapeHtml(String(value))}</span>
                        </div>
                    `).join('')}
                </div>
            ` : ''}
        </div>
    `).join('');
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
    const modal = document.getElementById('edit-modal');
    if (e.target === modal) {
        closeEditModal();
    }
});

// ESC 键关闭模态框
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeEditModal();
    }
});
