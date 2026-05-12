/* ============================================
   7L街舞工作室 - 前端应用
   ============================================ */

// ── 配置 ──────────────────────────────────
const API_BASE = '/api/v1';

// 从 localStorage 获取 API Key（部署时配置）
function getApiKey() {
    return localStorage.getItem('7l_api_key') || '';
}

// 清除已保存的 API Key（鉴权失败时调用）
function clearApiKey() {
    localStorage.removeItem('7l_api_key');
}

// ── 状态 ──────────────────────────────────
let currentPage = 'dashboard';
let cardPrices = [];
let allStudents = [];
let currentStudent = null;

// ── 初始化 ────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    updateTime();
    setInterval(updateTime, 60000);
    navigate('dashboard');
    loadCardPrices();
});

function updateTime() {
    const now = new Date();
    const h = now.getHours().toString().padStart(2, '0');
    const m = now.getMinutes().toString().padStart(2, '0');
    document.getElementById('headerTime').textContent = `${h}:${m}`;
}

// ── 导航 ──────────────────────────────────
function navigate(page, data) {
    currentPage = page;
    
    // 更新底部导航高亮（4个主tab + 子页面）
    const mainPages = ['dashboard', 'students', 'analytics', 'settings', 'ai'];
    document.querySelectorAll('.nav-item').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.page === page || 
            (page === 'student-detail' && btn.dataset.page === 'students'));
    });

    const main = document.getElementById('mainContent');
    
    switch(page) {
        case 'dashboard': renderDashboard(); break;
        case 'students': renderStudentList(); break;
        case 'student-detail': renderStudentDetail(data); break;
        case 'ai': renderAIPage(); break;
        case 'analytics': renderAnalytics(); break;
        case 'settings': renderSettings(); break;
        default: renderDashboard();
    }
}

// ── API 调用 ──────────────────────────────
async function api(endpoint, options = {}) {
    try {
        const apiKey = getApiKey();
        const headers = { 'Content-Type': 'application/json' };
        if (apiKey) {
            headers['Authorization'] = `Bearer ${apiKey}`;
        }
        const resp = await fetch(`${API_BASE}${endpoint}`, {
            headers,
            ...options,
        });

        // 401 未授权 → 清除旧 key，提示重新输入
        if (resp.status === 401) {
            clearApiKey();
            const newKey = prompt('API Key 无效或已过期，请重新输入:');
            if (newKey) {
                localStorage.setItem('7l_api_key', newKey);
                // 重试一次
                headers['Authorization'] = `Bearer ${newKey}`;
                const retryResp = await fetch(`${API_BASE}${endpoint}`, { headers, ...options });
                const retryData = await retryResp.json();
                if (!retryResp.ok) {
                    throw new Error(retryData.detail || '鉴权失败');
                }
                return retryData;
            }
            throw new Error('未授权：请提供有效的 API Key');
        }

        const data = await resp.json();
        if (!resp.ok) {
            throw new Error(data.detail || '请求失败');
        }
        return data;
    } catch (err) {
        console.error('API Error:', err);
        throw err;
    }
}

// ── Toast 通知 ────────────────────────────
function showToast(message, type = 'success') {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    const icons = { success: '✅', error: '❌', warning: '⚠️' };
    toast.innerHTML = `<span>${icons[type] || ''}</span><span>${message}</span>`;
    container.appendChild(toast);
    setTimeout(() => toast.remove(), 3000);
}

// ── 模态框 ────────────────────────────────
function openModal(html) {
    document.getElementById('modalContent').innerHTML = html;
    document.getElementById('modalOverlay').classList.add('show');
}

function closeModal() {
    document.getElementById('modalOverlay').classList.remove('show');
}

// ── 加载卡种定价 ──────────────────────────
async function loadCardPrices() {
    try {
        const data = await api('/card-prices');
        cardPrices = data.prices || [];
    } catch(e) {
        console.error('加载卡种定价失败', e);
    }
}

// ── 仪表盘（3区版：经营数据 + 待办提醒 + 快捷操作） ────────
async function renderDashboard() {
    const main = document.getElementById('mainContent');
    main.innerHTML = `<div class="page"><div class="loading"><div class="spinner"></div>加载中...</div></div>`;

    try {
        const [stats, recentOps] = await Promise.all([
            api('/stats'),
            api('/recent-operations').catch(() => ({ operations: [] })),
        ]);

        main.innerHTML = `
        <div class="page">
            <!-- ① 经营数据区（顶部） -->
            <div class="section-title">📊 经营数据</div>
            <div class="dashboard-metrics">
                <div class="metric-card metric-primary" onclick="navigate('analytics')">
                    <div class="metric-value">¥${(stats.month_class_revenue || 0).toLocaleString()}</div>
                    <div class="metric-label">本月课消金额</div>
                    <div class="metric-sub">点击查看趋势 →</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">${stats.month_class_count || 0}</div>
                    <div class="metric-label">本月课消次数</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value text-accent">${stats.month_new_students || 0}</div>
                    <div class="metric-label">本月新学员</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">${stats.renew_rate || 0}%</div>
                    <div class="metric-label">续费率</div>
                    <div class="metric-sub">${stats.renew_count || 0}次续费</div>
                </div>
            </div>

            <!-- ② 待办提醒区（中部） -->
            <div class="section-title">🔔 待办提醒</div>
            <div class="dashboard-reminders">
                ${stats.birthday_soon && stats.birthday_soon.length > 0 ? `
                    <div class="reminder-section">
                        <div class="reminder-section-title">🎂 即将过生日（7天内）</div>
                        ${stats.birthday_soon.map(s => `
                            <div class="reminder-item" onclick="navigate('student-detail', '${s.name}')">
                                <div class="reminder-name">${s.name}</div>
                                <div class="reminder-detail">${s.birthday} · ${s.days_until === 0 ? '今天！' : s.days_until + '天后'}</div>
                            </div>
                        `).join('')}
                    </div>
                ` : ''}
                ${stats.expiring_soon && stats.expiring_soon.length > 0 ? `
                    <div class="reminder-section">
                        <div class="reminder-section-title">⚠️ 即将过期（7天内）</div>
                        ${stats.expiring_soon.map(s => `
                            <div class="reminder-item warning" onclick="navigate('student-detail', '${s.name}')">
                                <div class="reminder-name">${s.name}</div>
                                <div class="reminder-detail">${s.card_name} · ${s.expire_date}</div>
                            </div>
                        `).join('')}
                    </div>
                ` : ''}
                ${stats.low_hours && stats.low_hours.length > 0 ? `
                    <div class="reminder-section">
                        <div class="reminder-section-title">🔋 课时不足（≤2次）</div>
                        ${stats.low_hours.map(s => `
                            <div class="reminder-item danger" onclick="navigate('student-detail', '${s.name}')">
                                <div class="reminder-name">${s.name}</div>
                                <div class="reminder-detail">${s.card_name} · 剩余 ${s.remaining} 次</div>
                            </div>
                        `).join('')}
                    </div>
                ` : ''}
                ${(!stats.birthday_soon || stats.birthday_soon.length === 0) && (!stats.expiring_soon || stats.expiring_soon.length === 0) && (!stats.low_hours || stats.low_hours.length === 0) ? `
                    <div class="empty-state" style="padding:20px">
                        <div class="text">✅ 暂无待办提醒</div>
                    </div>
                ` : ''}
            </div>

            <!-- ③ 快捷操作区（底部） -->
            <div class="section-title" style="margin-top:8px">⚡ 快捷操作</div>
            <div class="quick-actions">
                <button class="quick-action-btn qa-ocr" onclick="showOCRModal()">
                    <div class="quick-action-icon">📸</div>
                    <div class="quick-action-text">截图识别</div>
                </button>
                <button class="quick-action-btn manual-btn" onclick="showRegisterModal()">
                    <div class="quick-action-icon">✏️</div>
                    <div class="quick-action-text">手动录入</div>
                </button>
                <button class="quick-action-btn import-btn" onclick="showImportModal()">
                    <div class="quick-action-icon">📥</div>
                    <div class="quick-action-text">批量导入</div>
                </button>
            </div>

            <!-- 最近操作记录 -->
            <div class="section-title" style="margin-top:4px">📋 最近操作</div>
            <div id="recentOpsList">
                ${recentOps.operations && recentOps.operations.length > 0 ? 
                    recentOps.operations.map(op => `
                        <div class="op-record-item ${op.undone ? 'op-undone' : ''}">
                            <div class="op-record-left">
                                <span class="op-type-badge ${op.type === '扣课' ? 'badge-danger' : op.type === '录入' ? 'badge-success' : op.type === '续费' ? 'badge-accent' : 'badge-info'}">${op.type}</span>
                                <span class="op-student-name">${op.student_name}</span>
                            </div>
                            <div class="op-record-right">
                                <span class="op-time">${formatOpTime(op.time_ms)}</span>
                                ${op.can_undo ? `<button class="op-undo-btn" onclick="undoOperation('${op.record_id}')">撤销</button>` : ''}
                                ${op.undone ? '<span class="op-undone-tag">已撤销</span>' : ''}
                            </div>
                        </div>
                    `).join('')
                    : '<div class="empty-state" style="padding:20px"><div class="text">暂无操作记录</div></div>'
                }
            </div>
        </div>
        `;
    } catch(e) {
        main.innerHTML = `<div class="page"><div class="empty-state"><div class="icon">😵</div><div class="text">加载失败: ${e.message}</div></div></div>`;
    }
}

// 格式化操作时间为相对时间
function formatOpTime(timeMs) {
    if (!timeMs) return '';
    const now = Date.now();
    const diff = now - timeMs;
    const minutes = Math.floor(diff / 60000);
    const hours = Math.floor(diff / 3600000);
    const days = Math.floor(diff / 86400000);
    if (minutes < 1) return '刚刚';
    if (minutes < 60) return `${minutes}分钟前`;
    if (hours < 24) return `${hours}小时前`;
    if (days < 7) return `${days}天前`;
    return new Date(timeMs).toLocaleDateString('zh-CN', {month:'numeric', day:'numeric'});
}

// 撤销操作
async function undoOperation(recordId) {
    if (!confirm('确认撤销此操作？')) return;
    try {
        const result = await api(`/undo/${recordId}`, { method: 'POST' });
        showToast(result.message);
        renderDashboard();
    } catch(e) {
        showToast(e.message, 'error');
    }
}

// 跳转到学员列表并筛选即将过期
function filterExpiringStudents() {
    navigate('students');
    // 等待学员列表加载后设置筛选
    setTimeout(() => {
        const statusChips = document.querySelectorAll('.chip[data-filter="status"]');
        statusChips.forEach(c => {
            if (c.dataset.value === '已过期') {
                c.click();
            }
        });
    }, 500);
}

// ── 学员列表 ──────────────────────────────
async function renderStudentList() {
    const main = document.getElementById('mainContent');
    main.innerHTML = `
    <div class="page">
        <div class="section-title">👥 学员列表</div>
        <div class="search-bar">
            <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
            <input type="text" id="searchInput" placeholder="搜索姓名/会员号/电话" oninput="filterStudents()">
        </div>
        <div class="filter-row">
            <button class="chip active" data-filter="status" data-value="" onclick="setFilter(this, 'status')">全部</button>
            <button class="chip" data-filter="status" data-value="有效" onclick="setFilter(this, 'status')">有效</button>
            <button class="chip" data-filter="status" data-value="已过期" onclick="setFilter(this, 'status')">已过期</button>
            <button class="chip" data-filter="status" data-value="已退卡" onclick="setFilter(this, 'status')">已退卡</button>
        </div>
        <div class="filter-row">
            <button class="chip active" data-filter="type" data-value="" onclick="setFilter(this, 'type')">全部卡型</button>
            <button class="chip" data-filter="type" data-value="次卡" onclick="setFilter(this, 'type')">次卡</button>
            <button class="chip" data-filter="type" data-value="月卡" onclick="setFilter(this, 'type')">月卡</button>
            <button class="chip" data-filter="type" data-value="期卡" onclick="setFilter(this, 'type')">期卡</button>
            <button class="chip" data-filter="type" data-value="体验卡" onclick="setFilter(this, 'type')">体验卡</button>
        </div>
        <div id="studentList"><div class="loading"><div class="spinner"></div>加载中...</div></div>
    </div>
    `;

    await loadStudents();
}

let filterStatus = '';
let filterType = '';

function setFilter(el, type) {
    const value = el.dataset.value;
    if (type === 'status') filterStatus = value;
    if (type === 'type') filterType = value;

    // 更新chip样式
    el.parentElement.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
    el.classList.add('active');

    filterStudents();
}

async function loadStudents() {
    try {
        const data = await api(`/students?card_status=${filterStatus}&card_type=${filterType}`);
        allStudents = data.students || [];
        filterStudents();
    } catch(e) {
        document.getElementById('studentList').innerHTML = `<div class="empty-state"><div class="icon">😵</div><div class="text">加载失败</div></div>`;
    }
}

function filterStudents() {
    const search = (document.getElementById('searchInput')?.value || '').toLowerCase();
    let filtered = allStudents;

    if (search) {
        filtered = filtered.filter(s =>
            s.name.toLowerCase().includes(search) ||
            s.member_id.toLowerCase().includes(search) ||
            (s.phone && s.phone.includes(search)) ||
            (s.wechat && s.wechat.toLowerCase().includes(search))
        );
    }

    const listEl = document.getElementById('studentList');
    if (!listEl) return;

    if (filtered.length === 0) {
        listEl.innerHTML = `<div class="empty-state"><div class="icon">🔍</div><div class="text">没有找到学员</div></div>`;
        return;
    }

    listEl.innerHTML = filtered.map(s => {
        const statusClass = s.card_status === '有效' ? 'badge-success' :
                           s.card_status === '已过期' ? 'badge-warning' :
                           s.card_status === '已退卡' ? 'badge-danger' : 'badge-info';
        return `
        <div class="list-item" onclick="navigate('student-detail', '${s.name}')">
            <div class="list-item-left">
                <div class="list-item-name">${s.name}</div>
                <div class="list-item-sub">${s.card_name} · ${s.card_type}</div>
            </div>
            <div class="list-item-right">
                <span class="badge ${statusClass}">${s.card_status}</span>
                <div class="text-muted" style="font-size:12px;margin-top:2px">
                    ${s.card_type === '月卡' ? '不限次' : `剩${s.remaining_hours}次`}
                </div>
            </div>
        </div>
        `;
    }).join('');
}

// ── 学员详情（新3tab版：操作按钮按频率排序） ──────
async function renderStudentDetail(name) {
    const main = document.getElementById('mainContent');
    main.innerHTML = `<div class="page"><div class="loading"><div class="spinner"></div>加载中...</div></div>`;

    try {
        const s = await api(`/students/${encodeURIComponent(name)}`);
        currentStudent = s;

        const statusClass = s.card_status === '有效' ? 'badge-success' :
                           s.card_status === '已过期' ? 'badge-warning' :
                           s.card_status === '已退卡' ? 'badge-danger' : 'badge-info';

        main.innerHTML = `
        <div class="page">
            <button class="back-btn" onclick="navigate('students')">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 12H5"/><path d="M12 19l-7-7 7-7"/></svg>
                返回学员列表
            </button>

            <div class="detail-header">
                <div class="detail-avatar">${s.name.charAt(0)}</div>
                <div class="detail-name">${s.name}</div>
                <div class="detail-sub">
                    ${s.member_id} · <span class="badge ${statusClass}">${s.card_status}</span>
                </div>
            </div>

            <div class="info-grid mb-16">
                <div class="info-item">
                    <div class="label">卡种</div>
                    <div class="value">${s.card_name || '-'}</div>
                </div>
                <div class="info-item">
                    <div class="label">卡类型</div>
                    <div class="value">${s.card_type || '-'}</div>
                </div>
                <div class="info-item">
                    <div class="label">${s.card_type === '月卡' ? '状态' : '剩余课时'}</div>
                    <div class="value ${s.card_type !== '月卡' && s.remaining_hours <= 2 ? 'text-danger' : 'text-success'}">
                        ${s.card_type === '月卡' ? '不限次' : s.remaining_hours + '次'}
                    </div>
                </div>
                <div class="info-item">
                    <div class="label">金额</div>
                    <div class="value">¥${s.amount}</div>
                </div>
                <div class="info-item">
                    <div class="label">激活日期</div>
                    <div class="value">${s.activate_date || '-'}</div>
                </div>
                <div class="info-item">
                    <div class="label">有效期至</div>
                    <div class="value ${s.expire_date && new Date(s.expire_date) < new Date() ? 'text-danger' : ''}">${s.expire_date || '-'}</div>
                </div>
                <div class="info-item">
                    <div class="label">电话</div>
                    <div class="value">${s.phone || '-'}</div>
                </div>
                <div class="info-item">
                    <div class="label">付款方式</div>
                    <div class="value">${s.payment_method || '-'}</div>
                </div>
            </div>

            ${s.note ? `<div class="card"><div class="card-title">备注</div><div>${s.note}</div></div>` : ''}

            <!-- 操作按钮区（按频率排序） -->
            <div class="section-title" style="margin-top:4px">⚡ 操作</div>
            <div class="student-action-area">
                <button class="student-action-btn action-primary" onclick="showDeductModal('${s.name}')">
                    <span class="action-icon">✋</span>
                    <span>扣课签到</span>
                </button>
                <div class="student-action-row">
                    <button class="student-action-btn action-renew" onclick="showRenewModal('${s.name}')">
                        <span class="action-icon">🔄</span>
                        <span>续费</span>
                    </button>
                    <button class="student-action-btn action-edit" onclick="showEditModal('${s.name}')">
                        <span class="action-icon">✏️</span>
                        <span>修改信息</span>
                    </button>
                </div>
                <div class="student-action-row">
                    <button class="student-action-btn action-refund" onclick="confirmRefund('${s.name}')">
                        <span class="action-icon">💳</span>
                        <span>退卡</span>
                    </button>
                    <button class="student-action-btn action-undo" onclick="showUndoModal('${s.name}')">
                        <span class="action-icon">↩️</span>
                        <span>撤销</span>
                    </button>
                </div>
            </div>

            <!-- 操作历史记录 -->
            <div class="section-title" style="margin-top:8px">📋 操作历史</div>
            <div id="detailTabContent">
                ${renderLogs(s.logs || [])}
            </div>
        </div>
        `;
    } catch(e) {
        main.innerHTML = `<div class="page"><div class="empty-state"><div class="icon">😵</div><div class="text">${e.message}</div></div></div>`;
    }
}

// 退卡确认
function confirmRefund(name) {
    openModal(`
        <div class="modal-header">
            <div class="modal-title">💳 确认退卡</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <div style="text-align:center;padding:20px 0">
            <div style="font-size:48px;margin-bottom:12px">💳</div>
            <div style="font-size:16px;margin-bottom:8px">确认退卡「<strong>${name}</strong>」？</div>
            <div style="font-size:13px;color:var(--text-muted)">退卡后学员状态将变为已退卡</div>
        </div>
        <div class="btn-group">
            <button class="btn btn-outline" onclick="closeModal()">取消</button>
            <button class="btn btn-danger" onclick="doRefund('${name}')">确认退卡</button>
        </div>
    `);
}

async function doRefund(name) {
    try {
        const result = await api(`/students/${encodeURIComponent(name)}/refund`, {
            method: 'POST',
            body: JSON.stringify({}),
        });
        showToast(result.message);
        closeModal();
        renderStudentDetail(name);
    } catch(e) {
        showToast(e.message, 'error');
    }
}

// 显示撤销操作模态框
async function showUndoModal(name) {
    try {
        const logs = await api(`/students/${encodeURIComponent(name)}`);
        const undoableLogs = (logs.logs || []).filter(l => !l.undone);
        
        if (undoableLogs.length === 0) {
            showToast('没有可撤销的操作', 'warning');
            return;
        }

        openModal(`
            <div class="modal-header">
                <div class="modal-title">↩️ 撤销操作</div>
                <button class="modal-close" onclick="closeModal()">✕</button>
            </div>
            <div style="font-size:13px;color:var(--text-muted);margin-bottom:12px">选择要撤销的操作（7天内可撤销）</div>
            ${undoableLogs.map(l => `
                <div class="op-record-item" style="margin-bottom:8px">
                    <div class="op-record-left">
                        <span class="op-type-badge badge-info">${l.type}</span>
                        <span class="op-student-name" style="font-size:13px">${l.detail}</span>
                    </div>
                    <div class="op-record-right">
                        <span class="op-time">${l.time}</span>
                    </div>
                </div>
            `).join('')}
        `);
    } catch(e) {
        showToast(e.message, 'error');
    }
}

// 显示编辑学员信息模态框
function showEditModal(name) {
    if (!currentStudent) return;
    const s = currentStudent;
    
    openModal(`
        <div class="modal-header">
            <div class="modal-title">✏️ 修改信息</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <form onsubmit="submitEdit(event, '${name}')">
            <div class="form-group">
                <label class="form-label">电话</label>
                <input class="form-input" id="editPhone" value="${s.phone || ''}" placeholder="手机号">
            </div>
            <div class="form-group">
                <label class="form-label">微信昵称</label>
                <input class="form-input" id="editWechat" value="${s.wechat || ''}" placeholder="微信昵称">
            </div>
            <div class="form-group">
                <label class="form-label">备注</label>
                <input class="form-input" id="editNote" value="${s.note || ''}" placeholder="备注信息">
            </div>
            <button type="submit" class="btn btn-primary">✅ 保存修改</button>
        </form>
    `);
}

async function submitEdit(e, name) {
    e.preventDefault();
    const phone = document.getElementById('editPhone').value.trim();
    const wechat = document.getElementById('editWechat').value.trim();
    const note = document.getElementById('editNote').value.trim();

    try {
        // 找到当前学员的有效卡记录ID
        if (currentStudent && currentStudent.record_id) {
            await api(`/students/${currentStudent.record_id}/update`, {
                method: 'POST',
                body: JSON.stringify({ phone, wechat, note }),
            });
        }
        showToast('修改成功');
        closeModal();
        renderStudentDetail(name);
    } catch(e) {
        showToast(e.message, 'error');
    }
}

function renderClassRecords(records) {
    if (!records.length) return '<div class="empty-state"><div class="icon">📭</div><div class="text">暂无上课记录</div></div>';
    return records.map(r => `
        <div class="record-item">
            <div class="record-left">
                <div class="record-date">${r.date}</div>
                <div class="record-detail">${r.teacher ? r.teacher + ' · ' : ''}${r.dance_type || ''}</div>
            </div>
            <div class="record-right ${r.deduct_count > 0 ? 'text-danger' : 'text-success'}">
                ${r.deduct_count > 0 ? '-' + r.deduct_count + '次' : '签到'}
            </div>
        </div>
    `).join('');
}

function renderLogs(logs) {
    if (!logs.length) return '<div class="empty-state"><div class="icon">📭</div><div class="text">暂无操作日志</div></div>';
    return logs.map(l => `
        <div class="record-item" ${l.undone ? 'style="opacity:0.5"' : ''}>
            <div class="record-left">
                <div class="record-date">${l.time} · ${l.operator}</div>
                <div class="record-detail">${l.type}${l.undone ? ' (已撤销)' : ''}</div>
            </div>
            <div class="record-right text-muted" style="font-size:12px;max-width:60%">${l.detail}</div>
        </div>
    `).join('');
}

function renderAllCards(cards) {
    if (!cards.length) return '<div class="empty-state"><div class="icon">📭</div><div class="text">暂无卡记录</div></div>';
    return cards.map(c => {
        const statusClass = c.card_status === '有效' ? 'badge-success' :
                           c.card_status === '已过期' ? 'badge-warning' : 'badge-danger';
        return `
        <div class="card">
            <div class="flex-between mb-8">
                <strong>${c.card_name}</strong>
                <span class="badge ${statusClass}">${c.card_status}</span>
            </div>
            <div class="info-grid">
                <div class="info-item"><div class="label">金额</div><div class="value">¥${c.amount}</div></div>
                <div class="info-item"><div class="label">剩余</div><div class="value">${c.remaining_hours}次</div></div>
            </div>
        </div>
        `;
    }).join('');
}

function showDetailTab(tab, el) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    el.classList.add('active');

    const content = document.getElementById('detailTabContent');
    if (!currentStudent) return;

    switch(tab) {
        case 'class': content.innerHTML = renderClassRecords(currentStudent.class_records || []); break;
        case 'log': content.innerHTML = renderLogs(currentStudent.logs || []); break;
        case 'cards': content.innerHTML = renderAllCards(currentStudent.all_cards || []); break;
    }
}


// ── 录入新学员模态框 ──────────────────────
function showRegisterModal() {
    openModal(`
        <div class="modal-header">
            <div class="modal-title">📝 录入新学员</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <form onsubmit="submitRegister(event)">
            <div class="form-group">
                <label class="form-label">姓名 *</label>
                <input class="form-input" id="regName" required placeholder="学员姓名">
            </div>
            <div class="form-group" style="position:relative">
                <label class="form-label">卡种名称 <span style="font-weight:normal;color:#888">(选填)</span></label>
                <div style="display:flex;gap:8px">
                    <input class="form-input" id="regCardName" type="text" placeholder="选填，如：次卡16次" autocomplete="off" oninput="onCardNameInput()" onfocus="onCardNameInput()" onblur="setTimeout(()=>closeCardDropdown(),200)">
                    <button type="button" class="btn btn-outline" style="white-space:nowrap;font-size:13px;padding:8px 12px" onclick="showTemplateSelector()">📋 选模板</button>
                </div>
                <div class="card-dropdown" id="cardDropdown" style="display:none"></div>
            </div>
            <div class="form-group">
                <label class="form-label">课次 *</label>
                <input class="form-input" id="regHours" type="number" min="0" placeholder="购买课次，如16">
            </div>
            <div class="form-group">
                <label class="form-label">金额 *</label>
                <input class="form-input" id="regAmount" type="number" step="0.01" min="0" placeholder="付款金额">
                <div class="form-hint" id="regAmountHint"></div>
            </div>
            <div class="form-group">
                <label class="form-label">付款方式</label>
                <select class="form-select" id="regPayment">
                    <option value="微信">微信</option>
                    <option value="支付宝">支付宝</option>
                    <option value="现金">现金</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">电话</label>
                <input class="form-input" id="regPhone" placeholder="手机号">
            </div>
            <div class="form-group">
                <label class="form-label">微信昵称</label>
                <input class="form-input" id="regWechat" placeholder="微信昵称">
            </div>
            <div class="form-group">
                <label class="form-label">渠道来源</label>
                <select class="form-select" id="regChannel">
                    <option value="">未选择</option>
                    <option value="朋友推荐">朋友推荐</option>
                    <option value="小红书">小红书</option>
                    <option value="抖音">抖音</option>
                    <option value="路过">路过</option>
                    <option value="其他">其他</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">备注</label>
                <input class="form-input" id="regNote" placeholder="备注信息">
            </div>
            <button type="submit" class="btn btn-primary">✅ 确认录入</button>
        </form>
    `);
}

// ── 卡种名称模糊匹配下拉 ──────────────────
function onCardNameInput() {
    const input = document.getElementById('regCardName');
    const dropdown = document.getElementById('cardDropdown');
    if (!input || !dropdown) return;
    
    const val = input.value.trim().toLowerCase();
    
    // 筛选匹配的卡种
    let matches = cardPrices;
    if (val) {
        matches = cardPrices.filter(p => p.name.toLowerCase().includes(val));
    }
    
    if (matches.length === 0) {
        dropdown.style.display = 'none';
        return;
    }
    
    dropdown.innerHTML = matches.map(p => 
        `<div class="card-dropdown-item" onclick="selectCardTemplate('${p.name.replace(/'/g, "\\'")}')">
            <span class="card-dropdown-name">${p.name}</span>
            <span class="card-dropdown-detail">¥${p.price} · ${p.hours}次 · ${p.valid_days}天</span>
        </div>`
    ).join('');
    dropdown.style.display = 'block';
}

function closeCardDropdown() {
    const dropdown = document.getElementById('cardDropdown');
    if (dropdown) dropdown.style.display = 'none';
}

// 选择模板：快速填充课次+金额+卡种名称
function selectCardTemplate(name) {
    const card = cardPrices.find(p => p.name === name);
    if (!card) return;
    
    const nameInput = document.getElementById('regCardName');
    const hoursInput = document.getElementById('regHours');
    const amountInput = document.getElementById('regAmount');
    const hint = document.getElementById('regAmountHint');
    const dropdown = document.getElementById('cardDropdown');
    
    if (nameInput) nameInput.value = card.name;
    if (hoursInput) hoursInput.value = card.hours;
    if (amountInput) amountInput.value = card.price;
    if (hint) hint.textContent = `模板: ${card.name} · ¥${card.price} · ${card.hours}次 · ${card.valid_days}天有效`;
    if (dropdown) dropdown.style.display = 'none';
}

// 模板选择器（弹出完整列表）
function showTemplateSelector() {
    const dropdown = document.getElementById('cardDropdown');
    if (!dropdown) return;
    
    if (cardPrices.length === 0) {
        showToast('暂无卡种模板', 'warning');
        return;
    }
    
    dropdown.innerHTML = cardPrices.map(p => 
        `<div class="card-dropdown-item" onclick="selectCardTemplate('${p.name.replace(/'/g, "\\'")}')">
            <span class="card-dropdown-name">${p.name}</span>
            <span class="card-dropdown-detail">¥${p.price} · ${p.hours}次 · ${p.valid_days}天</span>
        </div>`
    ).join('');
    dropdown.style.display = 'block';
}

async function submitRegister(e) {
    e.preventDefault();
    const name = document.getElementById('regName').value.trim();
    const hours = document.getElementById('regHours').value;
    const amount = document.getElementById('regAmount').value;
    const card_name = document.getElementById('regCardName').value.trim();
    const payment_method = document.getElementById('regPayment').value;
    const phone = document.getElementById('regPhone').value.trim();
    const wechat = document.getElementById('regWechat').value.trim();
    const channel = document.getElementById('regChannel').value;
    const note = document.getElementById('regNote').value.trim();

    if (!name) { showToast('请输入姓名', 'error'); return; }
    
    // 课次和金额允许为0，但不能同时为0
    const hoursVal = hours ? parseFloat(hours) : 0;
    const amountVal = amount ? parseFloat(amount) : 0;
    if (hoursVal === 0 && amountVal === 0) {
        showToast('课次和金额不能同时为0，请至少填写一项', 'error'); return;
    }

    try {
        const result = await api('/students', {
            method: 'POST',
            body: JSON.stringify({
                name, card_name, hours: hours ? parseFloat(hours) : null, amount: amount ? parseFloat(amount) : null,
                payment_method, phone, wechat, channel, note,
            }),
        });
        showToast(`录入成功！会员号: ${result.member_id}`);
        closeModal();
        if (currentPage === 'students') loadStudents();
    } catch(e) {
        showToast(e.message, 'error');
    }
}

// ── 扣课模态框 ────────────────────────────
function showDeductModal(name) {
    openModal(`
        <div class="modal-header">
            <div class="modal-title">✋ 扣课签到</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <form onsubmit="submitDeduct(event)">
            <div class="form-group">
                <label class="form-label">学员姓名 *</label>
                ${name ? 
                    `<input class="form-input" id="deductName" value="${name}" readonly>` :
                    `<input class="form-input" id="deductName" required placeholder="输入姓名搜索">`
                }
            </div>
            <div class="form-group">
                <label class="form-label">扣课次数</label>
                <select class="form-select" id="deductCount">
                    <option value="1">1 次</option>
                    <option value="2">2 次</option>
                    <option value="3">3 次</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">老师</label>
                <input class="form-input" id="deductTeacher" placeholder="授课老师">
            </div>
            <div class="form-group">
                <label class="form-label">舞种</label>
                <select class="form-select" id="deductDance">
                    <option value="">未选择</option>
                    <option value="HipHop">HipHop</option>
                    <option value="Breaking">Breaking</option>
                    <option value="Popping">Popping</option>
                    <option value="Locking">Locking</option>
                    <option value="Jazz">Jazz</option>
                    <option value="Waacking">Waacking</option>
                    <option value="Urban">Urban</option>
                    <option value="K-pop">K-pop</option>
                    <option value="其他">其他</option>
                </select>
            </div>
            <button type="submit" class="btn btn-primary">✅ 确认扣课</button>
        </form>
    `);
}

async function submitDeduct(e) {
    e.preventDefault();
    const name = document.getElementById('deductName').value.trim();
    const deduct_count = parseInt(document.getElementById('deductCount').value);
    const teacher = document.getElementById('deductTeacher').value.trim();
    const dance_type = document.getElementById('deductDance').value;

    if (!name) { showToast('请输入姓名', 'error'); return; }

    try {
        const result = await api('/deduct', {
            method: 'POST',
            body: JSON.stringify({ name, deduct_count, teacher, dance_type }),
        });
        showToast(result.message + (result.warning ? ' ⚠️' + result.warning : ''));
        closeModal();
        if (currentPage === 'student-detail') renderStudentDetail(name);
        if (currentPage === 'dashboard') renderDashboard();
    } catch(e) {
        showToast(e.message, 'error');
    }
}

// ── 续费模态框 ────────────────────────────
function showRenewModal(name) {
    const priceOptions = cardPrices.map(p => 
        `<option value="${p.name}" data-price="${p.price}" data-hours="${p.hours}" data-days="${p.valid_days}">${p.name} - ¥${p.price} (${p.hours}次/${p.valid_days}天)</option>`
    ).join('');

    openModal(`
        <div class="modal-header">
            <div class="modal-title">🔄 续费</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <form onsubmit="submitRenew(event)">
            <div class="form-group">
                <label class="form-label">学员姓名 *</label>
                ${name ? 
                    `<input class="form-input" id="renewName" value="${name}" readonly>` :
                    `<input class="form-input" id="renewName" required placeholder="输入姓名搜索">`
                }
            </div>
            <div class="form-group">
                <label class="form-label">续费卡种 *</label>
                <select class="form-select" id="renewCard" onchange="onRenewCardSelect()" required>
                    <option value="">请选择</option>
                    ${priceOptions}
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">金额</label>
                <input class="form-input" id="renewAmount" type="number" step="0.01" placeholder="付款金额">
                <div class="form-hint" id="renewAmountHint"></div>
            </div>
            <div class="form-group">
                <label class="form-label">付款方式</label>
                <select class="form-select" id="renewPayment">
                    <option value="微信">微信</option>
                    <option value="支付宝">支付宝</option>
                    <option value="现金">现金</option>
                </select>
            </div>
            <button type="submit" class="btn btn-success">✅ 确认续费</button>
        </form>
    `);
}

function onRenewCardSelect() {
    const sel = document.getElementById('renewCard');
    const opt = sel.options[sel.selectedIndex];
    const amountInput = document.getElementById('renewAmount');
    const hint = document.getElementById('renewAmountHint');

    if (opt.value) {
        amountInput.value = opt.dataset.price;
        hint.textContent = `标准价 ¥${opt.dataset.price} · +${opt.dataset.hours}次 · ${opt.dataset.days}天有效`;
    } else {
        hint.textContent = '';
    }
}

async function submitRenew(e) {
    e.preventDefault();
    const name = document.getElementById('renewName').value.trim();
    const card_name = document.getElementById('renewCard').value;
    const amount = document.getElementById('renewAmount').value;
    const payment_method = document.getElementById('renewPayment').value;

    if (!name) { showToast('请输入姓名', 'error'); return; }
    if (!card_name) { showToast('请选择卡种', 'error'); return; }

    try {
        const result = await api('/renew', {
            method: 'POST',
            body: JSON.stringify({
                name, card_name, amount: amount ? parseFloat(amount) : null, payment_method,
            }),
        });
        showToast(result.message);
        closeModal();
        if (currentPage === 'student-detail') renderStudentDetail(name);
        if (currentPage === 'dashboard') renderDashboard();
    } catch(e) {
        showToast(e.message, 'error');
    }
}

// ── 设置页面（新3tab版） ──────────────────
// ── 数据分析页面 ──────────────────────────────────
// ── 数据分析页面（4区版：营收总览 + 老师&课程 + 学员健康度 + 卡类分析） ──
async function renderAnalytics() {
    const main = document.getElementById('mainContent');
    main.innerHTML = `<div class="page"><div class="loading"><div class="spinner"></div>加载中...</div></div>`;

    try {
        const apiKey = getApiKey();
        const headers = {};
        if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;
        const resp = await fetch(`${API_BASE}/analytics`, { headers });
        if (!resp.ok) {
            const errText = await resp.text();
            main.innerHTML = `<div class="page"><div class="empty-state"><div class="icon">😵</div><div class="text">请求失败 (${resp.status})</div><div class="sub">${errText}</div></div></div>`;
            return;
        }
        const data = await resp.json();

        const monthly = data.monthly_card || {};
        const yearly = data.yearly_card || {};
        const yoy = data.yoy || {};
        const mom = data.mom || {};
        const yearlyClassRev = data.yearly_class_revenue || {};
        const teacherStats = data.teacher_stats || [];
        const danceStats = data.dance_stats || [];
        const monthlyClassCount = data.monthly_class_count || {};
        const studentHealth = data.student_health || {};
        const cardTypeStats = data.card_type_stats || {};

        const thisYear = new Date().getFullYear().toString();
        const lastYear = (new Date().getFullYear() - 1).toString();

        // 最近12个月
        const allMonths = Object.keys(monthly).sort();
        const last12 = allMonths.slice(-12);
        const maxAmount = Math.max(...last12.map(m => monthly[m]?.amount || 0), 1);

        // 课消次数柱状图
        const classMonths = Object.keys(monthlyClassCount).sort();
        const last12Class = classMonths.slice(-12);
        const maxClassCount = Math.max(...last12Class.map(m => monthlyClassCount[m] || 0), 1);

        // 同比计算
        const thisYearRev = yearlyClassRev[thisYear] || 0;
        const lastYearRev = yearlyClassRev[lastYear] || 0;
        const revYoy = lastYearRev > 0 ? ((thisYearRev - lastYearRev) / lastYearRev * 100).toFixed(1) : null;
        const thisYearCard = yearly[thisYear]?.amount || 0;
        const lastYearCard = yearly[lastYear]?.amount || 0;
        const cardYoy = lastYearCard > 0 ? ((thisYearCard - lastYearCard) / lastYearCard * 100).toFixed(1) : null;

        main.innerHTML = `
        <div class="page">
            <div class="page-header">
                <h1>📊 数据分析</h1>
            </div>

            <!-- ① 营收总览 -->
            <div class="section-title">💰 营收总览</div>
            <div class="stats-grid" style="grid-template-columns: 1fr 1fr">
                <div class="overview-card">
                    <div class="overview-label">${thisYear}年课消</div>
                    <div class="overview-value text-success">¥${thisYearRev.toLocaleString()}</div>
                    ${revYoy != null ? `<div class="overview-sub"><span class="tag ${revYoy >= 0 ? 'tag-success' : 'tag-danger'}">${revYoy >= 0 ? '↑' : '↓'}${Math.abs(revYoy)}%</span> 同比</div>` : ''}
                </div>
                <div class="overview-card">
                    <div class="overview-label">${lastYear}年课消</div>
                    <div class="overview-value">¥${lastYearRev.toLocaleString()}</div>
                </div>
                <div class="overview-card">
                    <div class="overview-label">${thisYear}年报卡</div>
                    <div class="overview-value text-success">¥${thisYearCard.toLocaleString()}</div>
                    ${cardYoy != null ? `<div class="overview-sub"><span class="tag ${cardYoy >= 0 ? 'tag-success' : 'tag-danger'}">${cardYoy >= 0 ? '↑' : '↓'}${Math.abs(cardYoy)}%</span> 同比</div>` : ''}
                </div>
                <div class="overview-card">
                    <div class="overview-label">${lastYear}年报卡</div>
                    <div class="overview-value">¥${lastYearCard.toLocaleString()}</div>
                </div>
            </div>

            <!-- 月度课消金额柱状图 -->
            <div class="section-title" style="margin-top:16px">月度课消金额</div>
            <div class="bar-chart">
                ${last12.map(m => {
                    const v = monthly[m]?.amount || 0;
                    const pct = Math.max((v / maxAmount) * 100, 2);
                    const momVal = mom[m];
                    const momTag = momVal != null ? `<span class="tag ${momVal >= 0 ? 'tag-success' : 'tag-danger'}">${momVal >= 0 ? '+' : ''}${momVal}%</span>` : '';
                    return `
                    <div class="bar-item">
                        <div class="bar-value">¥${(v/1000).toFixed(v>=1000?0:1)}${v>=1000?'k':''}</div>
                        <div class="bar" style="height:${pct}%"></div>
                        <div class="bar-label">${m.slice(5)}</div>
                        <div class="bar-tags">${momTag}</div>
                    </div>`;
                }).join('')}
            </div>

            <!-- 月度课消次数 -->
            ${last12Class.length > 0 ? `
            <div class="section-title" style="margin-top:16px">月度课消次数</div>
            <div class="bar-chart">
                ${last12Class.map(m => {
                    const v = monthlyClassCount[m] || 0;
                    const pct = Math.max((v / maxClassCount) * 100, 2);
                    return `
                    <div class="bar-item">
                        <div class="bar-value">${v}</div>
                        <div class="bar" style="height:${pct}%"></div>
                        <div class="bar-label">${m.slice(5)}</div>
                    </div>`;
                }).join('')}
            </div>
            ` : ''}

            <!-- ② 老师&课程分析 -->
            ${teacherStats.length > 0 ? `
            <div class="section-title" style="margin-top:16px">👨‍🏫 老师&课程分析</div>
            <div class="detail-card-list">
                <div class="sub-title">老师开课统计</div>
                ${teacherStats.map(t => `
                    <div class="detail-card">
                        <div class="detail-card-header">
                            <span class="detail-card-name">${t.name}</span>
                            <span class="detail-card-value">${t.count}次</span>
                        </div>
                        <div class="progress-bar"><div class="progress-fill" style="width:${t.pct}%"></div></div>
                        <div class="detail-card-meta">
                            <span>占比 ${t.pct}%</span>
                            <span>课消 ¥${(t.revenue || 0).toLocaleString()}</span>
                            <span>${(t.dances || []).join(' / ')}</span>
                        </div>
                    </div>
                `).join('')}
            </div>
            ` : ''}

            ${danceStats.length > 0 ? `
            <div class="detail-card-list" style="margin-top:12px">
                <div class="sub-title">舞种热度</div>
                ${danceStats.map(d => `
                    <div class="detail-card">
                        <div class="detail-card-header">
                            <span class="detail-card-name">💃 ${d.name}</span>
                            <span class="detail-card-value">${d.count}次</span>
                        </div>
                        <div class="progress-bar"><div class="progress-fill" style="width:${d.pct}%"></div></div>
                        <div class="detail-card-meta">
                            <span>占比 ${d.pct}%</span>
                            ${d.trend ? `<span class="${d.trend === '↑' ? 'text-success' : 'text-danger'}">${d.trend}</span>` : ''}
                        </div>
                    </div>
                `).join('')}
            </div>
            ` : ''}

            <!-- ③ 学员健康度 -->
            <div class="section-title" style="margin-top:16px">💚 学员健康度</div>
            <div class="stats-grid" style="grid-template-columns: 1fr 1fr">
                <div class="overview-card">
                    <div class="overview-label">活跃学员</div>
                    <div class="overview-value text-success">${studentHealth.active_count || 0}</div>
                    <div class="overview-sub">30天内有课消</div>
                </div>
                <div class="overview-card">
                    <div class="overview-label">沉默学员</div>
                    <div class="overview-value" style="color:var(--warning,orange)">${studentHealth.silent_count || 0}</div>
                    <div class="overview-sub">30天无课消</div>
                </div>
                <div class="overview-card">
                    <div class="overview-label">本月新增</div>
                    <div class="overview-value text-accent">${studentHealth.new_this_month || 0}</div>
                </div>
                <div class="overview-card">
                    <div class="overview-label">流失学员</div>
                    <div class="overview-value" style="color:var(--danger,red)">${studentHealth.lost_count || 0}</div>
                    <div class="overview-sub">到期未续费</div>
                </div>
            </div>
            <div class="detail-card-list" style="margin-top:8px">
                <div class="detail-card">
                    <div class="detail-card-header">
                        <span>平均每学员月上课</span>
                        <span class="detail-card-value">${(studentHealth.avg_monthly_classes || 0).toFixed(1)}次</span>
                    </div>
                </div>
                ${(studentHealth.renew_trend || []).length > 0 ? `
                <div class="detail-card">
                    <div class="detail-card-header">
                        <span>续费率趋势</span>
                    </div>
                    <div class="detail-card-meta">
                        ${studentHealth.renew_trend.map(r => `<span>${r.month}: ${r.rate}%</span>`).join(' → ')}
                    </div>
                </div>
                ` : ''}
            </div>

            <!-- ④ 卡类分析 -->
            <div class="section-title" style="margin-top:16px">💳 卡类分析</div>
            ${(cardTypeStats.distribution || []).length > 0 ? `
            <div class="detail-card-list">
                <div class="sub-title">卡类型分布</div>
                ${cardTypeStats.distribution.map(c => `
                    <div class="detail-card">
                        <div class="detail-card-header">
                            <span class="detail-card-name">${c.type}</span>
                            <span class="detail-card-value">${c.count}人 · ${c.pct}%</span>
                        </div>
                        <div class="progress-bar"><div class="progress-fill" style="width:${c.pct}%"></div></div>
                    </div>
                `).join('')}
            </div>
            ` : ''}
            ${(cardTypeStats.class_contribution || []).length > 0 ? `
            <div class="detail-card-list" style="margin-top:12px">
                <div class="sub-title">课消贡献</div>
                ${cardTypeStats.class_contribution.map(c => `
                    <div class="detail-card">
                        <div class="detail-card-header">
                            <span class="detail-card-name">${c.type}</span>
                            <span class="detail-card-value">${c.count}次 · ${c.pct}%</span>
                        </div>
                        <div class="progress-bar"><div class="progress-fill" style="width:${c.pct}%"></div></div>
                    </div>
                `).join('')}
            </div>
            ` : ''}
            <div class="detail-card-list" style="margin-top:8px">
                <div class="detail-card">
                    <div class="detail-card-header">
                        <span>⚠️ 即将过期（7天内）</span>
                        <span class="detail-card-value" style="color:var(--warning,orange)">${cardTypeStats.expiring_soon || 0}张</span>
                    </div>
                </div>
            </div>
        </div>
        `;
    } catch(e) {
        main.innerHTML = `<div class="page"><div class="empty-state"><div class="icon">😵</div><div class="text">加载失败: ${e.message}</div></div></div>`;
    }
}
async function renderSettings() {
    const main = document.getElementById('mainContent');
    main.innerHTML = `<div class="page"><div class="loading"><div class="spinner"></div>加载中...</div></div>`;

    try {
        const [_, teachersData] = await Promise.all([
            loadCardPrices(),
            api('/teachers').catch(() => ({ teachers: [] })),
        ]);
        const teachers = teachersData.teachers || [];
        _teachersCache = teachers; // 缓存供编辑弹窗使用
        
        // 读取过期阈值设置
        const expireThreshold = localStorage.getItem('7l_expire_threshold') || '7';
        const savedApiKey = getApiKey();
        
        main.innerHTML = `
        <div class="page">
            <div class="section-title">⚙️ 设置</div>

            <!-- 卡种模板管理（快捷模板，非强制规则） -->
            <div class="section-title" style="font-size:15px">
                💳 卡种模板管理
                <button class="btn btn-sm btn-primary" style="font-size:12px;padding:6px 12px;width:auto" onclick="showCardPriceModal()">+ 新增卡种</button>
            </div>
            <div id="cardPriceList">
                ${cardPrices.length > 0 ? cardPrices.map(p => `
                    <div class="card-price-item">
                        <div class="card-price-info">
                            <div class="card-price-name">
                                ${p.name}
                                <span class="badge badge-accent" style="margin-left:6px">${p.card_type}</span>
                            </div>
                            <div class="card-price-detail">
                                ¥${p.price} · ${p.hours}次 · ${p.valid_days}天有效
                                ${p.note ? ' · ' + p.note : ''}
                            </div>
                        </div>
                        <div class="card-price-actions">
                            <button class="btn-icon btn-icon-edit" onclick="showCardPriceModal('${p.record_id}')" title="编辑">✏️</button>
                            <button class="btn-icon btn-icon-delete" onclick="confirmDeleteCardPrice('${p.record_id}', '${p.name}')" title="删除">🗑️</button>
                        </div>
                    </div>
                `).join('') : '<div class="card"><div class="text-muted">暂无卡种定价，点击右上角新增</div></div>'}
            </div>

            <!-- 老师管理 -->
            <div class="section-title" style="font-size:15px;margin-top:20px">
                👨‍🏫 老师管理
                <button class="btn btn-sm btn-primary" style="font-size:12px;padding:6px 12px;width:auto" onclick="showTeacherModal()">+ 新增老师</button>
            </div>
            <div id="teacherList">
                ${teachers.length > 0 ? teachers.map(t => `
                    <div class="card-price-item">
                        <div class="card-price-info">
                            <div class="card-price-name">
                                ${t.name}
                                <span class="badge" style="margin-left:6px;background:${t.status === '在教' ? '#10b981' : '#9ca3af'};color:#fff;font-size:11px;padding:2px 8px;border-radius:10px">${t.status}</span>
                            </div>
                            <div class="card-price-detail">
                                ${(t.dances || []).map(d => '<span style="display:inline-block;background:var(--bg-input);border:1px solid var(--border);border-radius:10px;padding:1px 8px;font-size:11px;margin-right:4px">' + d + '</span>').join('')}
                                ${t.phone ? ' · 📱' + t.phone : ''}
                                ${t.join_date ? ' · 📅' + t.join_date : ''}
                                ${t.note ? ' · ' + t.note : ''}
                            </div>
                        </div>
                        <div class="card-price-actions">
                            <button class="btn-icon btn-icon-edit" onclick="showTeacherModal('${t.record_id}')" title="编辑">✏️</button>
                            <button class="btn-icon" onclick="toggleTeacherStatus('${t.record_id}', '${t.status}')" title="${t.status === '在教' ? '停用' : '启用'}" style="font-size:16px">${t.status === '在教' ? '⏸️' : '▶️'}</button>
                        </div>
                    </div>
                `).join('') : '<div class="card"><div class="text-muted">暂无老师，点击右上角新增</div></div>'}
            </div>

            <!-- 即将过期阈值设置 -->
            <div class="section-title" style="font-size:15px;margin-top:20px">⏰ 提醒设置</div>
            <div class="card">
                <div class="flex-between">
                    <div>
                        <div style="font-weight:600">即将过期阈值</div>
                        <div class="text-muted" style="font-size:12px;margin-top:2px">学员有效期在此天数内将标为即将过期</div>
                    </div>
                    <div style="display:flex;align-items:center;gap:8px">
                        <input type="number" id="expireThresholdInput" value="${expireThreshold}" min="1" max="90" 
                            style="width:60px;background:var(--bg-input);border:1px solid var(--border);border-radius:var(--radius-sm);padding:8px 10px;color:var(--text-primary);font-size:14px;text-align:center"
                            onchange="saveExpireThreshold(this.value)">
                        <span class="text-muted" style="font-size:13px">天</span>
                    </div>
                </div>
            </div>

            <!-- 系统设置 -->
            <div class="section-title" style="font-size:15px;margin-top:20px">🔧 系统设置</div>
            <div class="card">
                <div class="form-group" style="margin-bottom:0">
                    <label class="form-label">API Key</label>
                    <div style="display:flex;gap:8px;align-items:center">
                        <input class="form-input" id="apiKeyInput" type="password" value="${savedApiKey}" placeholder="输入API Key" style="flex:1">
                        <button class="btn btn-sm btn-primary" style="width:auto;padding:8px 16px" onclick="saveApiKey()">保存</button>
                    </div>
                    <div class="form-hint">用于鉴权访问后端API</div>
                </div>
            </div>

            <!-- ⚠️ 危险操作 -->
            <div class="section-title" style="font-size:15px;margin-top:20px;color:var(--danger,red)">⚠️ 危险操作</div>
            <div class="card" style="border:1px solid var(--danger,red);background:var(--bg-card)">
                <div style="font-size:13px;color:var(--text-muted);margin-bottom:12px">以下操作不可恢复，请谨慎使用</div>
                <div style="display:flex;flex-wrap:wrap;gap:8px">
                    <button class="btn btn-sm" style="background:#fee2e2;color:#dc2626;border:1px solid #fecaca;padding:6px 14px;font-size:12px" onclick="confirmClearData('class_records','课消记录')">🗑️ 清空课消记录</button>
                    <button class="btn btn-sm" style="background:#fee2e2;color:#dc2626;border:1px solid #fecaca;padding:6px 14px;font-size:12px" onclick="confirmClearData('teachers','老师数据')">🗑️ 清空老师数据</button>
                    <button class="btn btn-sm" style="background:#fee2e2;color:#dc2626;border:1px solid #fecaca;padding:6px 14px;font-size:12px" onclick="confirmClearData('students','学员数据')">🗑️ 清空学员数据</button>
                    <button class="btn btn-sm" style="background:#fee2e2;color:#dc2626;border:1px solid #fecaca;padding:6px 14px;font-size:12px" onclick="confirmClearData('logs','操作日志')">🗑️ 清空操作日志</button>
                </div>
            </div>
        </div>
        `;
    } catch(e) {
        main.innerHTML = `<div class="page"><div class="empty-state"><div class="icon">😵</div><div class="text">加载失败: ${e.message}</div></div></div>`;
    }
}

// 保存过期阈值
function saveExpireThreshold(value) {
    const v = parseInt(value);
    if (v >= 1 && v <= 90) {
        localStorage.setItem('7l_expire_threshold', v.toString());
        showToast(`过期阈值已设为 ${v} 天`);
    } else {
        showToast('请输入1-90之间的数字', 'error');
    }
}

// 保存API Key
function saveApiKey() {
    const key = document.getElementById('apiKeyInput')?.value?.trim() || '';
    if (key) {
        localStorage.setItem('7l_api_key', key);
        showToast('API Key 已保存');
    } else {
        localStorage.removeItem('7l_api_key');
        showToast('API Key 已清除');
    }
}

// ── 卡种定价 新增/编辑模态框 ────────────────
function showCardPriceModal(recordId) {
    const isEdit = !!recordId;
    let price = { name: '', card_type: '次卡', price: 0, hours: 0, valid_days: 30, note: '' };

    if (isEdit) {
        price = cardPrices.find(p => p.record_id === recordId) || price;
    }

    const cardTypeOptions = ['次卡', '月卡', '期卡', '体验卡'].map(t =>
        `<option value="${t}" ${price.card_type === t ? 'selected' : ''}>${t}</option>`
    ).join('');

    openModal(`
        <div class="modal-header">
            <div class="modal-title">${isEdit ? '✏️ 编辑卡种' : '➕ 新增卡种'}</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <form onsubmit="submitCardPrice(event, '${recordId || ''}')">
            <div class="form-group">
                <label class="form-label">卡种名称 *</label>
                <input class="form-input" id="cpName" value="${price.name}" required placeholder="如：次卡·16次">
            </div>
            <div class="form-group">
                <label class="form-label">卡类型</label>
                <select class="form-select" id="cpCardType">
                    ${cardTypeOptions}
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">金额 (元)</label>
                <input class="form-input" id="cpPrice" type="number" step="0.01" min="0" value="${price.price}" placeholder="0">
            </div>
            <div class="form-group">
                <label class="form-label">课时数</label>
                <input class="form-input" id="cpHours" type="number" min="0" value="${price.hours}" placeholder="0">
            </div>
            <div class="form-group">
                <label class="form-label">有效期 (天)</label>
                <input class="form-input" id="cpValidDays" type="number" min="1" value="${price.valid_days}" placeholder="30">
            </div>
            <div class="form-group">
                <label class="form-label">说明</label>
                <input class="form-input" id="cpNote" value="${price.note || ''}" placeholder="可选说明">
            </div>
            <button type="submit" class="btn btn-primary">${isEdit ? '✅ 保存修改' : '✅ 确认新增'}</button>
        </form>
    `);
}

async function submitCardPrice(e, recordId) {
    e.preventDefault();
    const name = document.getElementById('cpName').value.trim();
    const card_type = document.getElementById('cpCardType').value;
    const price = parseFloat(document.getElementById('cpPrice').value) || 0;
    const hours = parseInt(document.getElementById('cpHours').value) || 0;
    const valid_days = parseInt(document.getElementById('cpValidDays').value) || 30;
    const note = document.getElementById('cpNote').value.trim();

    if (!name) { showToast('请输入卡种名称', 'error'); return; }

    try {
        if (recordId) {
            // 编辑
            await api(`/card-prices/${recordId}`, {
                method: 'PUT',
                body: JSON.stringify({ name, card_type, price, hours, valid_days, note }),
            });
            showToast('修改成功');
        } else {
            // 新增
            await api('/card-prices', {
                method: 'POST',
                body: JSON.stringify({ name, card_type, price, hours, valid_days, note }),
            });
            showToast('新增成功');
        }
        closeModal();
        await loadCardPrices();
        renderSettings();
    } catch(err) {
        showToast(err.message, 'error');
    }
}

// ── 卡种定价 删除确认 ──────────────────────
function confirmDeleteCardPrice(recordId, name) {
    openModal(`
        <div class="modal-header">
            <div class="modal-title">⚠️ 确认删除</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <div style="text-align:center;padding:20px 0">
            <div style="font-size:48px;margin-bottom:12px">🗑️</div>
            <div style="font-size:16px;margin-bottom:8px">确定删除卡种「<strong>${name}</strong>」？</div>
            <div style="font-size:13px;color:var(--text-muted)">删除后无法恢复，已有学员的卡种名称不受影响</div>
        </div>
        <div class="btn-group">
            <button class="btn btn-outline" onclick="closeModal()">取消</button>
            <button class="btn btn-danger" onclick="deleteCardPrice('${recordId}')">确认删除</button>
        </div>
    `);
}

async function deleteCardPrice(recordId) {
    try {
        await api(`/card-prices/${recordId}`, { method: 'DELETE' });
        showToast('删除成功');
        closeModal();
        await loadCardPrices();
        renderSettings();
    } catch(err) {
        showToast(err.message, 'error');
    }
}

// ── 老师管理 新增/编辑模态框 ────────────────
const DANCE_OPTIONS = ['HipHop', 'Jazz', 'Breaking', 'Popping', 'Urban', 'K-pop', '其他'];
let _teachersCache = []; // 缓存老师列表供编辑用

function showTeacherModal(recordId) {
    const isEdit = !!recordId;
    let teacher = { name: '', dances: [], phone: '', join_date: '', status: '在教', note: '' };

    if (isEdit) {
        // 从当前渲染的列表中查找
        const items = document.querySelectorAll('#teacherList .card-price-item');
        // 简单方式：重新请求
        teacher = _teachersCache.find(t => t.record_id === recordId) || teacher;
    }

    const danceCheckboxes = DANCE_OPTIONS.map(d =>
        `<label style="display:inline-flex;align-items:center;gap:4px;margin-right:12px;font-size:13px;cursor:pointer">
            <input type="checkbox" class="teacher-dance-cb" value="${d}" ${(teacher.dances || []).includes(d) ? 'checked' : ''} style="width:16px;height:16px">
            ${d}
        </label>`
    ).join('');

    openModal(`
        <div class="modal-header">
            <div class="modal-title">${isEdit ? '✏️ 编辑老师' : '➕ 新增老师'}</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <form onsubmit="submitTeacher(event, '${recordId || ''}')">
            <div class="form-group">
                <label class="form-label">姓名 *</label>
                <input class="form-input" id="tName" value="${teacher.name}" required placeholder="老师姓名">
            </div>
            <div class="form-group">
                <label class="form-label">舞种</label>
                <div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:4px">
                    ${danceCheckboxes}
                </div>
            </div>
            <div class="form-group">
                <label class="form-label">手机号</label>
                <input class="form-input" id="tPhone" value="${teacher.phone}" placeholder="手机号">
            </div>
            <div class="form-group">
                <label class="form-label">入职日期</label>
                <input class="form-input" id="tJoinDate" type="date" value="${teacher.join_date}">
            </div>
            <div class="form-group">
                <label class="form-label">状态</label>
                <select class="form-select" id="tStatus">
                    <option value="在教" ${teacher.status === '在教' ? 'selected' : ''}>在教</option>
                    <option value="停用" ${teacher.status === '停用' ? 'selected' : ''}>停用</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">备注</label>
                <input class="form-input" id="tNote" value="${teacher.note || ''}" placeholder="可选备注">
            </div>
            <button type="submit" class="btn btn-primary">${isEdit ? '✅ 保存修改' : '✅ 确认新增'}</button>
        </form>
    `);
}

async function submitTeacher(e, recordId) {
    e.preventDefault();
    const name = document.getElementById('tName').value.trim();
    const dances = Array.from(document.querySelectorAll('.teacher-dance-cb:checked')).map(cb => cb.value);
    const phone = document.getElementById('tPhone').value.trim();
    const join_date = document.getElementById('tJoinDate').value;
    const status = document.getElementById('tStatus').value;
    const note = document.getElementById('tNote').value.trim();

    if (!name) { showToast('请输入老师姓名', 'error'); return; }

    try {
        const body = { name, dances, phone, join_date: join_date || null, status, note };
        if (recordId) {
            await api(`/teachers/${recordId}`, {
                method: 'PUT',
                body: JSON.stringify(body),
            });
            showToast('修改成功');
        } else {
            await api('/teachers', {
                method: 'POST',
                body: JSON.stringify(body),
            });
            showToast('新增成功');
        }
        closeModal();
        renderSettings();
    } catch(err) {
        showToast(err.message, 'error');
    }
}

async function toggleTeacherStatus(recordId, currentStatus) {
    const newStatus = currentStatus === '在教' ? '停用' : '在教';
    const action = newStatus === '停用' ? '停用' : '启用';
    try {
        await api(`/teachers/${recordId}`, {
            method: 'PUT',
            body: JSON.stringify({ status: newStatus }),
        });
        showToast(`已${action}`);
        renderSettings();
    } catch(err) {
        showToast(err.message, 'error');
    }
}

// ── 数据清空 ──────────────────────────
function confirmClearData(dataType, label) {
    openModal(`
        <div class="modal-header">
            <div class="modal-title" style="color:var(--danger,red)">⚠️ 确认清空${label}</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <div style="padding:16px">
            <p style="font-size:14px;margin-bottom:12px">此操作将<strong>永久删除</strong>所有${label}，不可恢复！</p>
            <p style="font-size:13px;color:var(--text-muted);margin-bottom:16px">请输入 <strong style="color:var(--danger,red)">确认清空</strong> 4个字以继续：</p>
            <input class="form-input" id="clearConfirmInput" placeholder="输入“确认清空”" style="margin-bottom:16px">
            <div style="display:flex;gap:8px;justify-content:flex-end">
                <button class="btn" onclick="closeModal()">取消</button>
                <button class="btn" id="clearConfirmBtn" style="background:#dc2626;color:#fff;opacity:0.5;cursor:not-allowed" disabled onclick="executeClearData('${dataType}','${label}')">确认清空</button>
            </div>
        </div>
    `);
    // 监听输入，只有输入"确认清空"才能点按钮
    setTimeout(() => {
        const input = document.getElementById('clearConfirmInput');
        const btn = document.getElementById('clearConfirmBtn');
        if (input && btn) {
            input.addEventListener('input', () => {
                if (input.value === '确认清空') {
                    btn.disabled = false;
                    btn.style.opacity = '1';
                    btn.style.cursor = 'pointer';
                } else {
                    btn.disabled = true;
                    btn.style.opacity = '0.5';
                    btn.style.cursor = 'not-allowed';
                }
            });
        }
    }, 100);
}

async function executeClearData(dataType, label) {
    try {
        const result = await api(`/clear-data/${dataType}`, { method: 'POST' });
        showToast(`已清空${label}：${result.deleted_count || 0}条`);
        closeModal();
        renderSettings();
    } catch(err) {
        showToast(`清空失败: ${err.message}`, 'error');
    }
}

// ── OCR 截图识别 ──────────────────────────

// OCR 状态
let ocrStudents = []; // 识别出的学员列表
let ocrRawText = [];  // 原始识别文本

// 图片压缩工具函数
function compressImage(file, maxWidth = 1280, quality = 0.8) {
    return new Promise((resolve) => {
        const reader = new FileReader();
        reader.onload = (e) => {
            const img = new Image();
            img.onload = () => {
                // 如果图片已经够小，不压缩
                if (img.width <= maxWidth && file.size < 2 * 1024 * 1024) {
                    resolve(file);
                    return;
                }
                const canvas = document.createElement('canvas');
                let w = img.width;
                let h = img.height;
                if (w > maxWidth) {
                    h = Math.round(h * maxWidth / w);
                    w = maxWidth;
                }
                canvas.width = w;
                canvas.height = h;
                const ctx = canvas.getContext('2d');
                ctx.drawImage(img, 0, 0, w, h);
                canvas.toBlob((blob) => {
                    resolve(new File([blob], file.name, { type: 'image/jpeg' }));
                }, 'image/jpeg', quality);
            };
            img.src = e.target.result;
        };
        reader.readAsDataURL(file);
    });
}

// 显示OCR模态框
function showOCRModal() {
    ocrStudents = [];
    ocrRawText = [];

    openModal(`
        <div class="modal-header">
            <div class="modal-title">📸 截图识别</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <div id="ocrContent">
            <div class="ocr-upload-area" id="ocrUploadArea">
                <div class="ocr-upload-icon">📸</div>
                <div class="ocr-upload-title">上传接龙截图</div>
                <div class="ocr-upload-desc">支持微信接龙截图、转账截图</div>
                <div class="ocr-btn-group">
                    <button class="btn btn-primary ocr-btn-camera" onclick="ocrTakePhoto()">
                        📷 拍照
                    </button>
                    <button class="btn btn-outline ocr-btn-album" onclick="ocrChooseFromAlbum()">
                        🖼️ 从相册选择
                    </button>
                </div>
                <div class="ocr-paste-hint">💡 电脑用户可 Ctrl+V 粘贴截图</div>
            </div>
        </div>
    `);

    // 监听粘贴事件
    document.addEventListener('paste', handleOCRPaste);
}

// 关闭OCR时清理
const _originalCloseModal = closeModal;
closeModal = function() {
    document.removeEventListener('paste', handleOCRPaste);
    _originalCloseModal();
};

// 拍照
function ocrTakePhoto() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/*';
    input.capture = 'environment'; // 后置摄像头
    input.onchange = (e) => ocrHandleFile(e.target.files[0]);
    input.click();
}

// 从相册选择
function ocrChooseFromAlbum() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/*';
    input.onchange = (e) => ocrHandleFile(e.target.files[0]);
    input.click();
}

// 粘贴处理
function handleOCRPaste(e) {
    const items = e.clipboardData?.items;
    if (!items) return;
    for (const item of items) {
        if (item.type.startsWith('image/')) {
            e.preventDefault();
            const file = item.getAsFile();
            ocrHandleFile(file);
            return;
        }
    }
}

// 处理选中的图片文件
async function ocrHandleFile(file) {
    if (!file) return;

    // 显示加载状态
    const content = document.getElementById('ocrContent');
    if (!content) return;

    content.innerHTML = `
        <div class="ocr-loading">
            <div class="spinner" style="width:48px;height:48px;border-width:4px"></div>
            <div class="ocr-loading-text">正在识别中...</div>
            <div class="ocr-loading-sub">图片已上传，百度OCR正在识别</div>
        </div>
    `;

    try {
        // 压缩图片
        const compressed = await compressImage(file);

        // 上传到后端OCR API
        const apiKey = getApiKey();
        const headers = {};
        if (apiKey) {
            headers['Authorization'] = `Bearer ${apiKey}`;
        }

        const formData = new FormData();
        formData.append('file', compressed);

        const resp = await fetch(`${API_BASE}/ocr`, {
            method: 'POST',
            headers,
            body: formData,
        });

        if (resp.status === 401) {
            clearApiKey();
            const newKey = prompt('API Key 无效或已过期，请重新输入:');
            if (newKey) {
                localStorage.setItem('7l_api_key', newKey);
                return ocrHandleFile(file); // 重试
            }
            throw new Error('未授权');
        }

        const data = await resp.json();
        if (!resp.ok) {
            throw new Error(data.detail || '识别失败');
        }

        ocrRawText = data.raw_text || [];
        ocrStudents = data.students || [];

        renderOCRResult();
    } catch(e) {
        content.innerHTML = `
            <div class="ocr-error">
                <div class="ocr-error-icon">😵</div>
                <div class="ocr-error-text">识别失败: ${e.message}</div>
                <button class="btn btn-outline" onclick="showOCRModal()" style="margin-top:16px">重新上传</button>
            </div>
        `;
    }
}

// 渲染OCR识别结果
function renderOCRResult() {
    const content = document.getElementById('ocrContent');
    if (!content) return;

    const priceOptions = cardPrices.map(p =>
        `<option value="${p.name}">${p.name} ¥${p.price}</option>`
    ).join('');

    if (ocrStudents.length === 0) {
        content.innerHTML = `
            <div class="ocr-no-result">
                <div class="ocr-no-result-icon">🤔</div>
                <div class="ocr-no-result-text">未识别到学员信息</div>
                <div class="ocr-no-result-sub">可能是截图格式不支持，或文字不清晰</div>
                ${ocrRawText.length > 0 ? `
                    <div class="ocr-raw-text">
                        <div class="ocr-raw-title">识别到的原始文字：</div>
                        ${ocrRawText.map(l => `<div class="ocr-raw-line">${l}</div>`).join('')}
                    </div>
                ` : ''}
                <button class="btn btn-outline" onclick="showOCRModal()" style="margin-top:16px">重新上传</button>
            </div>
        `;
        return;
    }

    content.innerHTML = `
        <div class="ocr-result">
            <div class="ocr-result-header">
                <span>✅ 识别到 <strong>${ocrStudents.length}</strong> 位学员</span>
                <button class="btn btn-sm btn-outline" onclick="showOCRModal()">重新上传</button>
            </div>

            <div class="ocr-student-list" id="ocrStudentList">
                ${ocrStudents.map((s, i) => `
                    <div class="ocr-student-item" id="ocrStudent${i}">
                        <div class="ocr-student-row">
                            <div class="ocr-student-index">${i + 1}</div>
                            <div class="ocr-student-fields">
                                <div class="form-group" style="margin-bottom:8px">
                                    <input class="form-input ocr-name-input" data-index="${i}" value="${s.name}" placeholder="姓名">
                                </div>
                                <div class="form-group" style="margin-bottom:8px">
                                    <select class="form-select ocr-card-select" data-index="${i}">
                                        <option value="">不指定卡种</option>
                                        ${priceOptions}
                                    </select>
                                </div>
                                <div class="form-group" style="margin-bottom:0">
                                    <input class="form-input ocr-note-input" data-index="${i}" value="${s.note || ''}" placeholder="备注">
                                </div>
                            </div>
                            <button class="ocr-remove-btn" onclick="ocrRemoveStudent(${i})" title="删除">✕</button>
                        </div>
                    </div>
                `).join('')}
            </div>

            <!-- 添加手动学员 -->
            <button class="btn btn-outline ocr-add-btn" onclick="ocrAddStudent()">
                ➕ 添加学员
            </button>

            <!-- 原始文字折叠 -->
            ${ocrRawText.length > 0 ? `
                <details class="ocr-raw-details">
                    <summary>查看原始识别文字 (${ocrRawText.length}行)</summary>
                    <div class="ocr-raw-content">
                        ${ocrRawText.map(l => `<div>${l}</div>`).join('')}
                    </div>
                </details>
            ` : ''}

            <!-- 批量录入按钮 -->
            <button class="btn btn-primary ocr-submit-btn" onclick="ocrBatchRegister()">
                ✅ 确认录入 ${ocrStudents.length} 位学员
            </button>
        </div>
    `;
}

// 删除某个识别出的学员
function ocrRemoveStudent(index) {
    ocrStudents.splice(index, 1);
    renderOCRResult();
}

// 手动添加学员
function ocrAddStudent() {
    ocrStudents.push({ name: '', note: '' });
    renderOCRResult();
    // 滚动到底部
    const list = document.getElementById('ocrStudentList');
    if (list) list.lastElementChild?.scrollIntoView({ behavior: 'smooth' });
}

// 批量录入
async function ocrBatchRegister() {
    // 收集当前编辑的值
    const students = [];
    const items = document.querySelectorAll('.ocr-student-item');
    items.forEach((item, i) => {
        const nameInput = item.querySelector('.ocr-name-input');
        const cardSelect = item.querySelector('.ocr-card-select');
        const noteInput = item.querySelector('.ocr-note-input');
        const name = nameInput?.value?.trim() || '';
        if (!name) return;
        students.push({
            name,
            card_name: cardSelect?.value || '',
            note: noteInput?.value?.trim() || '',
            payment_method: '微信',
        });
    });

    if (students.length === 0) {
        showToast('没有可录入的学员', 'warning');
        return;
    }

    // 禁用按钮，显示进度
    const submitBtn = document.querySelector('.ocr-submit-btn');
    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = '录入中...';
    }

    try {
        const result = await api('/ocr/batch-register', {
            method: 'POST',
            body: JSON.stringify({ students }),
        });

        const msg = `录入完成！成功 ${result.success_count} 人${result.fail_count > 0 ? '，失败 ' + result.fail_count + ' 人' : ''}`;
        showToast(msg, result.fail_count > 0 ? 'warning' : 'success');

        // 显示详细结果
        const content = document.getElementById('ocrContent');
        if (content) {
            content.innerHTML = `
                <div class="ocr-result">
                    <div class="ocr-result-header">
                        <span>📋 录入结果</span>
                    </div>
                    ${result.results.map(r => `
                        <div class="ocr-result-item ${r.success ? 'success' : 'fail'}">
                            <span>${r.success ? '✅' : '❌'} ${r.name}</span>
                            <span class="text-muted" style="font-size:12px">
                                ${r.success ? '会员号: ' + (r.member_id || '-') : r.error}
                            </span>
                        </div>
                    `).join('')}
                    <button class="btn btn-primary" onclick="closeModal(); navigate('students');" style="margin-top:16px">
                        查看学员列表
                    </button>
                </div>
            `;
        }
    } catch(e) {
        showToast('批量录入失败: ' + e.message, 'error');
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = `✅ 确认录入 ${students.length} 位学员`;
        }
    }
}

// ── 批量导入（智能映射版） ──────────────────────

let importPreviewData = []; // 预览数据
let importMappingData = null; // 智能映射结果
let importRawFile = null; // 原始上传文件
let importParsedRows = []; // 解析后的原始行数据

function showImportModal() {
    importPreviewData = [];
    importMappingData = null;
    importRawFile = null;
    importParsedRows = [];

    openModal(`
        <div class="modal-header">
            <div class="modal-title">📥 批量导入</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <div id="importContent">
            <div class="import-upload-area">
                <div class="import-upload-icon">📥</div>
                <div class="import-upload-title">上传学员数据文件</div>
                <div class="import-upload-desc">AI自动识别列名，支持任意格式Excel</div>
                <div class="import-btn-group">
                    <button class="btn btn-outline import-btn-download" onclick="downloadImportTemplate()">
                        📄 下载模板
                    </button>
                    <button class="btn btn-primary import-btn-upload" onclick="chooseImportFile()">
                        📁 选择文件
                    </button>
                </div>
                <div class="import-format-hint">
                    <div class="import-format-title">📋 支持格式</div>
                    <div class="import-format-fields">.csv, .xlsx, .xls — 列名不限，AI自动识别映射</div>
                    <div class="import-format-note">* 姓名必填，卡种名称匹配定价表可自动填充课时和有效期</div>
                </div>
            </div>
        </div>
    `);
}

// 下载模板
async function downloadImportTemplate() {
    try {
        const apiKey = getApiKey();
        const headers = {};
        if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;

        const resp = await fetch(`${API_BASE}/import/template`, { headers });
        if (!resp.ok) throw new Error('下载失败');

        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = '7L_import_template.csv';
        a.click();
        URL.revokeObjectURL(url);
        showToast('模板已下载');
    } catch(e) {
        showToast('下载模板失败: ' + e.message, 'error');
    }
}

// 选择文件
function chooseImportFile() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.csv,.xlsx,.xls';
    input.onchange = (e) => handleImportFile(e.target.files[0]);
    input.click();
}

// 处理上传的文件（智能映射流程）
async function handleImportFile(file) {
    if (!file) return;
    importRawFile = file;

    const content = document.getElementById('importContent');
    if (!content) return;

    // 显示解析中
    content.innerHTML = `
        <div class="import-loading">
            <div class="spinner" style="width:48px;height:48px;border-width:4px"></div>
            <div class="import-loading-text">正在智能识别列名...</div>
            <div class="import-loading-sub">AI正在分析你的文件结构</div>
        </div>
    `;

    try {
        const apiKey = getApiKey();
        const headers = {};
        if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;

        const formData = new FormData();
        formData.append('file', file);

        // 调用智能映射API
        const resp = await fetch(`${API_BASE}/import/smart-map`, {
            method: 'POST',
            headers,
            body: formData,
        });

        if (resp.status === 401) {
            clearApiKey();
            const newKey = prompt('API Key 无效或已过期，请重新输入:');
            if (newKey) {
                localStorage.setItem('7l_api_key', newKey);
                return handleImportFile(file);
            }
            throw new Error('未授权');
        }

        const data = await resp.json();
        if (!resp.ok) {
            throw new Error(data.detail || '映射失败');
        }

        importMappingData = data;

        // 同时解析原始数据用于预览
        try {
            if (file.name.endsWith('.xlsx') || file.name.endsWith('.xls')) {
                // Excel从后端返回的preview_rows获取预览数据
                importParsedRows = data.preview_rows || [];
            } else {
                const text = await file.text();
                const lines = text.split(/\r?\n/).filter(l => l.trim());
                if (lines.length >= 2) {
                    importParsedRows = parseCSVLines(lines);
                }
            }
        } catch(e) {
            importParsedRows = [];
        }

        // 渲染映射确认页
        renderImportMapping();
    } catch(e) {
        // 智能映射失败，降级为手动映射
        console.warn('智能映射失败，降级为手动模式:', e);
        try {
            // 尝试前端解析CSV
            if (!file.name.endsWith('.xlsx') && !file.name.endsWith('.xls')) {
                const text = await file.text();
                const lines = text.split(/\r?\n/).filter(l => l.trim());
                if (lines.length >= 2) {
                    importParsedRows = parseCSVLines(lines);
                    renderImportManualMapping();
                    return;
                }
            }
        } catch(e2) {
            console.warn('前端解析也失败:', e2);
        }

        content.innerHTML = `
            <div class="import-error">
                <div class="import-error-icon">😵</div>
                <div class="import-error-text">解析失败: ${e.message}</div>
                <button class="btn btn-outline" onclick="showImportModal()" style="margin-top:16px">重新选择</button>
            </div>
        `;
    }
}

// 简易CSV行解析
function parseCSVLines(lines) {
    if (lines.length < 2) return [];

    // 解析表头
    const headers = parseCSVRow(lines[0]);
    const data = [];

    for (let i = 1; i < lines.length; i++) {
        const values = parseCSVRow(lines[i]);
        if (values.every(v => !v.trim())) continue; // 跳过空行

        const row = {};
        headers.forEach((h, j) => {
            row[h.trim()] = (values[j] || '').trim();
        });
        data.push(row);
    }
    return data;
}

// 解析单行CSV（处理引号内的逗号）
function parseCSVRow(line) {
    const result = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"') {
            if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
                current += '"';
                i++;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (ch === ',' && !inQuotes) {
            result.push(current);
            current = '';
        } else {
            current += ch;
        }
    }
    result.push(current);
    return result;
}

// 渲染映射确认页（智能映射结果）
function renderImportMapping() {
    const content = document.getElementById('importContent');
    if (!content || !importMappingData) return;

    const columns = importMappingData.columns || [];
    const standardFields = importMappingData.standard_fields || [];

    // 统计映射情况
    const highCount = columns.filter(c => c.confidence === 'high').length;
    const mediumCount = columns.filter(c => c.confidence === 'medium').length;
    const lowCount = columns.filter(c => c.confidence === 'low').length;
    const noneCount = columns.filter(c => c.confidence === 'none').length;

    const fieldOptions = standardFields.map(f => `<option value="${f}">${f}</option>`).join('');

    content.innerHTML = `
        <div class="smart-import-mapping">
            <div class="smart-import-summary">
                <div class="smart-import-summary-title">🤖 AI列名识别结果</div>
                <div class="smart-import-summary-stats">
                    <span class="confidence-high-badge">✅ ${highCount} 列自动匹配</span>
                    <span class="confidence-medium-badge">⚠️ ${mediumCount + lowCount} 列需检查</span>
                    <span class="confidence-none-badge">❌ ${noneCount} 列未识别</span>
                </div>
            </div>

            <div class="smart-import-column-list">
                ${columns.map((col, i) => {
                    const confClass = col.confidence === 'high' ? 'confidence-high' :
                                     col.confidence === 'medium' ? 'confidence-medium' :
                                     col.confidence === 'low' ? 'confidence-low' : 'confidence-none';
                    const confIcon = col.confidence === 'high' ? '✅' :
                                    col.confidence === 'medium' ? '⚠️' :
                                    col.confidence === 'low' ? '🤔' : '❌';
                    const confLabel = col.confidence === 'high' ? '自动匹配' :
                                     col.confidence === 'medium' ? '建议映射' :
                                     col.confidence === 'low' ? '低置信度' : '需手动选择';
                    const methodLabel = col.method === 'rule' ? '规则匹配' :
                                       col.method === 'ai' ? 'AI映射' : '未识别';

                    return `
                    <div class="smart-import-column-item ${confClass}" data-index="${i}">
                        <div class="smart-import-column-header">
                            <div class="smart-import-column-original">
                                <span class="confidence-icon">${confIcon}</span>
                                <strong>${col.original}</strong>
                                <span class="confidence-method">${methodLabel}</span>
                            </div>
                            <div class="smart-import-column-confidence-label">${confLabel}</div>
                        </div>
                        <div class="smart-import-column-samples">
                            ${col.samples.map(s => `<span class="sample-tag">${s || '(空)'}</span>`).join('')}
                        </div>
                        <div class="smart-import-column-select">
                            <select class="form-select smart-mapping-select" data-index="${i}" data-original="${col.original}">
                                <option value="" ${!col.mapped_to ? 'selected' : ''}>不导入此列</option>
                                ${standardFields.map(f => `<option value="${f}" ${col.mapped_to === f ? 'selected' : ''}>${f}</option>`).join('')}
                            </select>
                        </div>
                    </div>
                    `;
                }).join('')}
            </div>

            <button class="btn btn-primary" onclick="confirmMappingAndPreview()">
                📋 确认映射，预览数据
            </button>
        </div>
    `;
}

// 确认映射后，预览数据
async function confirmMappingAndPreview() {
    // 收集用户确认的映射
    const mapping = {};
    document.querySelectorAll('.smart-mapping-select').forEach(sel => {
        const original = sel.dataset.original;
        const mappedTo = sel.value;
        if (mappedTo) {
            mapping[original] = mappedTo;
        }
    });

    // === Menxia要求：必填字段校验 ===
    const hasName = Object.values(mapping).includes('姓名');
    if (!hasName) {
        showToast('⚠️ 姓名是必填字段！请至少将一列映射到「姓名」', 'error');
        return;
    }

    // === Menxia要求：映射冲突检测 ===
    const fieldCounts = {};
    for (const field of Object.values(mapping)) {
        fieldCounts[field] = (fieldCounts[field] || 0) + 1;
    }
    const conflicts = Object.entries(fieldCounts).filter(([_, count]) => count > 1);
    if (conflicts.length > 0) {
        const conflictFields = conflicts.map(([f, c]) => `「${f}」被${c}列同时映射`).join('、');
        if (!confirm(`⚠️ 映射冲突：${conflictFields}\n\n多列映射到同一字段会导致数据覆盖，建议只保留一列。\n\n是否继续？`)) {
            return;
        }
    }

    // 根据映射转换数据
    const content = document.getElementById('importContent');
    if (!content) return;

    content.innerHTML = `
        <div class="import-loading">
            <div class="spinner" style="width:48px;height:48px;border-width:4px"></div>
            <div class="import-loading-text">正在准备预览数据...</div>
        </div>
    `;

    // 如果有前端解析的数据，直接转换
    if (importParsedRows.length > 0) {
        importPreviewData = importParsedRows.map(row => {
            const newRow = {};
            for (const [original, mappedTo] of Object.entries(mapping)) {
                newRow[mappedTo] = row[original] || '';
            }
            return newRow;
        }).filter(row => row['姓名']); // 只保留有姓名的行

        renderImportPreviewWithMapping(mapping);
    } else {
        // Excel文件需要后端解析，直接上传导入
        // 先显示映射信息，然后直接导入
        renderImportPreviewWithMapping(mapping);
    }
}

// 渲染预览（带映射信息）
function renderImportPreviewWithMapping(mapping) {
    const content = document.getElementById('importContent');
    if (!content) return;

    const previewRows = importPreviewData.slice(0, 10);
    const hasMore = importPreviewData.length > 10;
    const totalRows = importParsedRows.length || importPreviewData.length;

    // 映射摘要
    const mappedFields = Object.entries(mapping).filter(([_, v]) => v);

    content.innerHTML = `
        <div class="smart-import-preview">
            <div class="smart-import-preview-header">
                <span>📋 数据预览（${totalRows} 条）</span>
                <button class="btn btn-sm btn-outline" onclick="renderImportMapping()">返回修改映射</button>
            </div>

            ${mappedFields.length > 0 ? `
            <div class="smart-import-mapping-summary">
                <div class="smart-import-mapping-summary-title">映射关系</div>
                <div class="smart-import-mapping-pairs">
                    ${mappedFields.map(([orig, mapped]) => `<span class="mapping-pair">${orig} → ${mapped}</span>`).join('')}
                </div>
            </div>
            ` : ''}

            ${previewRows.length > 0 ? `
            <div class="import-preview-table-wrap">
                <table class="import-preview-table">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>姓名</th>
                            <th>卡种</th>
                            <th>金额</th>
                            <th>课时</th>
                            <th>激活日期</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${previewRows.map((r, i) => `
                            <tr>
                                <td>${i + 1}</td>
                                <td><strong>${r['姓名'] || '-'}</strong></td>
                                <td>${r['卡种名称'] || r['卡类型'] || '-'}</td>
                                <td>${r['金额'] ? '¥' + r['金额'] : '-'}</td>
                                <td>${r['总课时'] || '-'}</td>
                                <td>${r['激活日期'] || '-'}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
            ${hasMore ? `<div class="import-preview-more">还有 ${importPreviewData.length - 10} 条数据未显示...</div>` : ''}
            ` : `
            <div class="smart-import-no-preview">
                <div style="font-size:14px;color:var(--text-secondary);padding:20px;text-align:center">
                    Excel文件需要后端解析，点击下方按钮直接导入
                </div>
            </div>
            `}

            <button class="btn btn-primary import-submit-btn" onclick="submitSmartImport()">
                ✅ 确认导入 ${totalRows} 条数据
            </button>
        </div>
    `;
}

// 提交智能导入
async function submitSmartImport() {
    const submitBtn = document.querySelector('.import-submit-btn');
    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = '导入中...';
    }

    try {
        // 收集映射关系
        const mapping = {};
        document.querySelectorAll('.smart-mapping-select').forEach(sel => {
            const original = sel.dataset.original;
            const mappedTo = sel.value;
            if (mappedTo) {
                mapping[original] = mappedTo;
            }
        });

        // 如果有前端解析的数据，用映射转换后重建CSV上传
        if (importParsedRows.length > 0) {
            const mappedData = importParsedRows.map(row => {
                const newRow = {};
                for (const [original, mappedTo] of Object.entries(mapping)) {
                    newRow[mappedTo] = row[original] || '';
                }
                return newRow;
            }).filter(row => row['姓名']);

            const csvContent = buildCSVFromData(mappedData);
            const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8-sig' });
            const file = new File([blob], 'import.csv', { type: 'text/csv' });

            const apiKey = getApiKey();
            const headers = {};
            if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;

            const formData = new FormData();
            formData.append('file', file);

            const resp = await fetch(`${API_BASE}/import`, {
                method: 'POST',
                headers,
                body: formData,
            });

            if (resp.status === 401) {
                clearApiKey();
                const newKey = prompt('API Key 无效或已过期，请重新输入:');
                if (newKey) {
                    localStorage.setItem('7l_api_key', newKey);
                    return submitSmartImport();
                }
                throw new Error('未授权');
            }

            const result = await resp.json();
            if (!resp.ok) {
                throw new Error(result.detail || '导入失败');
            }

            renderImportResult(result);
        } else if (importRawFile) {
            // Excel文件：带映射关系上传到/import/with-mapping API
            const apiKey = getApiKey();
            const headers = {};
            if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;

            const formData = new FormData();
            formData.append('file', importRawFile);
            formData.append('mapping', JSON.stringify(mapping));

            const resp = await fetch(`${API_BASE}/import/with-mapping`, {
                method: 'POST',
                headers,
                body: formData,
            });

            if (resp.status === 401) {
                clearApiKey();
                const newKey = prompt('API Key 无效或已过期，请重新输入:');
                if (newKey) {
                    localStorage.setItem('7l_api_key', newKey);
                    return submitSmartImport();
                }
                throw new Error('未授权');
            }

            const result = await resp.json();
            if (!resp.ok) {
                throw new Error(result.detail || '导入失败');
            }

            renderImportResult(result);
        } else {
            throw new Error('没有可导入的数据');
        }
    } catch(e) {
        showToast('导入失败: ' + e.message, 'error');
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = `✅ 确认导入数据`;
        }
    }
}

// 降级：手动映射界面
function renderImportManualMapping() {
    const content = document.getElementById('importContent');
    if (!content) return;

    const columns = importParsedRows.length > 0 ? Object.keys(importParsedRows[0]) : [];
    const standardFields = ['姓名', '微信昵称', '电话', '金额', '付款方式', '卡类型', '卡种名称', '总课时', '剩余课时', '激活日期', '有效期至', '渠道来源', '备注'];

    // 提取每列前3行样本
    const samplesMap = {};
    for (const col of columns) {
        samplesMap[col] = importParsedRows.slice(0, 3).map(row => row[col] || '(空)');
    }

    content.innerHTML = `
        <div class="smart-import-mapping">
            <div class="smart-import-summary">
                <div class="smart-import-summary-title">📋 手动列名映射</div>
                <div class="smart-import-summary-stats">
                    <span style="color:var(--text-secondary);font-size:13px">AI识别未启用，请手动为每列选择对应的标准字段</span>
                </div>
            </div>

            <div class="smart-import-column-list">
                ${columns.map((col, i) => `
                    <div class="smart-import-column-item confidence-none" data-index="${i}">
                        <div class="smart-import-column-header">
                            <div class="smart-import-column-original">
                                <span class="confidence-icon">❓</span>
                                <strong>${col}</strong>
                            </div>
                        </div>
                        <div class="smart-import-column-samples">
                            ${samplesMap[col].map(s => `<span class="sample-tag">${s}</span>`).join('')}
                        </div>
                        <div class="smart-import-column-select">
                            <select class="form-select smart-mapping-select" data-index="${i}" data-original="${col}">
                                <option value="">不导入此列</option>
                                ${standardFields.map(f => `<option value="${f}" ${col === f ? 'selected' : ''}>${f}</option>`).join('')}
                            </select>
                        </div>
                    </div>
                `).join('')}
            </div>

            <button class="btn btn-primary" onclick="confirmMappingAndPreview()">
                📋 确认映射，预览数据
            </button>
        </div>
    `;
}

// 从数据重建CSV
function buildCSVFromData(data) {
    const headers = ['姓名', '微信昵称', '电话', '金额', '付款方式', '卡类型', '卡种名称', '总课时', '剩余课时', '激活日期', '有效期至', '渠道来源', '备注'];
    const lines = [headers.join(',')];

    for (const row of data) {
        const values = headers.map(h => {
            const val = row[h] || '';
            // 如果包含逗号或引号，用引号包裹
            if (val.includes(',') || val.includes('"')) {
                return '"' + val.replace(/"/g, '""') + '"';
            }
            return val;
        });
        lines.push(values.join(','));
    }

    return lines.join('\n');
}

// 渲染导入结果
function renderImportResult(result) {
    const content = document.getElementById('importContent');
    if (!content) return;

    content.innerHTML = `
        <div class="import-result">
            <div class="import-result-summary">
                <div class="import-result-icon">${result.fail_count > 0 ? '⚠️' : '🎉'}</div>
                <div class="import-result-text">${result.message}</div>
            </div>

            <div class="import-result-stats">
                <div class="import-stat-item success">
                    <div class="import-stat-value">${result.success_count}</div>
                    <div class="import-stat-label">成功</div>
                </div>
                <div class="import-stat-item fail">
                    <div class="import-stat-value">${result.fail_count}</div>
                    <div class="import-stat-label">失败</div>
                </div>
                <div class="import-stat-item total">
                    <div class="import-stat-value">${result.total}</div>
                    <div class="import-stat-label">总计</div>
                </div>
            </div>

            ${result.errors && result.errors.length > 0 ? `
                <div class="import-result-errors">
                    <div class="import-errors-title">❌ 失败明细</div>
                    ${result.errors.map(e => `
                        <div class="import-error-item">
                            <span>第${e.row}行 · ${e.name || '未知'}</span>
                            <span class="text-muted" style="font-size:12px">${e.error}</span>
                        </div>
                    `).join('')}
                </div>
            ` : ''}

            <button class="btn btn-primary" onclick="closeModal(); navigate('students');" style="margin-top:16px">
                查看学员列表
            </button>
        </div>
    `;
}

// ── AI助手 ──────────────────────────────────

// AI状态
let aiSessionId = null;
let aiMessages = []; // [{role: 'user'/'ai', content: '...', pending_action: null}]
let aiLoading = false;

// AI页面渲染（移动端全屏）
function renderAIPage() {
    const main = document.getElementById('mainContent');
    main.innerHTML = `
    <div class="page ai-page">
        <div class="ai-page-header">
            <h2>🤖 AI助手</h2>
            <p>用自然语言操作，如“张三买了次卡16次，980元”</p>
        </div>
        <div class="ai-messages" id="aiPageMessages">
            ${aiMessages.length === 0 ? `
                <div class="ai-msg ai-msg-ai">
                    <div class="ai-msg-bubble">
                        你好！我是7L AI助手 🏄\n\n我可以帮你：\n• **录入学员** — “张三买了次卡16次，980元”\n• **扣课签到** — “张三签到”\n• **查询学员** — “查张三”\n• **查看统计** — “本月统计”\n• **续费/退卡** — “张三续费次卡16次”\n\n试试看吧！
                    </div>
                </div>
            ` : aiMessages.map(m => renderAIMessage(m)).join('')}
        </div>
        <div class="ai-suggestions" id="aiSuggestions">
            <button class="ai-suggestion-chip" onclick="sendAISuggestion('录入学员')">📝 录入学员</button>
            <button class="ai-suggestion-chip" onclick="sendAISuggestion('扣课签到')">✋ 扣课签到</button>
            <button class="ai-suggestion-chip" onclick="sendAISuggestion('查看统计')">📊 查看统计</button>
            <button class="ai-suggestion-chip" onclick="sendAISuggestion('查询学员')">🔍 查询学员</button>
        </div>
        <div class="ai-input-area">
            <textarea class="ai-input" id="aiPageInput" placeholder="说点什么..." rows="1" onkeydown="handleAIInputKey(event, 'page')"></textarea>
            <button class="ai-send-btn" onclick="sendAIMessage('page')">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 21 11 13 2 9 22 2"/></svg>
            </button>
        </div>
    </div>
    `;
    scrollAIToBottom('aiPageMessages');
}

// 渲染单条AI消息
function renderAIMessage(msg) {
    const isUser = msg.role === 'user';
    let content = msg.content;
    
    // 简单Markdown渲染（加粗、列表）
    if (!isUser) {
        content = content
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\n• /g, '<br>• ')
            .replace(/\n- /g, '<br>- ')
            .replace(/\n/g, '<br>');
    }
    
    let confirmBtns = '';
    if (msg.pending_action && msg.need_confirm) {
        confirmBtns = `
            <div class="ai-confirm-area">
                <button class="ai-confirm-btn ai-confirm-yes" onclick="confirmAIAction(true)">✅ 确认</button>
                <button class="ai-confirm-btn ai-confirm-no" onclick="confirmAIAction(false)">取消</button>
            </div>
        `;
    }
    
    return `
        <div class="ai-msg ${isUser ? 'ai-msg-user' : 'ai-msg-ai'}">
            <div class="ai-msg-bubble">${content}</div>
            ${confirmBtns}
        </div>
    `;
}

// 发送快捷建议
function sendAISuggestion(text) {
    const input = document.getElementById('aiPageInput');
    if (input) {
        input.value = text;
        sendAIMessage('page');
    }
}

// 处理输入框按键
function handleAIInputKey(event, source) {
    if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        sendAIMessage(source);
    }
}

// 发送AI消息
async function sendAIMessage(source) {
    const inputId = source === 'page' ? 'aiPageInput' : 'aiFloatInput';
    const messagesId = source === 'page' ? 'aiPageMessages' : 'aiFloatMessages';
    const input = document.getElementById(inputId);
    const messagesEl = document.getElementById(messagesId);
    
    if (!input || !messagesEl) return;
    
    const message = input.value.trim();
    if (!message || aiLoading) return;
    
    input.value = '';
    input.style.height = 'auto';
    
    // 添加用户消息
    const userMsg = {role: 'user', content: message};
    aiMessages.push(userMsg);
    appendAIMessage(messagesEl, userMsg);
    
    // 显示打字指示器
    aiLoading = true;
    const typingEl = showAITyping(messagesEl);
    scrollAIToBottom(messagesId);
    
    try {
        const result = await api('/ai/chat', {
            method: 'POST',
            body: JSON.stringify({
                message: message,
                session_id: aiSessionId || undefined,
            }),
        });
        
        aiSessionId = result.session_id;
        
        // 移除打字指示器
        if (typingEl) typingEl.remove();
        
        // 添加AI回复
        const aiMsg = {
            role: 'ai',
            content: result.reply,
            pending_action: result.pending_action,
            need_confirm: result.need_confirm,
        };
        aiMessages.push(aiMsg);
        appendAIMessage(messagesEl, aiMsg);
        scrollAIToBottom(messagesId);
        
    } catch(e) {
        if (typingEl) typingEl.remove();
        const errMsg = {role: 'ai', content: 'AI暂时不可用，请用按钮操作'};
        aiMessages.push(errMsg);
        appendAIMessage(messagesEl, errMsg);
    } finally {
        aiLoading = false;
    }
}

// 确认AI操作
async function confirmAIAction(confirmed) {
    if (!aiSessionId) return;
    
    // 找到最近的确认消息，更新UI
    const messagesEls = document.querySelectorAll('.ai-messages');
    
    // 禁用所有确认按钮
    document.querySelectorAll('.ai-confirm-btn').forEach(btn => {
        btn.disabled = true;
        btn.style.opacity = '0.5';
    });
    
    try {
        const result = await api('/ai/confirm', {
            method: 'POST',
            body: JSON.stringify({
                session_id: aiSessionId,
                confirmed: confirmed,
            }),
        });
        
        aiSessionId = result.session_id;
        
        // 添加结果消息
        const aiMsg = {
            role: 'ai',
            content: result.reply,
            pending_action: null,
            need_confirm: false,
        };
        aiMessages.push(aiMsg);
        
        // 更新所有消息区域
        messagesEls.forEach(el => {
            appendAIMessage(el, aiMsg);
        });
        
        // 刷新当前页面数据
        if (currentPage === 'dashboard') renderDashboard();
        if (currentPage === 'students') loadStudents();
        
    } catch(e) {
        const errMsg = {role: 'ai', content: '操作失败：' + e.message};
        aiMessages.push(errMsg);
        messagesEls.forEach(el => {
            appendAIMessage(el, errMsg);
        });
    }
}

// 追加消息到DOM
function appendAIMessage(container, msg) {
    container.insertAdjacentHTML('beforeend', renderAIMessage(msg));
}

// 显示打字指示器
function showAITyping(container) {
    const el = document.createElement('div');
    el.className = 'ai-msg ai-msg-ai';
    el.innerHTML = `
        <div class="ai-msg-bubble">
            <div class="ai-typing">
                <div class="ai-typing-dot"></div>
                <div class="ai-typing-dot"></div>
                <div class="ai-typing-dot"></div>
            </div>
        </div>
    `;
    container.appendChild(el);
    return el;
}

// 滚动到底部
function scrollAIToBottom(containerId) {
    const el = document.getElementById(containerId);
    if (el) {
        setTimeout(() => {
            el.scrollTop = el.scrollHeight;
        }, 50);
    }
}

// PC端：切换AI浮动面板
function toggleAIPanel() {
    const panel = document.getElementById('aiFloatPanel');
    const bubble = document.getElementById('aiFloatBubble');
    
    if (panel.classList.contains('show')) {
        panel.classList.remove('show');
        bubble.style.display = 'flex';
    } else {
        panel.classList.add('show');
        bubble.style.display = 'none';
        
        // 同步消息到浮动面板
        const floatMessages = document.getElementById('aiFloatMessages');
        if (floatMessages && aiMessages.length === 0) {
            floatMessages.innerHTML = renderAIMessage({
                role: 'ai',
                content: '你好！我是7L AI助手 🏄\n\n我可以帮你录入学员、扣课签到、查询信息等。试试看吧！',
            });
        } else if (floatMessages) {
            floatMessages.innerHTML = aiMessages.map(m => renderAIMessage(m)).join('');
            scrollAIToBottom('aiFloatMessages');
        }
    }
}
