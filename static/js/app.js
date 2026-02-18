const state = {
    signals: [],
    selectedSignal: null,
    lastUpdatedAt: null,
    livePrices: [],
    previousLiveBySymbol: {},
};

document.addEventListener('DOMContentLoaded', () => {
    setupSocket();
    fetchInitialSignals();
    fetchLivePrices();
    setInterval(updateFreshnessBadge, 1000);
    setInterval(fetchLivePrices, 3000);
    const toggle = document.getElementById('toggle-breakdown');
    if (toggle) {
        toggle.addEventListener('click', () => {
            const panel = document.getElementById('breakdown-panel');
            panel.classList.toggle('hidden');
            toggle.innerText = panel.classList.contains('hidden') ? 'Show details' : 'Hide details';
        });
    }
    const signalTable = document.getElementById('signals-table-body');
    if (signalTable) {
        signalTable.addEventListener('click', event => {
            const row = event.target.closest('tr[data-symbol]');
            if (!row) return;
            const symbol = row.dataset.symbol;
            const found = state.signals.find(s => s.symbol === symbol);
            if (found) {
                state.selectedSignal = found;
                renderPrimarySelection(found);
            }
        });
    }
});

function setupSocket() {
    const socket = io();
    socket.on('connect', () => setConnectionStatus(true));
    socket.on('disconnect', () => setConnectionStatus(false));
    socket.on('unified_signal_update', payload => {
        if (payload?.signals) {
            renderUnifiedSignals(payload.signals);
        }
    });
    socket.on('debug_log', data => appendLog(`[${data.level}] ${data.message}`));
}

async function fetchInitialSignals() {
    try {
        const res = await fetch('/api/unified/signals');
        const signals = await res.json();
        renderUnifiedSignals(signals);
    } catch (err) {
        appendLog(`Failed to load unified signals: ${err}`);
    }
}

function renderUnifiedSignals(signals = []) {
    state.signals = Array.isArray(signals) ? signals : [];
    const sorted = [...state.signals].sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
    if (!sorted.length) {
        showEmptyState();
        return;
    }
    const selected = state.selectedSignal ? sorted.find(item => item.symbol === state.selectedSignal.symbol) : null;
    const primary = selected || sorted[0];
    state.selectedSignal = primary;
    renderPrimarySelection(primary);
    state.lastUpdatedAt = primary.updated_at || null;
    updateFreshnessBadge();
    updateSignalTable(sorted);
    updateLivePriceBoard(state.livePrices.length ? state.livePrices : sorted);
}

function renderPrimarySelection(signal) {
    updateSignalCard(signal);
    updateComponentList(signal);
    loadHistory(signal.symbol);
}

function updateSignalCard(signal) {
    const directionEl = document.getElementById('signal-direction');
    const confidenceEl = document.getElementById('signal-confidence');
    const symbolEl = document.getElementById('signal-symbol');
    const timeEl = document.getElementById('signal-time');

    if (!directionEl) return;

    const direction = signal.direction || 'HOLD';
    directionEl.innerText = direction;
    directionEl.className = `direction-pill ${direction.toLowerCase()}`;
    confidenceEl.innerText = `${Math.round((signal.confidence || 0) * 100)}%`;
    symbolEl.innerText = signal.symbol || '—';
    timeEl.innerText = formatTimestamp(signal.updated_at) || new Date().toLocaleTimeString();

    const isHold = direction === 'HOLD';
    setText('entry-price', isHold ? '—' : formatPrice(signal.entry_price));
    setText('target-price', isHold ? '—' : formatPrice(signal.target_price));
    setText('stop-loss', isHold ? '—' : formatPrice(signal.stop_loss));
    setText('position-size', formatValue(signal.position_size_value));
    setText('risk-reward', signal.risk_reward ? signal.risk_reward.toFixed(2) : '—');
    setText('timeframe-execution', signal.timeframes?.execution || '1H');
    setText('timeframe-context', signal.timeframes?.context || '1D');
    const rationaleEl = document.getElementById('primary-rationale');
    if (rationaleEl) {
        rationaleEl.innerText = signal.rationale?.[0] || 'Monitoring market regime...';
    }
}

function updateComponentList(signal) {
    const list = document.getElementById('component-list');
    if (!list) return;
    const components = signal.components || [];
    if (!components.length) {
        list.innerHTML = '<p class="signal-empty">Waiting for component data...</p>';
        return;
    }
    list.innerHTML = components
        .map(component => {
            const direction = component.direction || 'HOLD';
            const score = component.score ?? component.raw_score;
            return `
                <div class="component-item">
                    <div>
                        <div>${component.name.replace('_', ' ').toUpperCase()}</div>
                        <small class="text-muted">${component.narrative || ''}</small>
                    </div>
                    <div class="component-score ${direction.toLowerCase()}">
                        ${score > 0 ? '+' : ''}${(score || 0).toFixed(2)}
                    </div>
                </div>
            `;
        })
        .join('');
}

function updateSignalTable(signals) {
    const tbody = document.getElementById('signals-table-body');
    if (!tbody) return;
    const rows = signals.slice(0, 8).map(signal => `
        <tr data-symbol="${escapeHtml(signal.symbol || '')}" style="cursor:pointer;">
            <td>${signal.symbol}</td>
            <td>${signal.direction}</td>
            <td>${Math.round((signal.confidence || 0) * 100)}%</td>
            <td>${formatPrice(signal.entry_price)}</td>
        </tr>
    `);
    tbody.innerHTML = rows.join('');
}

async function loadHistory(symbol) {
    if (!symbol) return;
    setText('history-symbol', `Selected symbol: ${symbol}`);
    try {
        const res = await fetch(`/api/unified/history/${encodeURIComponent(symbol)}?limit=20`);
        const history = await res.json();
        renderHistoryTable(Array.isArray(history) ? history : []);
    } catch (err) {
        appendLog(`History load failed: ${err}`);
    }
}

function renderHistoryTable(history) {
    const tbody = document.getElementById('history-table-body');
    if (!tbody) return;
    if (!history.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="signal-empty">No historical suggestions yet.</td></tr>';
        return;
    }
    const rows = [...history]
        .reverse()
        .slice(-12)
        .map(item => `
            <tr>
                <td>${item.symbol || '—'}</td>
                <td>${formatTimestamp(item.updated_at, true) || '—'}</td>
                <td>${item.direction || 'HOLD'}</td>
                <td>${Math.round((item.confidence || 0) * 100)}%</td>
                <td>${formatPrice(item.entry_price)}</td>
            </tr>
        `);
    tbody.innerHTML = rows.join('');
}

function updateLivePriceBoard(signals) {
    const tbody = document.getElementById('live-prices-body');
    if (!tbody) return;

    if (!signals || !signals.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="signal-empty">No live prices yet.</td></tr>';
        return;
    }

    const rows = signals.slice(0, 12).map(signal => {
        const isLiveRow = signal.ltp !== undefined || signal.best_bid !== undefined || signal.best_ask !== undefined;
        const price = isLiveRow ? signal : (signal.extra?.price_context || {});
        const symbol = signal.symbol || '—';
        const prev = state.previousLiveBySymbol[symbol] || {};

        const nowLtp = toNumber(price.ltp ?? signal.entry_price);
        const prevLtp = toNumber(prev.ltp);
        let rowClass = '';
        if (nowLtp !== null && prevLtp !== null && nowLtp !== prevLtp) {
            rowClass = nowLtp > prevLtp ? 'price-up' : 'price-down';
        }

        const updated = formatTimestamp(price.updated_at || signal.updated_at, true) || '—';

        state.previousLiveBySymbol[symbol] = {
            ltp: nowLtp,
            best_bid: toNumber(price.best_bid),
            best_ask: toNumber(price.best_ask),
            updated_at: price.updated_at || signal.updated_at || null,
        };

        return `
            <tr class="${rowClass}">
                <td>${symbol}</td>
                <td>${formatPrice(price.ltp ?? signal.entry_price)}</td>
                <td>${formatPrice(price.best_bid)}</td>
                <td>${formatPrice(price.best_ask)}</td>
                <td>${formatPercent(price.spread_pct)}</td>
                <td>${updated}</td>
            </tr>
        `;
    });

    tbody.innerHTML = rows.join('');
}

function toNumber(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return null;
    return Number(value);
}

async function fetchLivePrices() {
    try {
        const res = await fetch('/api/unified/live_prices');
        const prices = await res.json();
        state.livePrices = Array.isArray(prices) ? prices : [];
        updateLivePriceBoard(state.livePrices.length ? state.livePrices : state.signals);
    } catch (err) {
        appendLog(`Live prices fetch failed: ${err}`);
    }
}

function setConnectionStatus(isConnected) {
    const el = document.getElementById('connection-status');
    if (!el) return;
    el.classList.toggle('connected', isConnected);
    el.querySelector('span').style.background = isConnected ? 'var(--success)' : 'var(--danger)';
    el.querySelector('strong').innerText = isConnected ? 'Live feed' : 'Disconnected';
}

function showEmptyState() {
    setText('signal-symbol', 'Awaiting data');
    setText('signal-direction', 'HOLD');
    setText('signal-confidence', '0%');
    const list = document.getElementById('component-list');
    if (list) list.innerHTML = '<p class="signal-empty">No unified signals yet. Streaming data will appear here.</p>';
    const historyBody = document.getElementById('history-table-body');
    if (historyBody) historyBody.innerHTML = '<tr><td colspan="5" class="signal-empty">No historical suggestions yet.</td></tr>';
    const livePricesBody = document.getElementById('live-prices-body');
    if (livePricesBody) livePricesBody.innerHTML = '<tr><td colspan="6" class="signal-empty">No live prices yet.</td></tr>';
}

function setText(id, value) {
    const el = document.getElementById(id);
    if (el) {
        el.innerText = value ?? '—';
    }
}

function formatPrice(value) {
    if (value === null || value === undefined) return '—';
    return `₹${Number(value).toFixed(2)}`;
}

function formatValue(value) {
    if (!value) return '—';
    return `₹${Number(value).toLocaleString('en-IN', { maximumFractionDigits: 0 })}`;
}

function formatPercent(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
    return `${Number(value).toFixed(2)}%`;
}

function appendLog(message) {
    const el = document.getElementById('event-log');
    if (!el) return;
    const time = new Date().toLocaleTimeString();
    el.innerHTML = `<div>[${time}] ${message}</div>` + el.innerHTML;
}

function updateFreshnessBadge() {
    const freshness = document.getElementById('signal-freshness');
    if (!freshness) return;
    if (!state.lastUpdatedAt) {
        freshness.innerText = 'Waiting for first cycle';
        return;
    }
    const updated = parseTimestamp(state.lastUpdatedAt);
    if (!updated) {
        freshness.innerText = 'Time parse error';
        return;
    }
    const diffSec = Math.max(0, Math.floor((Date.now() - updated.getTime()) / 1000));
    freshness.innerText = diffSec <= 70 ? `Fresh (${diffSec}s ago)` : `Stale (${diffSec}s ago)`;
}

function parseTimestamp(value) {
    if (!value) return null;
    const hasZone = /[zZ]|[+-]\d{2}:?\d{2}$/.test(value);
    const iso = hasZone ? value : `${value}Z`;
    const dt = new Date(iso);
    return Number.isNaN(dt.getTime()) ? null : dt;
}

function formatTimestamp(value, short = false) {
    const dt = parseTimestamp(value);
    if (!dt) return null;
    if (short) {
        return dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    }
    return dt.toLocaleTimeString();
}

function escapeHtml(text) {
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}
