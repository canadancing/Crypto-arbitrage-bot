/* ===== Element References ===== */
const $ = (id) => document.getElementById(id);

const els = {
  statusPill: $("status-pill"),
  activePos: $("active-pos"),
  exchangeCards: $("exchange-cards"),
  positionsList: $("positions-list"),
  activityList: $("activity-list"),
  exchangeTabs: $("exchange-tabs"),
  refreshBtn: $("refresh-btn"),
  themeBtn: $("theme-btn"),
  themeIconMoon: $("theme-icon-moon"),
  themeIconSun: $("theme-icon-sun"),
  heroEquityVal: $("hero-equity-val"),
  historyBtn: $("history-btn"),
  historyModal: $("history-modal"),
  historyCloseBtn: $("history-close-btn"),
  historyTbody: $("history-tbody"),

  toast: $("toast"),

  // Analytics
  equityHeaderBtn: $("equity-header-btn"),
  analyticsModal: $("analytics-modal"),
  analyticsCloseBtn: $("analytics-close-btn"),
  pnl7d: $("pnl-7d"),
  pnl30d: $("pnl-30d"),
  pnlCum: $("pnl-cum"),
  chartCtx: $("earnings-chart"),

  // Earn / Loss Analysis
  elTotalPnl: $("el-total-pnl"),
  elWins: $("el-wins"),
  elLosses: $("el-losses"),
  elWinrate: $("el-winrate"),
  lossReasonsList: $("loss-reasons-list"),
  coinRankingList: $("coin-ranking-list"),

  // Optimization Insights
  optAvgWin: $("opt-avg-win"),
  optAvgLoss: $("opt-avg-loss"),
  optAvgRoi: $("opt-avg-roi"),
  optHoldWins: $("opt-hold-wins"),
  optHoldLosses: $("opt-hold-losses"),
  optHoldAll: $("opt-hold-all")
};

/* ===== Helpers ===== */
function formatDuration(seconds) {
  if (!seconds || seconds < 0) return "0h 0m";
  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  return `${hrs}h ${mins}m`;
}

function money(n) {
  if (typeof n !== "number" || !Number.isFinite(n)) return "-";
  const abs = Math.abs(n);
  const formatted = abs.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return n < 0 ? `-$${formatted}` : `$${formatted}`;
}
function esc(s) {
  return String(s ?? "").replace(/[<>&"]/g, (c) =>
    ({ "<": "&lt;", ">": "&gt;", "&": "&amp;", '"': "&quot;" }[c])
  );
}
function capitalize(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1) : ""; }

/* ===== Toast ===== */
function showToast(msg, type = "success") {
  els.toast.textContent = msg;
  els.toast.className = `toast show ${type}`;
  clearTimeout(showToast._t);
  showToast._t = setTimeout(() => els.toast.classList.remove("show"), 3500);
}

/* ===== Activity Feed Filter ===== */
let _allLogs = [];
let _feedFilter = 'all';
let _feedSearch = '';

function updateFeed() {
  const q = _feedSearch.toLowerCase();
  const filtered = _allLogs.filter(line => {
    const matchesFilter = _feedFilter === 'all' || line.includes(_feedFilter);
    const matchesSearch = !q || line.toLowerCase().includes(q);
    return matchesFilter && matchesSearch;
  });

  // Colour-code by level
  renderFeed(els.activityList, filtered.slice(0, 60), (line) => {
    let cls = '';
    if (line.includes(' - ERROR - ') || line.includes(' - CRITICAL - ')) cls = 'log-error';
    else if (line.includes(' - WARNING - ')) cls = 'log-warn';
    const badge = line.includes('bot.binance') ? '<span class="log-badge binance">BIN</span>'
      : line.includes('bot.gateio') ? '<span class="log-badge gateio">GATE</span>'
        : '';
    return `<div class="log-item ${cls}">${badge}${esc(line)}</div>`;
  }, filtered.length ? '' : 'No matching log entries');
}

/* ===== Exchange Tab State ===== */
let selectedExchange = "all";

function buildTabs(exchanges) {
  const names = exchanges.map(e => e.exchange);
  let html = `<div class="exchange-tab all ${selectedExchange === 'all' ? 'active' : ''}" data-ex="all">All</div>`;
  for (const name of names) {
    html += `<div class="exchange-tab ${selectedExchange === name ? 'active' : ''}" data-ex="${name}">${capitalize(name)}</div>`;
  }
  els.exchangeTabs.innerHTML = html;
  els.exchangeTabs.querySelectorAll(".exchange-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      selectedExchange = tab.dataset.ex;
      els.exchangeTabs.querySelectorAll(".exchange-tab").forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
      if (lastData) update(lastData);
    });
  });
}

/* ===== Main Update ===== */
let lastData = null;

function update(data) {
  lastData = data;
  const exchanges = data.exchanges || [];
  const filtered = selectedExchange === "all"
    ? exchanges
    : exchanges.filter(e => e.exchange === selectedExchange);

  const running = filtered.some(e => e.running);

  // Aggregate equity (Restored and fixed calculation so it doesn't double-count PnL)
  const totalEquity = filtered.reduce((s, e) => s + (e.starting_equity || 0), 0);
  els.heroEquityVal.textContent = money(totalEquity);

  // Remove top right status pill completely
  els.statusPill.style.display = 'none';

  // Open Positions Total Invested Badge
  const allPos = filtered.flatMap(e => (e.open_positions || []).map(p => ({ ...p, exchange: e.exchange })));
  const totalInvested = allPos.reduce((s, p) => s + (p.size_usd || 0), 0);

  els.activePos.className = `status-pill live`;
  els.activePos.innerHTML = `<span style="opacity:0.6; font-size:10px; margin-right:4px;">OPEN POS:</span><span style="font-family:var(--mono)">${money(totalInvested)}</span>`;

  // Build one tab on first load
  if (!els.exchangeTabs.children.length && exchanges.length) {
    buildTabs(exchanges);
  }

  // Per-exchange detail cards
  let cardsHtml = "";
  for (const ex of filtered) {
    const name = ex.exchange;
    const badgeCls = name === "binance" ? "binance" : "gateio";
    const lossIcon = ex.loss_limit_exceeded ? "⚠️ EXCEEDED" : "✅ OK";
    const binanceIcon = `
      <div class="glass-icon-wrapper" style="width: 30px; height: 30px; border-radius: 8px; background: linear-gradient(135deg, rgba(243, 186, 47, 0.2) 0%, rgba(243, 186, 47, 0.05) 100%); backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px); border: 1px solid rgba(243, 186, 47, 0.3); box-shadow: 0 4px 12px rgba(243, 186, 47, 0.15), inset 0 1px 0 rgba(255,255,255,0.2); display: flex; align-items: center; justify-content: center; position: relative; overflow: hidden;">
        <div style="position: absolute; top: -50%; left: -50%; width: 200%; height: 200%; background: linear-gradient(to bottom right, rgba(255,255,255,0.5) 0%, rgba(255,255,255,0) 40%, rgba(255,255,255,0) 100%); transform: rotate(30deg); pointer-events: none;"></div>
        <svg width="18" height="18" viewBox="0 0 24 24" fill="#F3BA2F" style="filter: drop-shadow(0 2px 4px rgba(243, 186, 47, 0.6)); position: relative; z-index: 1;"><path d="M12 24L1.258 13.257L4.544 9.971L12 17.427L19.456 9.971L22.742 13.257L12 24ZM12 0L22.742 10.743L19.456 14.029L12 6.573L4.544 14.029L1.258 10.743L12 0ZM5.986 12L8.608 9.378L12 12.771L15.392 9.378L18.014 12L12 18.014L5.986 12Z"/></svg>
      </div>`;
    const gateioIcon = `
      <div class="glass-icon-wrapper" style="width: 30px; height: 30px; border-radius: 8px; background: linear-gradient(135deg, rgba(14, 165, 233, 0.2) 0%, rgba(14, 165, 233, 0.05) 100%); backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px); border: 1px solid rgba(14, 165, 233, 0.3); box-shadow: 0 4px 12px rgba(14, 165, 233, 0.15), inset 0 1px 0 rgba(255,255,255,0.2); display: flex; align-items: center; justify-content: center; position: relative; overflow: hidden;">
        <div style="position: absolute; top: -50%; left: -50%; width: 200%; height: 200%; background: linear-gradient(to bottom right, rgba(255,255,255,0.5) 0%, rgba(255,255,255,0) 40%, rgba(255,255,255,0) 100%); transform: rotate(30deg); pointer-events: none;"></div>
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#0ea5e9" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="filter: drop-shadow(0 2px 4px rgba(14, 165, 233, 0.6)); position: relative; z-index: 1;"><path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z"></path></svg>
      </div>`;

    // Process recent errors for warning banner
    let errorsHtml = "";
    if (ex.recent_errors && ex.recent_errors.length > 0) {
      const errorList = ex.recent_errors.map(err => {
        const timeStr = new Date(err.time * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        return `<div style="font-size: 11px; margin-bottom: 2px;">• <strong>${esc(err.symbol)}</strong>: ${esc(err.message)} <span style="opacity:0.7">(${timeStr})</span></div>`;
      }).join("");

      errorsHtml = `
        <div style="background: rgba(239, 68, 68, 0.1); border: 1px solid rgba(239, 68, 68, 0.3); border-radius: 8px; padding: 10px 12px; margin-bottom: 16px; color: #fca5a5;">
          <div style="display: flex; align-items: center; gap: 8px; font-weight: 600; font-size: 13px; margin-bottom: 6px;">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>
            Recent Close Failures (Requires Manual Check)
          </div>
          ${errorList}
        </div>
      `;
    }

    cardsHtml += `
      <div class="card exchange-card">
        <div class="card-header-flex" style="margin-bottom: 20px; align-items: center;">
          <div style="display: flex; align-items: center; gap: 12px;">
            <div class="status-dot tooltip" style="width: 10px; height: 10px; border-radius: 50%; box-shadow: 0 0 10px var(--${ex.running ? 'good' : 'bad'}); background: ${ex.running ? '#10B981' : '#EF4444'};" title="${ex.running ? 'Online' : 'Offline'}">
            </div>
            ${name === 'binance' ? binanceIcon : gateioIcon}
          </div>
          <div class="positions-stats">
            <span class="status-pill down" style="font-family: var(--mono);">${ex.positions || 0} / ${ex.max_positions || 0} POS</span>
          </div>
        </div>
        
        ${errorsHtml}
        
        <div class="equity-showcase">
          <div class="equity-total">
            <span class="eq-label">Total Equity</span>
            <span class="eq-value">${money(ex.starting_equity || 0)}</span>
          </div>
          <div class="equity-split">
            <div class="split-col">
              <span class="split-label">Spot</span>
              <span class="split-val">${money(ex.spot_equity || 0)}</span>
            </div>
            <div class="split-div"></div>
            <div class="split-col">
              <span class="split-label">${name === 'gateio' ? 'USDT' : 'Futures'}</span>
              <span class="split-val">${money(ex.futures_equity || 0)}</span>
            </div>
            <div class="split-div"></div>
            <div class="split-col">
              <span class="split-label">Daily PnL</span>
              <span class="split-val ${ex.daily_pnl >= 0 ? 'good' : 'bad'}">${ex.daily_pnl >= 0 ? '+' : ''}${money(ex.daily_pnl || 0)}</span>
            </div>
          </div>
        </div>
      </div>`;
  }
  els.exchangeCards.innerHTML = cardsHtml;

  // Open Positions
  const allPositions = filtered.flatMap(e => (e.open_positions || []).map(p => ({ ...p, exchange: e.exchange })));
  renderFeed(els.positionsList, allPositions, (p) => {
    const stratBadge = p.strategy === "positive_carry" ? "pos" : "rev";
    const stratLabel = p.strategy === "positive_carry" ? "POS" : "REV";
    const pnl = p.pnl?.total_pnl ?? 0;
    const fundingFee = p.pnl?.funding_fee ?? 0;
    const elapsed = Date.now() / 1000 - p.entry_time;
    const hrs = Math.floor(elapsed / 3600);
    const mins = Math.floor((elapsed % 3600) / 60);
    return `<div class="feed-item" style="display: flex; justify-content: space-between; align-items: center; padding: 12px 14px; margin-bottom: 8px; background: rgba(255, 255, 255, 0.02); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 10px; transition: transform 0.2s ease, box-shadow 0.2s ease; cursor: default;">
      <style>
        .feed-item:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(0,0,0,0.15); background: rgba(255, 255, 255, 0.04) !important; border-color: rgba(255,255,255,0.1) !important; }
        [data-theme="light"] .feed-item { background: rgba(0, 0, 0, 0.02) !important; border-color: rgba(0, 0, 0, 0.05) !important; }
        [data-theme="light"] .feed-item:hover { background: rgba(0, 0, 0, 0.04) !important; box-shadow: 0 4px 12px rgba(0,0,0,0.05); border-color: rgba(0,0,0,0.1) !important; }
      </style>
      <div class="info" style="display: flex; align-items: center; gap: 12px;">
        <div style="display: flex; flex-direction: column; gap: 4px;">
          <div class="primary" style="display: flex; align-items: center; gap: 6px; font-weight: 600; font-size: 14px; color: var(--text);">
            <span class="badge badge-${stratBadge}" style="font-size: 10px; padding: 2px 6px;">${stratLabel}</span>
            <span class="exch-badge ${p.exchange}" style="font-size: 10px; padding: 2px 6px;">${p.exchange.toUpperCase()}</span>
            ${esc(p.symbol)}
          </div>
          <div class="secondary" style="font-size: 12px; color: var(--text-2); font-family: var(--mono);">${money(p.size_usd || 0)} <span style="opacity:0.5; margin: 0 4px;">•</span> ${hrs}h ${mins}m</div>
        </div>
      </div>
      <div style="text-align: right; display: flex; flex-direction: column; gap: 4px; align-items: flex-end;">
        <div class="value ${pnl >= 0 ? 'good' : 'bad'}" style="font-family: var(--mono); font-weight: 700; font-size: 15px; background: rgba(16, 185, 129, 0.1); padding: 2px 8px; border-radius: 6px;">
          ${pnl < 0 ? '' : '+'}${money(pnl)}
        </div>
        <div style="font-size: 11px; color: var(--cyan); font-family: var(--mono); font-weight: 500;">
          Yield: +${money(fundingFee)}
        </div>
      </div>
    </div>`;
  }, "No open positions");

  // Activity — store all lines, re-render through filter/search
  _allLogs = (data.recent_logs || []).slice().reverse();
  updateFeed();
}

function renderFeed(el, rows, fn, empty) {
  if (!rows || !rows.length) {
    el.innerHTML = `<div class="log-item" style="color:var(--text-3); text-align:center;">${esc(empty)}</div>`;
    return;
  }
  el.innerHTML = rows.map(fn).join("");
}

/* ===== Analytics Chart ===== */
let earningsChart = null;
let lastAnalyticsData = null;

function renderAnalytics(data) {
  lastAnalyticsData = data;
  if (!els.pnlCum) return;

  // Yesterday Asset
  if (data.yesterday_asset !== null && data.yesterday_asset !== undefined) {
    els.pnl30d.textContent = money(data.yesterday_asset);
    els.pnl30d.style.color = "var(--cyan)";
  } else {
    els.pnl30d.textContent = "—";
    els.pnl30d.style.color = "var(--text-2)";
  }
  els.pnl30d.className = `value hero-value`;

  // Today Asset — always present
  els.pnlCum.textContent = money(data.today_asset);
  els.pnlCum.className = `value hero-value`;
  els.pnlCum.style.color = "var(--cyan)";

  // Daily Change
  if (data.daily_change !== null && data.daily_change !== undefined) {
    els.pnl7d.textContent = (data.daily_change >= 0 ? '+' : '') + money(data.daily_change);
    els.pnl7d.className = `value hero-value ${data.daily_change >= 0 ? 'good' : 'bad'}`;
    els.pnl7d.style.color = "";
  } else {
    els.pnl7d.textContent = "—";
    els.pnl7d.className = `value hero-value`;
    els.pnl7d.style.color = "var(--text-2)";
  }

  const isDark = document.documentElement.getAttribute("data-theme") !== "light";
  const gridColor = isDark ? "rgba(255, 255, 255, 0.05)" : "rgba(15, 23, 42, 0.05)";
  const textColor = isDark ? "#94a3b8" : "#64748b";

  // Build daily PnL bar data from graph_data (each point is a close event)
  const dailyMap = {};
  for (const pt of (data.graph_data || [])) {
    const d = new Date(pt.ts * 1000);
    const key = `${d.getMonth() + 1}/${d.getDate()}`;
    const pnl = pt.daily_pnl || 0;
    dailyMap[key] = (dailyMap[key] || 0) + pnl;
  }
  const labels = Object.keys(dailyMap);
  const points = Object.values(dailyMap);

  // If we have real daily_change, add today's entry
  if (data.daily_change && labels.length > 0) {
    const todayKey = (() => { const d = new Date(); return `${d.getMonth() + 1}/${d.getDate()}`; })();
    if (!dailyMap[todayKey]) {
      labels.push(todayKey);
      points.push(data.daily_change);
    }
  }

  const barColors = points.map(v => v >= 0 ? 'rgba(16, 185, 129, 0.7)' : 'rgba(239, 68, 68, 0.7)');
  const barBorders = points.map(v => v >= 0 ? '#10B981' : '#EF4444');

  if (earningsChart) {
    earningsChart.data.labels = labels;
    earningsChart.data.datasets[0].data = points;
    earningsChart.data.datasets[0].backgroundColor = barColors;
    earningsChart.data.datasets[0].borderColor = barBorders;
    earningsChart.options.scales.x.grid.color = gridColor;
    earningsChart.options.scales.y.grid.color = gridColor;
    earningsChart.options.scales.x.ticks.color = textColor;
    earningsChart.options.scales.y.ticks.color = textColor;
    earningsChart.update('none');
    return;
  }

  earningsChart = new Chart(els.chartCtx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Daily PnL',
        data: points,
        backgroundColor: barColors,
        borderColor: barBorders,
        borderWidth: 1,
        borderRadius: 4,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => ' ' + (ctx.parsed.y >= 0 ? '+' : '') + money(ctx.parsed.y)
          }
        }
      },
      scales: {
        x: {
          grid: { color: gridColor, drawBorder: false },
          ticks: { color: textColor, maxTicksLimit: 14 }
        },
        y: {
          grid: { color: gridColor, drawBorder: false },
          ticks: {
            color: textColor,
            callback: v => '$' + v.toFixed(2)
          }
        }
      }
    }
  });
}



/* ===== Event Wiring ===== */

// Feed filter tags
document.getElementById('feed-tags').addEventListener('click', (e) => {
  const btn = e.target.closest('.feed-tag');
  if (!btn) return;
  document.querySelectorAll('.feed-tag').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _feedFilter = btn.dataset.filter;
  updateFeed();
});

// Feed search — hits /api/logs for deep history when searching
let _searchTimer = null;
document.getElementById('feed-search').addEventListener('input', (e) => {
  _feedSearch = e.target.value.trim();
  clearTimeout(_searchTimer);
  if (!_feedSearch) {
    updateFeed();
    return;
  }
  // Debounce 300ms then fetch full log history from server
  _searchTimer = setTimeout(async () => {
    try {
      const res = await fetch(`/api/logs?q=${encodeURIComponent(_feedSearch)}&limit=2000`, { cache: 'no-store' });
      const data = await res.json();
      _allLogs = data.lines || [];  // already reversed server-side
    } catch (_) { }
    updateFeed();
  }, 300);
});

els.refreshBtn.addEventListener("click", () => {
  els.refreshBtn.classList.add("spinning");
  loadStatus().finally(() => setTimeout(() => els.refreshBtn.classList.remove("spinning"), 500));
});

els.themeBtn.addEventListener("click", () => {
  const isDark = document.documentElement.getAttribute("data-theme") !== "light";
  const newTheme = isDark ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", newTheme);
  localStorage.setItem("arb-theme", newTheme); // Changed to "arb-theme" to match initTheme
  els.themeIconMoon.style.display = newTheme === "dark" ? "block" : "none";
  els.themeIconSun.style.display = newTheme === "light" ? "block" : "none";
  if (lastAnalyticsData) renderAnalytics(lastAnalyticsData);
});

els.historyBtn.addEventListener("click", openHistoryModal);
els.historyCloseBtn.addEventListener("click", closeHistoryModal);

els.equityHeaderBtn.addEventListener("click", openAnalyticsModal);
els.analyticsCloseBtn.addEventListener("click", closeAnalyticsModal);

// Close modals on overlay click
document.addEventListener("click", (e) => {
  if (e.target.classList.contains("modal-overlay")) {
    if (e.target === els.historyModal) closeHistoryModal();
    if (e.target === els.analyticsModal) closeAnalyticsModal();
  }
});
document.addEventListener("keydown", (e) => { if (e.key === "Escape") { closeHistoryModal(); closeAnalyticsModal(); } });

/* ===== Theme Toggle ===== */
function initTheme() {
  const saved = localStorage.getItem("arb-theme") || "dark";
  document.documentElement.setAttribute("data-theme", saved);
  updateThemeIcon(saved);
}

function updateThemeIcon(theme) {
  if (theme === "light") {
    els.themeIconMoon.style.display = "none";
    els.themeIconSun.style.display = "block";
  } else {
    els.themeIconMoon.style.display = "block";
    els.themeIconSun.style.display = "none";
  }
}

if (els.themeBtn) {
  els.themeBtn.addEventListener("click", () => {
    const current = document.documentElement.getAttribute("data-theme");
    const next = current === "light" ? "dark" : "light";
    document.documentElement.setAttribute("data-theme", next);
    localStorage.setItem("arb-theme", next);
    updateThemeIcon(next);
    if (lastAnalyticsData) renderAnalytics(lastAnalyticsData);
  });
}
initTheme();

// ───── History Modal Logic ─────

function openHistoryModal() {
  els.historyModal.classList.add("open");
  loadHistory();
}

function closeHistoryModal() {
  els.historyModal.classList.remove("open");
}

/* ===== Analytics Modal Logic ===== */

// Snapshot today's equity into localStorage for daily tracking
function snapshotEquity(totalEquity) {
  const today = new Date().toDateString();
  const stored = JSON.parse(localStorage.getItem("equity-snapshots") || "{}");
  if (!stored[today]) {
    stored[today] = totalEquity;
    // Keep only last 3 days
    const keys = Object.keys(stored).sort((a, b) => new Date(a) - new Date(b));
    if (keys.length > 3) delete stored[keys[0]];
    localStorage.setItem("equity-snapshots", JSON.stringify(stored));
  }
}

function openAnalyticsModal() {
  els.analyticsModal.classList.add("open");

  // Server now handles equity snapshots (persisted to disk), so we just
  // use the values it returns instead of fragile localStorage lookups.
  Promise.all([
    fetch("/api/status", { cache: "no-store" }).then(r => r.json()),
    fetch("/api/analytics", { cache: "no-store" }).then(r => r.json())
  ]).then(([statusData, analyticsData]) => {
    // Use live equity from status as today_asset if server didn't provide one
    if (analyticsData.today_asset == null) {
      const exchanges = statusData.exchanges || [];
      analyticsData.today_asset = exchanges.reduce((s, e) => s + (e.starting_equity || 0) + (e.daily_pnl || 0), 0);
    }

    renderAnalytics(analyticsData);
  }).catch(err => {
    console.error("Failed to load analytics", err);
    showToast("Failed to load analytics data", "error");
  });
}

function closeAnalyticsModal() {
  els.analyticsModal.classList.remove("open");
}

function fmtTs(ts) {
  if (!ts) return "-";
  return new Date(Number(ts) * 1000).toLocaleString();
}

async function loadHistory() {
  els.historyTbody.innerHTML = `<tr><td colspan="8" style="padding:10px;color:var(--text-3); text-align:center;">Loading...</td></tr>`;
  try {
    const res = await fetch("/api/history", { cache: "no-store" });
    const data = await res.json();
    const rows = data.rows || [];
    if (!rows.length) {
      els.historyTbody.innerHTML = `<tr><td colspan="8" style="padding:10px;color:var(--text-3); text-align:center;">No history yet</td></tr>`;
      return;
    }
    els.historyTbody.innerHTML = rows
      .map((r) => {
        const pnl = Number(r.pnl || 0);
        return `
        <tr style="border-top:1px solid rgba(255,255,255,0.07);">
          <td style="padding:8px; color:var(--text-2);">${fmtTs(r.ts)}</td>
          <td style="padding:8px; color:var(--text-2);">${r.event || "-"}</td>
          <td style="padding:8px; color:var(--text);">${r.exchange || "-"}</td>
          <td style="padding:8px; color:var(--text); font-weight:bold;">${r.symbol || "-"}</td>
          <td style="padding:8px; color:var(--text-2);">${r.strategy || "-"}</td>
          <td style="padding:8px; color:var(--text); text-align:right;">${money(r.size_usd || 0)}</td>
          <td style="padding:8px; text-align:right; color:${pnl >= 0 ? "var(--good)" : "var(--bad)"};">${r.event === "CLOSE" ? money(pnl) : "-"}</td>
          <td style="padding:8px; color:var(--text-3);">${r.close_reason || "-"}</td>
        </tr>`;
      })
      .join("");
  } catch (e) {
    els.historyTbody.innerHTML = `<tr><td colspan="8" style="padding:10px;color:var(--bad); text-align:center;">Failed to load history: ${String(e)}</td></tr>`;
  }
}

if (els.historyBtn) els.historyBtn.addEventListener("click", openHistoryModal);
if (els.historyCloseBtn) els.historyCloseBtn.addEventListener("click", closeHistoryModal);

// Close modal if clicked outside
if (els.historyModal) {
  els.historyModal.addEventListener("click", (e) => {
    if (e.target === els.historyModal) closeHistoryModal();
  });
}

/* ===== Polling ===== */
async function loadStatus() {
  try {
    const res = await fetch("/api/status", { cache: "no-store" });
    const data = await res.json();
    update(data);
    // Snapshot the current equity for daily analytics tracking
    const exchanges = data.exchanges || [];
    const totalEquity = exchanges.reduce((s, e) => s + (e.starting_equity || 0) + (e.daily_pnl || 0), 0);
    if (totalEquity > 0) snapshotEquity(totalEquity);
  } catch (_) {
    els.statusPill.className = "status-pill down";
    els.statusPill.textContent = "Dashboard Offline";
  }
}

async function loadEarnLossAnalysis() {
  if (!els.elTotalPnl) return;
  try {
    const res = await fetch("/api/history", { cache: "no-store" });
    const data = await res.json();
    const summary = data.summary || {};
    const rows = data.rows || [];

    els.elTotalPnl.textContent = (summary.realized_pnl >= 0 ? '+' : '') + money(summary.realized_pnl);
    els.elTotalPnl.className = `value hero-value ${summary.realized_pnl >= 0 ? 'good' : 'bad'}`;

    els.elWins.textContent = summary.wins || 0;
    els.elLosses.textContent = summary.losses || 0;
    els.elWinrate.textContent = (summary.win_rate || 0).toFixed(2) + '%';

    // Breakdown loss reasons
    const winners = rows.filter(r => (r.event === "CLOSE" || r.event === "CLOSE_FORCED") && Number(r.pnl || 0) > 0);
    const losers = rows.filter(r => (r.event === "CLOSE" || r.event === "CLOSE_FORCED") && Number(r.pnl || 0) <= 0);
    const reasonGroups = {};
    for (const r of losers) {
      const reason = r.close_reason || "Unknown";
      if (!reasonGroups[reason]) reasonGroups[reason] = { count: 0, totalLoss: 0 };
      reasonGroups[reason].count++;
      reasonGroups[reason].totalLoss += Number(r.pnl || 0);
    }

    const sortedReasons = Object.entries(reasonGroups).sort((a, b) => a[1].totalLoss - b[1].totalLoss);

    if (sortedReasons.length === 0) {
      els.lossReasonsList.innerHTML = `<div class="feed-item" style="color:var(--text-3); text-align:center; justify-content:center;">No losses recorded yet</div>`;
    } else {
      els.lossReasonsList.innerHTML = sortedReasons.map(([reason, stats]) => `
        <div class="feed-item" style="display: flex; justify-content: space-between; align-items: center; padding: 12px 16px; margin-bottom: 8px; background: rgba(239, 68, 68, 0.05); border: 1px solid rgba(239, 68, 68, 0.1); border-radius: 10px;">
          <div class="info" style="display: flex; align-items: center; gap: 12px;">
            <div style="display: flex; flex-direction: column; gap: 4px;">
              <div class="primary" style="font-size: 14px; font-weight: 600; color: var(--text);">
                ${esc(reason)}
              </div>
              <div class="secondary" style="font-size: 12px; color: var(--text-2);">
                Occurred ${stats.count} time${stats.count !== 1 ? 's' : ''}
              </div>
            </div>
          </div>
          <div style="text-align: right;">
            <div class="value bad" style="font-family: var(--mono); font-weight: 700; font-size: 15px;">
              ${money(stats.totalLoss)}
            </div>
          </div>
        </div>
      `).join("");
    }

    // Coin Profit / Loss Ranking
    const allCloses = rows.filter(r => r.event === "CLOSE" || r.event === "CLOSE_FORCED");
    const coinGroups = {};
    for (const r of allCloses) {
      const sym = r.symbol || "Unknown";
      if (!coinGroups[sym]) coinGroups[sym] = { pnl: 0, trades: 0, exch: r.exchange || "" };
      coinGroups[sym].pnl += Number(r.pnl || 0);
      coinGroups[sym].trades++;
    }

    const sortedCoins = Object.entries(coinGroups).sort((a, b) => b[1].pnl - a[1].pnl);

    if (sortedCoins.length === 0) {
      els.coinRankingList.innerHTML = `<div class="feed-item" style="color:var(--text-3); text-align:center; justify-content:center;">No trades recorded yet</div>`;
    } else {
      els.coinRankingList.innerHTML = sortedCoins.map(([sym, stats]) => {
        const pnl = stats.pnl;
        const isGood = pnl >= 0;
        return `
          <div class="feed-item" style="display: flex; justify-content: space-between; align-items: center; padding: 12px 16px; margin-bottom: 8px; background: rgba(${isGood ? '16, 185, 129' : '239, 68, 68'}, 0.05); border: 1px solid rgba(${isGood ? '16, 185, 129' : '239, 68, 68'}, 0.1); border-radius: 10px;">
            <div class="info" style="display: flex; align-items: center; gap: 12px;">
              <div style="display: flex; flex-direction: column; gap: 4px;">
                <div class="primary" style="display: flex; align-items: center; gap: 6px; font-weight: 600; font-size: 14px; color: var(--text);">
                  <span class="exch-badge ${stats.exch}" style="font-size: 10px; padding: 2px 6px;">${stats.exch.toUpperCase()}</span>
                  ${esc(sym)}
                </div>
                <div class="secondary" style="font-size: 12px; color: var(--text-2);">
                  ${stats.trades} Trade${stats.trades !== 1 ? 's' : ''}
                </div>
              </div>
            </div>
            <div style="text-align: right;">
              <div class="value ${isGood ? 'good' : 'bad'}" style="font-family: var(--mono); font-weight: 700; font-size: 15px;">
                ${isGood ? '+' : ''}${money(pnl)}
              </div>
            </div>
          </div>
        `;
      }).join("");
    }

    // Optimization Insights Calculations
    if (els.optAvgWin) {
      let totalWinAmount = 0, winCount = 0, winDurationList = [];
      let totalLossAmount = 0, lossCount = 0, lossDurationList = [];
      let totalDuration = 0, roiSum = 0, tradesWithSize = 0;

      for (const r of allCloses) {
        const pnl = Number(r.pnl || 0);
        const duration = Number(r.hold_seconds || (r.close_time - r.entry_time) || 0);
        const size = Number(r.size_usd || 0);

        totalDuration += duration;
        if (size > 0) {
          roiSum += (pnl / size) * 100;
          tradesWithSize++;
        }

        if (pnl > 0) {
          totalWinAmount += pnl;
          winCount++;
          if (duration > 0) winDurationList.push(duration);
        } else if (pnl < 0) {
          totalLossAmount += pnl;
          lossCount++;
          if (duration > 0) lossDurationList.push(duration);
        }
      }

      const avgWin = winCount > 0 ? totalWinAmount / winCount : 0;
      const avgLoss = lossCount > 0 ? totalLossAmount / lossCount : 0;
      const avgRoi = tradesWithSize > 0 ? roiSum / tradesWithSize : 0;

      const avgHoldWin = winDurationList.length > 0 ? winDurationList.reduce((a, b) => a + b, 0) / winDurationList.length : 0;
      const avgHoldLoss = lossDurationList.length > 0 ? lossDurationList.reduce((a, b) => a + b, 0) / lossDurationList.length : 0;
      const avgHoldAll = allCloses.length > 0 ? totalDuration / allCloses.length : 0;

      // Update DOM
      els.optAvgWin.textContent = `+${money(avgWin)}`;
      els.optAvgLoss.textContent = money(avgLoss);

      els.optAvgRoi.textContent = `${avgRoi >= 0 ? '+' : ''}${avgRoi.toFixed(2)}%`;
      els.optAvgRoi.style.color = avgRoi >= 0 ? 'var(--good)' : 'var(--bad)';

      els.optHoldWins.textContent = formatDuration(avgHoldWin);
      els.optHoldLosses.textContent = formatDuration(avgHoldLoss);
      els.optHoldAll.textContent = formatDuration(avgHoldAll);

      // Color-code loss holding time to highlight if holding losing trades too long
      if (avgHoldLoss > avgHoldWin * 1.5) {
        els.optHoldLosses.style.color = 'var(--warn)';
      } else {
        els.optHoldLosses.style.color = 'var(--text)';
      }
    }
  } catch (e) {
    console.error("Failed to load earn/loss analysis", e);
  }
}

async function tick() {
  await Promise.all([
    loadStatus(),
    loadEarnLossAnalysis()
  ]);
}

tick();
setInterval(tick, 5000);
