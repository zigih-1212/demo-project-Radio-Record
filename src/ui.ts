// Inline dashboard. IMPORTANT: this string is inside a TS template literal,
// so the HTML/JS below must NOT contain backticks or "${" sequences.
export const APP_HTML = String.raw`<!doctype html>
<html lang="ru"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>PulseOps — фоновые задачи</title>
<style>
:root{--bg:#0e1420;--panel:#17202f;--panel2:#1d2940;--line:#2a3852;--text:#e8edf7;--muted:#8b98b3;--accent:#4f7cff;--ok:#2fbf8f;--warn:#e8b13f;--err:#e35d6a;--info:#4f7cff}
*{box-sizing:border-box}body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--text)}
header{display:flex;align-items:center;gap:12px;padding:14px 22px;background:var(--panel);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:5}
header h1{font-size:17px;margin:0;font-weight:700;letter-spacing:.3px}
.badge{font-size:11px;font-weight:700;padding:3px 9px;border-radius:999px;background:var(--panel2);border:1px solid var(--line);color:var(--muted)}
.badge.demo{color:var(--warn);border-color:var(--warn)}
.dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:5px}
.dot.ok{background:var(--ok)}.dot.bad{background:var(--err)}
.spacer{flex:1}.btn{border:0;border-radius:8px;padding:8px 14px;font-weight:600;font-size:13px;cursor:pointer;background:var(--accent);color:#fff}
.btn.ghost{background:var(--panel2);color:var(--text);border:1px solid var(--line)}
.btn.danger{background:#3a1d24;color:var(--err);border:1px solid #5a2a33}
.btn.sm{padding:5px 9px;font-size:12px}
main{max-width:1180px;margin:22px auto;padding:0 16px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px;margin-bottom:16px}
.stats{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:16px}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px 14px}
.stat .n{font-size:22px;font-weight:700}.stat .l{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-top:2px}
.toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px}
select,input[type=text],input[type=email],input[type=password]{background:var(--panel2);border:1px solid var(--line);color:var(--text);border-radius:8px;padding:8px 10px;font-size:13px}
input[type=text]{min-width:200px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:9px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
th{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.5px}
tr:hover td{background:var(--panel2)}
.pill{display:inline-block;padding:3px 9px;border-radius:999px;font-size:11px;font-weight:700}
.p-queued{background:#3a3320;color:var(--warn)}.p-processing{background:#1c2a4a;color:#7ea2ff}
.p-succeeded{background:#17382c;color:var(--ok)}.p-failed{background:#3d1f24;color:var(--err)}.p-cancelled{background:#2a2f3a;color:var(--muted)}
.pager{display:flex;gap:10px;align-items:center;justify-content:flex-end;margin-top:10px;font-size:12px;color:var(--muted)}
.drawer{position:fixed;top:0;right:0;bottom:0;width:min(460px,94vw);background:var(--panel);border-left:1px solid var(--line);padding:20px;overflow:auto;transform:translateX(105%);transition:transform .18s ease;z-index:20}
.drawer.open{transform:none}
.drawer h2{font-size:15px;margin:0 0 4px}.drawer .sub{color:var(--muted);font-size:12px;margin-bottom:14px}
.timeline{list-style:none;margin:14px 0 0;padding:0}.timeline li{border-left:2px solid var(--line);padding:6px 0 6px 12px;margin-left:6px;font-size:12px}
.timeline li b{display:block;font-size:12px}.timeline li span{color:var(--muted)}
.modal{position:fixed;inset:0;background:rgba(5,8,14,.7);display:none;place-items:center;z-index:30}
.modal.open{display:grid}.modal .box{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:20px;width:min(440px,92vw)}
.modal label{display:block;font-size:12px;color:var(--muted);margin:10px 0 4px}
.modal input,.modal select{width:100%}
.modal .row{display:flex;gap:10px;justify-content:flex-end;margin-top:16px}
.check{display:flex;gap:8px;align-items:center;margin-top:10px;font-size:13px}
#toasts{position:fixed;bottom:18px;right:18px;display:flex;flex-direction:column;gap:8px;z-index:40}
.toast{background:var(--panel2);border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:10px;padding:10px 14px;font-size:13px;max-width:340px}
.toast.ok{border-left-color:var(--ok)}.toast.err{border-left-color:var(--err)}
#loginCard{max-width:380px;margin:60px auto;text-align:center}
#loginCard h2{margin:0 0 14px}#loginCard input{width:100%;margin-bottom:10px}
#loginHint{font-size:12px;color:var(--muted);margin-top:10px}
.hide{display:none!important}
</style></head><body>
<header>
  <h1>PulseOps</h1>
  <span class="badge" id="modeBadge">…</span>
  <span id="healthWrap"><span class="dot ok" id="healthDot"></span><span id="healthText" style="font-size:12px;color:var(--muted)">health</span></span>
  <span class="spacer"></span>
  <span class="badge" id="userBadge">не авторизован</span>
  <button class="btn ghost sm" id="logoutBtn" style="display:none">Выйти</button>
</header>
<main>
  <div class="card" id="loginCard">
    <h2>Вход в PulseOps</h2>
    <input type="email" id="loginEmail" placeholder="email"/>
    <input type="password" id="loginPassword" placeholder="пароль"/>
    <button class="btn" id="loginBtn" style="width:100%">Войти</button>
    <div id="loginHint">Демо-админ: admin@demo.local / admin-demo-123</div>
  </div>

  <div id="app" class="hide">
    <div class="stats" id="stats"></div>
    <div class="card">
      <div class="toolbar">
        <select id="fStatus"><option value="">Все статусы</option><option value="queued">queued</option><option value="processing">processing</option><option value="succeeded">succeeded</option><option value="failed">failed</option><option value="cancelled">cancelled</option></select>
        <select id="fType"><option value="">Все типы</option><option value="campaign_publish">campaign_publish</option><option value="sync_catalog">sync_catalog</option><option value="send_digest">send_digest</option><option value="import_orders">import_orders</option></select>
        <select id="fPriority"><option value="">Любой приоритет</option><option value="high">high</option><option value="normal">normal</option><option value="low">low</option></select>
        <input type="text" id="fSearch" placeholder="Поиск по названию…"/>
        <button class="btn ghost sm" id="resetBtn">Сброс</button>
        <span class="spacer"></span>
        <button class="btn sm" id="newJobBtn">+ Новая задача</button>
      </div>
      <table><thead><tr><th>Задача</th><th>Тип</th><th>Приоритет</th><th>Статус</th><th>Попытки</th><th>Обновлено</th><th></th></tr></thead>
      <tbody id="rows"></tbody></table>
      <div class="pager"><button class="btn ghost sm" id="prevBtn">← Назад</button><span id="pageInfo"></span><button class="btn ghost sm" id="nextBtn">Вперёд →</button></div>
    </div>
  </div>
</main>

<div class="drawer" id="drawer">
  <button class="btn ghost sm" id="drawerClose" style="float:right">✕</button>
  <h2 id="dTitle"></h2><div class="sub" id="dMeta"></div>
  <pre id="dPayload" style="white-space:pre-wrap;font-size:12px;color:var(--muted)"></pre>
  <h3 style="font-size:13px;margin:14px 0 6px">События</h3>
  <ul class="timeline" id="dEvents"></ul>
  <div style="display:flex;gap:8px;margin-top:16px">
    <button class="btn sm" id="dRetry">Повторить</button>
    <button class="btn danger sm" id="dCancel">Отменить</button>
  </div>
</div>

<div class="modal" id="modal"><div class="box">
  <h3 style="margin:0 0 4px">Новая задача</h3>
  <label>Название</label><input type="text" id="mTitle"/>
  <label>Тип</label><select id="mType"><option value="campaign_publish">Публикация кампании</option><option value="sync_catalog">Синхронизация каталога</option><option value="send_digest">Email-дайджест</option><option value="import_orders">Импорт заказов</option></select>
  <label>Приоритет</label><select id="mPriority"><option value="low">Низкий</option><option value="normal" selected>Обычный</option><option value="high">Высокий</option></select>
  <label class="check"><input type="checkbox" id="mFail"/> Смоделировать ошибку (проверка retry/backoff/DLQ)</label>
  <div class="row"><button class="btn ghost" id="mCancel">Отмена</button><button class="btn" id="mCreate">Создать</button></div>
</div></div>

<div id="toasts"></div>
<script>
var token = localStorage.getItem('pulseops.token') || '';
var state = { items: [], total: 0, limit: 10, offset: 0, status: '', type: '', priority: '', q: '' };

function toast(msg, kind) {
  var t = document.createElement('div');
  t.className = 'toast' + (kind ? ' ' + kind : '');
  t.textContent = msg;
  document.getElementById('toasts').appendChild(t);
  setTimeout(function () { t.remove(); }, 4200);
}
async function api(path, opts) {
  opts = opts || {};
  opts.headers = opts.headers || {};
  opts.headers['Authorization'] = 'Bearer ' + token;
  if (opts.body) opts.headers['Content-Type'] = 'application/json';
  var r = await fetch(path, opts);
  var body = null;
  try { body = await r.json(); } catch (e) { /* ignore */ }
  if (r.status === 401) { showLogin(); throw new Error('Необходима авторизация'); }
  if (!r.ok) throw new Error((body && body.error) || ('HTTP ' + r.status));
  return body;
}
async function refreshHealth() {
  try {
    var h = await fetch('/api/health').then(function (r) { return r.json(); });
    document.getElementById('healthDot').className = 'dot ' + (h.ok ? 'ok' : 'bad');
    document.getElementById('healthText').textContent = h.ok ? 'сервисы OK' : 'проблемы с зависимостями';
    var mb = document.getElementById('modeBadge');
    mb.textContent = h.mode === 'demo' ? 'DEMO MODE' : 'DOCKER';
    mb.className = 'badge' + (h.mode === 'demo' ? ' demo' : '');
  } catch (e) { /* ignore */ }
}
async function refreshStats() {
  var s = await api('/api/stats');
  var labels = { queued: 'В очереди', processing: 'В работе', succeeded: 'Успех', failed: 'Ошибки', cancelled: 'Отменено', total: 'Всего' };
  var html = '';
  ['queued', 'processing', 'succeeded', 'failed', 'cancelled', 'total'].forEach(function (k) {
    html += '<div class="stat"><div class="n">' + (s[k] || 0) + '</div><div class="l">' + labels[k] + '</div></div>';
  });
  document.getElementById('stats').innerHTML = html;
}
function esc(v) { return String(v == null ? '' : v).replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); }
function fmtTs(v) { if (!v) return '—'; return new Date(v).toLocaleString('ru-RU'); }
function pill(st) { return '<span class="pill p-' + st + '">' + st + '</span>'; }
async function refreshJobs() {
  var p = new URLSearchParams();
  if (state.status) p.set('status', state.status);
  if (state.type) p.set('type', state.type);
  if (state.priority) p.set('priority', state.priority);
  if (state.q) p.set('q', state.q);
  p.set('limit', String(state.limit));
  p.set('offset', String(state.offset));
  var data = await api('/api/jobs?' + p.toString());
  state.items = data.items || [];
  state.total = data.total || 0;
  renderTable();
}
function renderTable() {
  var html = '';
  state.items.forEach(function (j) {
    html += '<tr data-id="' + j.id + '"><td><b>' + esc(j.title) + '</b><br/><span style="color:var(--muted);font-size:11px">' + esc(j.id.slice(0, 8)) + '</span></td>'
      + '<td>' + esc(j.job_type) + '</td><td>' + esc(j.priority) + '</td><td>' + pill(j.status) + '</td>'
      + '<td>' + j.attempts + '/' + j.max_attempts + '</td><td style="font-size:12px;color:var(--muted)">' + fmtTs(j.updated_at) + '</td>'
      + '<td style="white-space:nowrap"><button class="btn ghost sm" onclick="openDetail(\'' + j.id + '\')">Детали</button></td></tr>';
  });
  document.getElementById('rows').innerHTML = html || '<tr><td colspan="7" style="text-align:center;color:var(--muted)">Нет задач</td></tr>';
  var pages = Math.max(1, Math.ceil(state.total / state.limit));
  var cur = Math.floor(state.offset / state.limit) + 1;
  document.getElementById('pageInfo').textContent = 'стр. ' + cur + ' / ' + pages + ' (' + state.total + ')';
  document.getElementById('prevBtn').disabled = state.offset === 0;
  document.getElementById('nextBtn').disabled = state.offset + state.limit >= state.total;
}
async function openDetail(id) {
  var d = await api('/api/jobs/' + id);
  var j = d.job;
  document.getElementById('dTitle').textContent = j.title;
  document.getElementById('dMeta').textContent = j.job_type + ' · ' + j.priority + ' · ' + pill(j.status) + ' · попытки ' + j.attempts + '/' + j.max_attempts;
  document.getElementById('dPayload').textContent = JSON.stringify(j.payload, null, 2);
  var ev = '';
  (d.events || []).forEach(function (e) {
    ev += '<li><b>' + esc(e.event_type) + '</b><span>' + fmtTs(e.created_at) + (e.message ? ' — ' + esc(e.message) : '') + '</span></li>';
  });
  document.getElementById('dEvents').innerHTML = ev || '<li>Нет событий</li>';
  document.getElementById('dRetry').onclick = function () { api('/api/jobs/' + id + '/retry', { method: 'POST' }).then(function () { toast('Задача возвращена в очередь', 'ok'); closeDrawer(); reload(); }).catch(function (e) { toast(e.message, 'err'); }); };
  document.getElementById('dCancel').onclick = function () { api('/api/jobs/' + id + '/cancel', { method: 'POST' }).then(function () { toast('Задача отменена', 'ok'); closeDrawer(); reload(); }).catch(function (e) { toast(e.message, 'err'); }); };
  document.getElementById('drawer').classList.add('open');
}
function closeDrawer() { document.getElementById('drawer').classList.remove('open'); }
function reload() { refreshStats().then(refreshJobs).catch(function (e) { toast(e.message, 'err'); }); }

function showLogin() {
  token = '';
  localStorage.removeItem('pulseops.token');
  document.getElementById('app').classList.add('hide');
  document.getElementById('loginCard').classList.remove('hide');
  document.getElementById('userBadge').textContent = 'не авторизован';
  document.getElementById('logoutBtn').style.display = 'none';
}
async function doLogin() {
  var email = document.getElementById('loginEmail').value.trim();
  var password = document.getElementById('loginPassword').value;
  try {
    var r = await fetch('/api/auth/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: email, password: password }) });
    var body = await r.json().catch(function () { return {}; });
    if (!r.ok) throw new Error(body.error || ('HTTP ' + r.status));
    token = body.accessToken;
    localStorage.setItem('pulseops.token', token);
    document.getElementById('userBadge').textContent = body.user.email + ' (' + body.user.role + ')';
    document.getElementById('logoutBtn').style.display = '';
    document.getElementById('loginCard').classList.add('hide');
    document.getElementById('app').classList.remove('hide');
    toast('Вход выполнен', 'ok');
    reload();
  } catch (e) { toast(e.message, 'err'); }
}
async function doLogout() {
  try { await api('/api/auth/logout', { method: 'POST' }); } catch (e) { /* ignore */ }
  showLogin();
}

document.getElementById('loginBtn').onclick = doLogin;
document.getElementById('logoutBtn').onclick = doLogout;
document.getElementById('fStatus').onchange = function (e) { state.status = e.target.value; state.offset = 0; refreshJobs(); };
document.getElementById('fType').onchange = function (e) { state.type = e.target.value; state.offset = 0; refreshJobs(); };
document.getElementById('fPriority').onchange = function (e) { state.priority = e.target.value; state.offset = 0; refreshJobs(); };
document.getElementById('fSearch').oninput = function (e) { state.q = e.target.value; state.offset = 0; refreshJobs(); };
document.getElementById('resetBtn').onclick = function () {
  state.status = state.type = state.priority = state.q = ''; state.offset = 0;
  document.getElementById('fStatus').value = ''; document.getElementById('fType').value = '';
  document.getElementById('fPriority').value = ''; document.getElementById('fSearch').value = '';
  refreshJobs();
};
document.getElementById('prevBtn').onclick = function () { if (state.offset > 0) { state.offset -= state.limit; refreshJobs(); } };
document.getElementById('nextBtn').onclick = function () { state.offset += state.limit; refreshJobs(); };
document.getElementById('drawerClose').onclick = closeDrawer;
document.getElementById('newJobBtn').onclick = function () { document.getElementById('modal').classList.add('open'); };
document.getElementById('mCancel').onclick = function () { document.getElementById('modal').classList.remove('open'); };
document.getElementById('mCreate').onclick = async function () {
  var body = {
    title: document.getElementById('mTitle').value.trim(),
    type: document.getElementById('mType').value,
    priority: document.getElementById('mPriority').value,
    simulateFailure: document.getElementById('mFail').checked
  };
  if (body.title.length < 3) { toast('Название — минимум 3 символа', 'err'); return; }
  try {
    var r = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token, 'Idempotency-Key': (crypto.randomUUID ? crypto.randomUUID() : String(Date.now())) },
      body: JSON.stringify(body)
    });
    var data = await r.json().catch(function () { return {}; });
    if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
    document.getElementById('modal').classList.remove('open');
    document.getElementById('mTitle').value = '';
    toast(data.idempotent ? 'Задача уже существует (идемпотентность)' : 'Задача создана', 'ok');
    reload();
  } catch (e) { toast(e.message, 'err'); }
};

if (token) { api('/api/auth/me').then(function (me) {
  document.getElementById('userBadge').textContent = me.user.email + ' (' + me.user.role + ')';
  document.getElementById('loginCard').classList.add('hide');
  document.getElementById('app').classList.remove('hide');
  document.getElementById('logoutBtn').style.display = '';
  reload();
}).catch(showLogin); }

refreshHealth();
setInterval(refreshHealth, 10000);
setInterval(function () { if (token) reload(); }, 5000);
</script></body></html>`;
