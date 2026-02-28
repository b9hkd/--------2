const express = require('express');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
const initSqlJs = require('sql.js');

const app = express();
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'crm_orders.sqlite');
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret-change-me';
const PORT = process.env.PORT || 8002;

const uploadDir = process.env.UPLOAD_TMP || path.join(__dirname, 'uploads_tmp');
fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({ dest: uploadDir });

app.use(cors());
app.use(express.json({ limit: '2mb' }));
app.use(express.static(__dirname));
app.get('/', (_req, res) => res.redirect('/dashboard.html'));

let SQL;
let db;

async function loadDb() {
  if (!SQL) {
    SQL = await initSqlJs({
      locateFile: (file) => require.resolve('sql.js/dist/' + file),
    });
  }
  if (!db) {
    const buf = fs.existsSync(DB_PATH) ? fs.readFileSync(DB_PATH) : null;
    db = buf ? new SQL.Database(new Uint8Array(buf)) : new SQL.Database();
  }
}

function saveDb() {
  if (!db) return;
  const data = db.export();
  fs.writeFileSync(DB_PATH, Buffer.from(data));
}

async function run(sql, params = [], persist = true) {
  await loadDb();
  const stmt = db.prepare(sql);
  stmt.run(params);
  stmt.free();
  if (persist) saveDb();
}

async function get(sql, params = []) {
  await loadDb();
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const row = stmt.step() ? stmt.getAsObject() : null;
  stmt.free();
  return row;
}

async function all(sql, params = []) {
  await loadDb();
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

async function initDb() {
  await loadDb();
  await run(
    `CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      login TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK(role IN ('owner','admin','manager','employee')),
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );`
  );

  await run(
    `CREATE TABLE IF NOT EXISTS audit (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,
      user TEXT,
      role TEXT,
      action TEXT NOT NULL,
      detail TEXT
    );`
  );

  // Ensure orders has meta columns
  await run(`ALTER TABLE orders ADD COLUMN added_by TEXT`, []).catch(() => {});
  await run(`ALTER TABLE orders ADD COLUMN added_at TEXT`, []).catch(() => {});

  const defaults = [
    { login: 'root', pass: 'sunroot123', role: 'owner' },
    { login: 'admin', pass: 'sunadmin123', role: 'admin' },
    { login: 'manager', pass: 'sunmanager123', role: 'manager' },
    { login: 'user', pass: 'sunuser123', role: 'employee' },
  ];
  for (const u of defaults) {
    const exists = await get('SELECT id FROM users WHERE login = ?', [u.login]);
    if (!exists) {
      const hash = bcrypt.hashSync(u.pass, 10);
      await run(
        'INSERT INTO users (login, password_hash, role) VALUES (?, ?, ?)',
        [u.login, hash, u.role]
      );
    }
  }
}

const sanitizeLogin = (v) => typeof v === 'string' && /^[A-Za-z0-9_.-]{3,32}$/.test(v);
const sanitizePassword = (v) => typeof v === 'string' && v.length >= 6 && v.length <= 64 && !/[\"'`;]/.test(v);
const toISODate = (val) => {
  if (!val) return null;
  const d = val instanceof Date ? val : new Date(val);
  return Number.isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
};

const auth = async (req, res, next) => {
  const header = req.headers.authorization || '';
  const token = header.startsWith('Bearer ') ? header.slice(7) : null;
  if (!token) return res.status(401).json({ error: 'unauthorized' });
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    req.user = payload;
    next();
  } catch {
    return res.status(401).json({ error: 'unauthorized' });
  }
};

const requireRole = (roles) => (req, res, next) => {
  if (!req.user || !roles.includes(req.user.role)) return res.status(403).json({ error: 'forbidden' });
  next();
};

async function logAction(user, role, action, detail = '') {
  await run('INSERT INTO audit (ts, user, role, action, detail) VALUES (?, ?, ?, ?, ?)', [
    new Date().toISOString(),
    user || 'guest',
    role || 'guest',
    action,
    detail,
  ]).catch(() => {});
}

async function importExcel(filePath, addedBy) {
  const wb = XLSX.readFile(filePath, { cellDates: true });
  const sheet = wb.SheetNames[0];
  if (!sheet) throw new Error('Нет листов в файле');
  const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheet], { defval: '' });
  if (!rows.length) throw new Error('Пустой файл');

  const needed = ['ID', 'Дата заказа', 'Клиент', 'Тип клиента', 'Город', 'Состав заказа', 'Тип заказа', 'Менеджер', 'Источник заявки', 'Сумма заказа'];
  const missing = needed.filter((c) => !(c in rows[0]));
  if (missing.length) throw new Error('Отсутствуют колонки: ' + missing.join(', '));

  const parsed = rows.map((r, idx) => {
    const id = Number(r['ID']);
    const order_amount = Number(r['Сумма заказа']);
    const order_date = toISODate(r['Дата заказа']);
    if (!Number.isInteger(id)) throw new Error(`Строка ${idx + 2}: ID не число`);
    if (!order_date) throw new Error(`Строка ${idx + 2}: некорректная дата`);
    if (!Number.isFinite(order_amount)) throw new Error(`Строка ${idx + 2}: сумма не число`);
    return {
      id,
      order_date,
      client: String(r['Клиент'] || '').trim(),
      customer_type: String(r['Тип клиента'] || '').trim() || 'Частное лицо',
      city: String(r['Город'] || '').trim(),
      order_items: String(r['Состав заказа'] || '').trim(),
      order_type: String(r['Тип заказа'] || '').trim(),
      manager: String(r['Менеджер'] || '').trim() || '—',
      lead_source: String(r['Источник заявки'] || '').trim(),
      order_amount,
      added_by: addedBy,
      added_at: new Date().toISOString(),
    };
  });

  // sql.js no transactions; emulate by saving once after loop
  await loadDb();
  for (const row of parsed) {
    const stmt = db.prepare(
      `INSERT INTO orders (id, order_date, client, customer_type, city, order_items,
                           order_type, manager, lead_source, order_amount, added_by, added_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET
         order_date=excluded.order_date,
         client=excluded.client,
         customer_type=excluded.customer_type,
         city=excluded.city,
         order_items=excluded.order_items,
         order_type=excluded.order_type,
         manager=excluded.manager,
         lead_source=excluded.lead_source,
         order_amount=excluded.order_amount,
         added_by=excluded.added_by,
         added_at=excluded.added_at`
    );
    stmt.run([
      row.id,
      row.order_date,
      row.client,
      row.customer_type,
      row.city,
      row.order_items,
      row.order_type,
      row.manager,
      row.lead_source,
      row.order_amount,
      row.added_by,
      row.added_at,
    ]);
    stmt.free();
  }
  saveDb();
  return parsed.length;
}

// Auth
app.post('/api/login', async (req, res) => {
  const { login, password } = req.body || {};
  if (!sanitizeLogin(login) || !sanitizePassword(password)) {
    return res.status(400).json({ error: 'invalid credentials format' });
  }
  const user = await get('SELECT * FROM users WHERE login = ?', [login.toLowerCase()]).catch(() => null);
  if (!user) return res.status(401).json({ error: 'invalid credentials' });
  const ok = bcrypt.compareSync(password, user.password_hash);
  if (!ok) return res.status(401).json({ error: 'invalid credentials' });
  const token = jwt.sign({ id: user.id, login: user.login, role: user.role }, JWT_SECRET, { expiresIn: '8h' });
  await logAction(user.login, user.role, 'login');
  res.json({ token, user: { id: user.id, login: user.login, role: user.role } });
});

app.get('/api/me', auth, async (req, res) => {
  const user = await get('SELECT id, login, role, created_at FROM users WHERE id = ?', [req.user.id]).catch(() => null);
  if (!user) return res.status(401).json({ error: 'invalid session' });
  res.json(user);
});

// Orders
app.get('/api/orders', auth, async (req, res) => {
  const rows = await all(
    `SELECT id, order_date, client, customer_type, city, order_items,
            order_type, manager, lead_source, order_amount,
            added_by, added_at
     FROM orders
     ORDER BY order_date DESC, id DESC`
  ).catch(() => []);
  res.json(rows);
});

app.post('/api/orders', auth, requireRole(['owner', 'admin', 'manager', 'employee']), async (req, res) => {
  const {
    client = '',
    city = '',
    order_items = '',
    order_type = '',
    order_amount,
    lead_source = '',
    order_date,
  } = req.body || {};
  const errors = [];
  if (!client.trim()) errors.push('client');
  if (!city.trim()) errors.push('city');
  if (!order_date) errors.push('order_date');
  const amount = Number(order_amount);
  if (!Number.isFinite(amount) || amount <= 0) errors.push('order_amount');
  if (errors.length) return res.status(400).json({ error: 'invalid_fields', fields: errors });

  const added_at = new Date().toISOString();
  await run(
    `INSERT INTO orders (order_date, client, customer_type, city, order_items,
                         order_type, manager, lead_source, order_amount, added_by, added_at)
     VALUES (?, ?, 'Частное лицо', ?, ?, ?, '—', ?, ?, ?, ?)`,
    [
      order_date,
      client.trim(),
      city.trim(),
      order_items.trim() || '—',
      order_type.trim() || 'из наличия',
      lead_source.trim() || 'Не указано',
      amount,
      req.user.login,
      added_at,
    ]
  );
  const row = await get(
    `SELECT id, order_date, client, customer_type, city, order_items,
            order_type, manager, lead_source, order_amount, added_by, added_at
     FROM orders ORDER BY id DESC LIMIT 1`
  );
  await logAction(req.user.login, req.user.role, 'add_order', `id=${row.id}`);
  res.json(row);
});

app.delete('/api/orders/:id', auth, requireRole(['owner']), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'bad id' });
  await run('DELETE FROM orders WHERE id = ?', [id]);
  await logAction(req.user.login, req.user.role, 'delete_order', `id=${id}`);
  res.json({ ok: true });
});

// Users (admin/owner)
app.get('/api/users', auth, requireRole(['owner', 'admin']), async (req, res) => {
  const users = await all('SELECT id, login, role, created_at FROM users ORDER BY id ASC').catch(() => []);
  res.json(users);
});

app.post('/api/users', auth, requireRole(['owner', 'admin']), async (req, res) => {
  const { login, password, role } = req.body || {};
  if (!sanitizeLogin(login) || !sanitizePassword(password) || !['owner', 'admin', 'manager', 'employee'].includes(role || '')) {
    return res.status(400).json({ error: 'invalid_fields' });
  }
  const exists = await get('SELECT id FROM users WHERE login = ?', [login]).catch(() => null);
  if (exists) return res.status(409).json({ error: 'login_exists' });
  const hash = bcrypt.hashSync(password, 10);
  await run('INSERT INTO users (login, password_hash, role) VALUES (?, ?, ?)', [login, hash, role]);
  await logAction(req.user.login, req.user.role, 'add_user', login);
  const user = await get('SELECT id, login, role, created_at FROM users WHERE login = ?', [login]);
  res.status(201).json(user);
});

app.delete('/api/users/:login', auth, requireRole(['owner', 'admin']), async (req, res) => {
  const login = req.params.login;
  if (login === 'root') return res.status(400).json({ error: 'cannot_delete_root' });
  await run('DELETE FROM users WHERE login = ?', [login]);
  await logAction(req.user.login, req.user.role, 'delete_user', login);
  res.json({ ok: true });
});

// Audit
app.get('/api/audit', auth, requireRole(['owner', 'admin']), async (req, res) => {
  const rows = await all('SELECT ts, user, role, action, detail FROM audit ORDER BY ts DESC LIMIT 200').catch(() => []);
  res.json(rows);
});

// Upload stub (accepts file, no processing)
app.post('/api/upload', auth, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'file_required' });
  try {
    const imported = await importExcel(req.file.path, req.user.login);
    await logAction(req.user.login, req.user.role, 'upload_file', `${req.file.originalname} rows=${imported}`);
    res.json({ ok: true, imported });
  } catch (err) {
    return res.status(400).json({ error: err.message });
  } finally {
    fs.unlink(req.file.path, () => {});
  }
});

// Fallback for SPA
app.get('/api/health', (_req, res) => res.json({ ok: true }));

initDb().then(() => {
  app.listen(PORT, () => console.log(`API listening on http://localhost:${PORT}`));
});
