const express = require('express');
const { spawn } = require('child_process');
const http = require('http');
const path = require('path');
const { pool } = require('./db');
const { publishKabar, checkQueueStatus } = require('./producer');

const app = express();
const port = 3000;

const globalLogs = [];
const originalLog = console.log;
const originalError = console.error;

function addLog(type, msg) {
  const timestamp = new Date().toISOString().split('T')[1].slice(0, 12);
  globalLogs.push({ type, timestamp, message: msg.toString().trim() });
  if (globalLogs.length > 200) globalLogs.shift();
}

console.log = function(...args) {
  addLog('info', args.join(' '));
  originalLog.apply(console, args);
};

console.error = function(...args) {
  addLog('error', args.join(' '));
  originalError.apply(console, args);
};

process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Cleanup worker on exit
function cleanup() {
  if (workerProcess && !workerProcess.killed) {
    workerProcess.kill('SIGKILL');
  }
  process.exit();
}
['SIGINT', 'SIGTERM', 'SIGQUIT'].forEach(sig => process.on(sig, cleanup));
process.on('exit', () => {
  if (workerProcess && !workerProcess.killed) {
    workerProcess.kill('SIGKILL');
  }
});

app.use(express.json());
app.use(express.static('public'));

let workerProcess = null;

// GET /api/status - Ambil status db & worker
app.get('/api/status', async (req, res) => {
  let client;
  try {
    client = await pool.connect();
    const kabarCount = await client.query('SELECT COUNT(*) FROM kabar_pembayaran');
    const lunasCount = await client.query('SELECT COUNT(*) FROM status_lunas');

    res.json({
      workerRunning: workerProcess !== null && !workerProcess.killed,
      kabarCount: parseInt(kabarCount.rows[0].count),
      lunasCount: parseInt(lunasCount.rows[0].count)
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    if (client) client.release();
  }
});

// GET /api/kabar - Ambil data terbaru dari kabar_pembayaran
app.get('/api/kabar', async (req, res) => {
  let client;
  try {
    client = await pool.connect();
    const result = await client.query(`
      SELECT kabar_id, kode_billing, sumber, jumlah, 
      TO_CHAR(waktu_mulai_eksekusi AT TIME ZONE 'Asia/Jakarta', 'YYYY-MM-DD HH24:MI:SS.MS') as waktu_mulai_eksekusi,
      TO_CHAR(waktu_selesai_eksekusi AT TIME ZONE 'Asia/Jakarta', 'YYYY-MM-DD HH24:MI:SS.MS') as waktu_selesai_eksekusi
      FROM kabar_pembayaran ORDER BY kabar_id ASC LIMIT 50
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    if (client) client.release();
  }
});

// GET /api/kabar_ditolak
app.get('/api/kabar_ditolak', async (req, res) => {
  let client;
  try {
    client = await pool.connect();
    const result = await client.query(`
      SELECT kabar_id, alasan, 
      TO_CHAR(waktu_mulai_eksekusi AT TIME ZONE 'Asia/Jakarta', 'YYYY-MM-DD HH24:MI:SS.MS') as waktu_mulai_eksekusi,
      TO_CHAR(waktu_selesai_eksekusi AT TIME ZONE 'Asia/Jakarta', 'YYYY-MM-DD HH24:MI:SS.MS') as waktu_selesai_eksekusi
      FROM kabar_ditolak ORDER BY waktu_mulai_eksekusi DESC LIMIT 50
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    if (client) client.release();
  }
});

// GET /api/queue-status
app.get('/api/queue-status', async (req, res) => {
  try {
    const q = await checkQueueStatus();
    res.json(q);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/logs
app.get('/api/logs', (req, res) => {
  res.json(globalLogs);
});

// GET /api/rabbitmq-stats
app.get('/api/rabbitmq-stats', (req, res) => {
  const options = {
    hostname: 'localhost',
    port: 15672,
    path: '/api/queues/%2F/rekonsiliasi',
    method: 'GET',
    headers: {
      'Authorization': 'Basic ' + Buffer.from('guest:guest').toString('base64')
    }
  };

  const proxyReq = http.request(options, (proxyRes) => {
    let data = '';
    proxyRes.on('data', chunk => data += chunk);
    proxyRes.on('end', () => {
      try {
        const parsed = JSON.parse(data);
        res.json({
          ready: parsed.messages_ready || 0,
          unacked: parsed.messages_unacknowledged || 0,
          total: parsed.messages || 0
        });
      } catch(e) { 
        res.json({ ready: 0, unacked: 0, total: 0 }); 
      }
    });
  });
  proxyReq.on('error', (e) => res.json({ ready: 0, unacked: 0, total: 0 }));
  proxyReq.end();
});

// GET /api/rabbitmq-messages
app.get('/api/rabbitmq-messages', (req, res) => {
  const payload = JSON.stringify({
    count: 50,
    ackmode: 'ack_requeue_true',
    encoding: 'auto',
    truncate: 50000
  });

  const options = {
    hostname: 'localhost',
    port: 15672,
    path: '/api/queues/%2F/rekonsiliasi/get',
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + Buffer.from('guest:guest').toString('base64'),
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload)
    }
  };

  const proxyReq = http.request(options, (proxyRes) => {
    let data = '';
    proxyRes.on('data', chunk => data += chunk);
    proxyRes.on('end', () => {
      try {
        const parsed = JSON.parse(data);
        res.json(parsed);
      } catch(e) { 
        res.json([]); 
      }
    });
  });
  proxyReq.on('error', (e) => res.json([]));
  proxyReq.write(payload);
  proxyReq.end();
});

// POST /api/worker/start
app.post('/api/worker/start', (req, res) => {
  if (workerProcess && !workerProcess.killed) {
    return res.json({ message: 'Worker is already running' });
  }

  workerProcess = spawn('node', [path.join(__dirname, 'worker.js')]);
  
  workerProcess.stdout.on('data', data => console.log(`[Worker] ${data}`));
  workerProcess.stderr.on('data', data => console.error(`[Worker ERR] ${data}`));
  workerProcess.on('exit', code => {
    console.log(`Worker exited with code ${code}`);
    workerProcess = null;
  });

  res.json({ message: 'Worker started' });
});

// POST /api/worker/stop
app.post('/api/worker/stop', (req, res) => {
  if (workerProcess && !workerProcess.killed) {
    workerProcess.kill('SIGINT');
    workerProcess = null;
    return res.json({ message: 'Worker stopped' });
  }
  res.json({ message: 'Worker is not running' });
});

// POST /api/publish
app.post('/api/publish', async (req, res) => {
  try {
    const { kabar_id, kode_billing, sumber, jumlah } = req.body;
    await publishKabar({ kabar_id, kode_billing, sumber, jumlah: parseInt(jumlah) });
    res.json({ message: `Message ${kabar_id} published` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Skenario API
app.post('/api/test/u1', async (req, res) => {
  try {
    const { run_id } = req.body;
    for (let i = 1; i <= 20; i++) {
      const id = i.toString().padStart(2, '0');
      const sumber = i % 2 === 0 ? 'rekap' : 'langsung';
      const randomJumlah = (Math.floor(Math.random() * 90) + 10) * 10000;
      await publishKabar({ kabar_id: `${run_id}-N${id}`, kode_billing: `BIL-${run_id}-${id}`, sumber, jumlah: randomJumlah });
    }
    res.json({ message: 'U1 Sent (20 messages N01-N20)' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/test/u2', async (req, res) => {
  try {
    const { run_id } = req.body;
    for (let i = 1; i <= 5; i++) {
      const id = i.toString().padStart(2, '0');
      await publishKabar({ kabar_id: `${run_id}-G${id}`, kode_billing: `BIL-${run_id}-G${id}`, sumber: 'rekap', jumlah: 200000 });
    }
    res.json({ message: 'U2 Sent (5 messages G01-G05)' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/test/u3', async (req, res) => {
  try {
    const { run_id } = req.body;
    for (let i = 1; i <= 5; i++) {
      const id = i.toString().padStart(2, '0');
      const sumber = i % 2 === 0 ? 'rekap' : 'langsung';
      await publishKabar({ kabar_id: `${run_id}-N${id}`, kode_billing: `BIL-${run_id}-${id}`, sumber, jumlah: 150000 });
    }
    res.json({ message: 'U3 Sent (Replay N01-N05)' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/test/u4', async (req, res) => {
  try {
    const { run_id } = req.body;
    await publishKabar({ kabar_id: `${run_id}-X01`, sumber: 'rekap', jumlah: 150000 }); // missing kode_billing
    await publishKabar({ kabar_id: `${run_id}-V01`, kode_billing: `BIL-${run_id}-V01`, sumber: 'langsung', jumlah: 100000 });
    res.json({ message: 'U4 Sent (X01 and V01)' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/reset', async (req, res) => {
  let client;
  try {
    globalLogs.length = 0; // Clear logs on reset
    client = await pool.connect();
    await client.query('TRUNCATE TABLE kabar_pembayaran, status_lunas, kabar_ditolak');
    res.json({ message: 'Database reset successfully' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    if (client) client.release();
  }
});

app.listen(port, () => {
  console.log(`Dashboard listening at http://localhost:${port}`);
});
