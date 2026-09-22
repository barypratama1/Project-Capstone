const { spawn } = require('child_process');
const { initDB, pool } = require('./db');
const { publishKabar, closeProducer } = require('./producer');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function runTest() {
  console.log('--- Initializing Database ---');
  await initDB();
  
  // Bersihkan tabel untuk test
  const client = await pool.connect();
  await client.query('TRUNCATE TABLE kabar_pembayaran, status_lunas');
  client.release();

  console.log('--- Publishing 20 Initial Messages (10 pairs) ---');
  // 10 pairs = 20 messages
  for (let i = 1; i <= 10; i++) {
    const kodeBilling = `BIL-${i.toString().padStart(4, '0')}`;
    const randomJumlah = (Math.floor(Math.random() * 90) + 10) * 10000; // Nilai random kelipatan 10.000 antara 100.000 - 1.000.000
    if (i % 2 === 0) {
      // Rekap lalu langsung
      await publishKabar({ kabar_id: `KAB-R-${i}`, kode_billing: kodeBilling, sumber: 'rekap', jumlah: randomJumlah });
      await publishKabar({ kabar_id: `KAB-L-${i}`, kode_billing: kodeBilling, sumber: 'langsung', jumlah: randomJumlah });
    } else {
      // Langsung lalu rekap
      await publishKabar({ kabar_id: `KAB-L-${i}`, kode_billing: kodeBilling, sumber: 'langsung', jumlah: randomJumlah });
      await publishKabar({ kabar_id: `KAB-R-${i}`, kode_billing: kodeBilling, sumber: 'rekap', jumlah: randomJumlah });
    }
  }

  // Publish X01 (Invalid Message)
  console.log('--- Publishing X01 (Invalid Message) ---');
  await publishKabar({ kabar_id: `X01`, sumber: 'rekap', jumlah: 150000 }); // missing kode_billing

  console.log('--- Starting Worker 1 ---');
  let worker1 = spawn('node', ['worker.js']);
  worker1.stdout.on('data', data => console.log(`[Worker 1] ${data.toString().trim()}`));
  worker1.stderr.on('data', data => console.error(`[Worker 1 ERR] ${data.toString().trim()}`));

  await delay(3000); // Wait for worker 1 to process 21 messages

  console.log('--- Stopping Worker 1 ---');
  worker1.kill('SIGINT');
  await delay(1000);

  console.log('--- Publishing G01-G05 while worker is down ---');
  for (let i = 1; i <= 5; i++) {
    await publishKabar({ kabar_id: `G0${i}`, kode_billing: `BIL-G0${i}`, sumber: 'rekap', jumlah: 200000 });
  }

  console.log('--- Publishing 1 more to reach 26 total valid ---');
  await publishKabar({ kabar_id: `KAB-26`, kode_billing: `BIL-0026`, sumber: 'langsung', jumlah: 100000 });

  console.log('--- Restarting Worker (Pemulihan) ---');
  let worker2 = spawn('node', ['worker.js']);
  worker2.stdout.on('data', data => console.log(`[Worker 2] ${data.toString().trim()}`));
  worker2.stderr.on('data', data => console.error(`[Worker 2 ERR] ${data.toString().trim()}`));

  await delay(2000);

  console.log('--- Simulating Replay (Duplikasi Pesan) ---');
  await publishKabar({ kabar_id: `G01`, kode_billing: `BIL-G01`, sumber: 'rekap', jumlah: 200000 }); // Replay G01
  await publishKabar({ kabar_id: `KAB-R-2`, kode_billing: `BIL-0002`, sumber: 'rekap', jumlah: 150000 }); // Replay KAB-R-2

  await delay(3000);

  console.log('--- Verifying Results in DB ---');
  const dbClient = await pool.connect();
  const kabarRes = await dbClient.query('SELECT COUNT(*) FROM kabar_pembayaran');
  const kabarTotal = parseInt(kabarRes.rows[0].count);
  const lunasRes = await dbClient.query('SELECT COUNT(DISTINCT kode_billing) as count FROM status_lunas');
  const lunasTotal = parseInt(lunasRes.rows[0].count);

  console.log('\n=== TEST RESULTS ===');
  console.log(`Jumlah catatan kabar_pembayaran unik: ${kabarTotal} (Expected: 26)`);
  console.log(`Jumlah catatan status_lunas (kode_billing unik): ${lunasTotal} (Expected: 16)`);

  if (kabarTotal === 26 && lunasTotal === 16) {
    console.log('✅ TEST PASSED');
  } else {
    console.error('❌ TEST FAILED');
  }

  dbClient.release();

  console.log('--- Cleaning Up ---');
  worker2.kill('SIGINT');
  await delay(1000);
  await closeProducer();
  await pool.end();
  process.exit(0);
}

runTest().catch(console.error);
