const amqp = require('amqplib');
const { pool, initDB } = require('./db');

process.on('uncaughtException', (err) => {
  console.error('[Worker] Uncaught Exception:', err);
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('[Worker] Unhandled Rejection at:', promise, 'reason:', reason);
});

const RABBITMQ_URL = 'amqp://localhost';
const EXCHANGE_NAME = 'billing';
const QUEUE_NAME = 'rekonsiliasi';
const ROUTING_KEY = 'pembayaran.dikabarkan';

async function startWorker() {
  await initDB();

  let connection;
  try {
    connection = await amqp.connect(RABBITMQ_URL);
  } catch (error) {
    console.error('Failed to connect to RabbitMQ', error);
    process.exit(1);
  }

  const channel = await connection.createChannel();
  
  await channel.assertExchange(EXCHANGE_NAME, 'direct', { durable: true });
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

  console.log(`[*] Waiting for messages in ${QUEUE_NAME}. To exit press CTRL+C`);

  channel.consume(QUEUE_NAME, async (msg) => {
    if (msg !== null) {
      const waktuMulai = new Date();
      const payloadString = msg.content.toString();
      let payload;
      
      try {
        payload = JSON.parse(payloadString);
      } catch (err) {
        console.error('Failed to parse message:', payloadString);
        channel.nack(msg, false, false);
        return;
      }

      const { kabar_id, kode_billing, sumber, jumlah } = payload;
      console.log(`[Service Consumer] Menerima pesan kabar_id: ${kabar_id} (Status: Unacked)`);

      if (!kabar_id || !kode_billing) {
        console.error(`[Service Consumer] Invalid message format:`, payload);
        
        if (kabar_id) {
          const waktuSelesai = new Date();
          const client = await pool.connect();
          try {
            await client.query(`
              INSERT INTO kabar_ditolak (kabar_id, alasan, waktu_mulai_eksekusi, waktu_selesai_eksekusi)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT DO NOTHING
            `, [kabar_id, 'missing kode_billing', waktuMulai, waktuSelesai]);
            console.log(`[PostgreSQL] INSERT kabar_ditolak (${kabar_id})`);
          } catch (e) {
            console.error(`[PostgreSQL] Failed to log rejected message`, e);
          } finally {
            client.release();
          }
        }
        
        console.log(`[RabbitMQ] Ack pesan invalid (Dihapus dari antrean)`);
        channel.ack(msg);
        return;
      }

      const client = await pool.connect();
      try {
        console.log(`[PostgreSQL] BEGIN transaksi untuk ${kabar_id}`);
        await client.query('BEGIN');
        
        const waktuSelesai = new Date();
        await client.query(`
          INSERT INTO kabar_pembayaran (kabar_id, kode_billing, sumber, jumlah, waktu_mulai_eksekusi, waktu_selesai_eksekusi) 
          VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (kabar_id) DO NOTHING
        `, [kabar_id, kode_billing, sumber, jumlah, waktuMulai, waktuSelesai]);
        console.log(`[PostgreSQL] INSERT kabar_pembayaran (${kabar_id})`);

        await client.query(`
          INSERT INTO status_lunas (kode_billing) 
          VALUES ($1)
          ON CONFLICT (kode_billing) DO NOTHING
        `, [kode_billing]);
        console.log(`[PostgreSQL] INSERT status_lunas (${kode_billing})`);

        await client.query('COMMIT');
        console.log(`[PostgreSQL] COMMIT transaksi sukses`);
        
        console.log(`[Service Consumer] Selesai memproses ${kabar_id}`);
        console.log(`[RabbitMQ] Ack pesan ${kabar_id} (Dihapus dari antrean)`);
        channel.ack(msg);
      } catch (err) {
        await client.query('ROLLBACK');
        console.error(`[PostgreSQL] Error processing ${kabar_id}, ROLLBACK:`, err.message);
        console.log(`[RabbitMQ] Nack pesan ${kabar_id} (Requeue)`);
        channel.nack(msg, false, true);
      } finally {
        client.release();
      }
    }
  }, { noAck: false });
}

if (require.main === module) {
  startWorker();
}

module.exports = { startWorker };
