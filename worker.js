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
      if (!kabar_id || !kode_billing) {
        console.error('Invalid message format (missing kabar_id or kode_billing):', payload);
        
        if (kabar_id) {
          const waktuSelesai = new Date();
          const client = await pool.connect();
          try {
            await client.query(`
              INSERT INTO kabar_ditolak (kabar_id, alasan, waktu_mulai_eksekusi, waktu_selesai_eksekusi)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT DO NOTHING
            `, [kabar_id, 'missing kode_billing', waktuMulai, waktuSelesai]);
          } catch (e) {
            console.error('Failed to log rejected message', e);
          } finally {
            client.release();
          }
        }
        
        channel.ack(msg); // ack it to remove from queue
        return;
      }

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        
        // Simpan kabar apa adanya (deduplikasi dengan kabar_id menggunakan ON CONFLICT DO NOTHING)
        const waktuSelesai = new Date();
        await client.query(`
          INSERT INTO kabar_pembayaran (kabar_id, kode_billing, sumber, jumlah, waktu_mulai_eksekusi, waktu_selesai_eksekusi) 
          VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (kabar_id) DO NOTHING
        `, [kabar_id, kode_billing, sumber, jumlah, waktuMulai, waktuSelesai]);

        // Simpulkan status lunas (pastikan tidak ganda dengan ON CONFLICT DO NOTHING)
        await client.query(`
          INSERT INTO status_lunas (kode_billing) 
          VALUES ($1)
          ON CONFLICT (kode_billing) DO NOTHING
        `, [kode_billing]);

        await client.query('COMMIT');
        console.log(`[x] Processed ${kabar_id} for ${kode_billing} from ${sumber}`);
        channel.ack(msg);
      } catch (err) {
        await client.query('ROLLBACK');
        console.error(`[!] Error processing ${kabar_id}:`, err);
        // Requeue for retry
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
