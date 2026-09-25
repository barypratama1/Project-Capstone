const amqp = require('amqplib');

const RABBITMQ_URL = 'amqp://localhost';
const EXCHANGE_NAME = 'billing';
const ROUTING_KEY = 'pembayaran.dikabarkan';
const QUEUE_NAME = 'rekonsiliasi';

let connection;
let channel;

async function initProducer() {
  connection = await amqp.connect(RABBITMQ_URL);
  // Gunakan ConfirmChannel untuk memastikan pesan benar-benar terkirim
  channel = await connection.createConfirmChannel();
  
  // Tangani pesan yang tidak ter-route (karena flag mandatory: true)
  channel.on('return', (msg) => {
    console.error(`[Producer ERR] Pesan tidak ter-route ke antrean mana pun! Reply Code: ${msg.fields.replyCode}, Text: ${msg.fields.replyText}`);
  });

  await channel.assertExchange(EXCHANGE_NAME, 'direct', { durable: true });
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
}

async function publishKabar(kabar) {
  if (!channel) await initProducer();
  
  const payload = Buffer.from(JSON.stringify(kabar));
  
  return new Promise((resolve, reject) => {
    console.log(`[Service Producer] Memulai publish kabar_id: ${kabar.kabar_id} (Kode: ${kabar.kode_billing}, Rp${kabar.jumlah})`);
    
    // mandatory: true memastikan jika pesan tidak memiliki route (queue tidak bind), maka akan me-return error
    channel.publish(EXCHANGE_NAME, ROUTING_KEY, payload, { persistent: true, mandatory: true }, (err, ok) => {
      if (err) {
        console.error(`[RabbitMQ] Gagal enqueue pesan ${kabar.kabar_id}`, err);
        return reject(err);
      }
      console.log(`[RabbitMQ] Pesan ${kabar.kabar_id} masuk antrean 'rekonsiliasi' (Status: Ready)`);
      resolve(ok);
    });
  });
}

async function closeProducer() {
  if (channel) await channel.close();
  if (connection) await connection.close();
}

async function checkQueueStatus() {
  if (!channel) await initProducer();
  const q = await channel.checkQueue(QUEUE_NAME);
  return q;
}

// CLI Support: memungkinkan dieksekusi langsung dari terminal
if (require.main === module) {
  const args = process.argv.slice(2);
  if (args.length >= 4) {
    const kabar = {
      kabar_id: args[0],
      kode_billing: args[1],
      sumber: args[2],
      jumlah: parseInt(args[3])
    };
    initProducer().then(async () => {
      console.log(`$ node producer.js ${args.join(' ')}`);
      await publishKabar(kabar);
      await closeProducer();
      process.exit(0);
    }).catch(console.error);
  } else {
    console.log("Usage: node producer.js <kabar_id> <kode_billing> <sumber> <jumlah>");
    process.exit(1);
  }
}

module.exports = {
  publishKabar,
  initProducer,
  closeProducer,
  checkQueueStatus
};
