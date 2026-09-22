const { Pool } = require('pg');

const pool = new Pool({
  user: 'user',
  host: 'localhost',
  database: 'billing_db',
  password: 'password',
  port: 5432,
});

pool.on('error', (err, client) => {
  console.error('Unexpected error on idle client', err);
});

async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS kabar_pembayaran (
        kabar_id VARCHAR(255) PRIMARY KEY,
        kode_billing VARCHAR(255) NOT NULL,
        sumber VARCHAR(255) NOT NULL,
        jumlah NUMERIC NOT NULL,
        waktu_mulai_eksekusi TIMESTAMPTZ,
        waktu_selesai_eksekusi TIMESTAMPTZ
      );
    `);

    // Tambahkan kolom baru untuk backward compatibility
    await client.query(`
      ALTER TABLE kabar_pembayaran 
      ADD COLUMN IF NOT EXISTS waktu_mulai_eksekusi TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS waktu_selesai_eksekusi TIMESTAMPTZ;
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS kabar_ditolak (
        kabar_id VARCHAR(255) PRIMARY KEY,
        alasan VARCHAR(255) NOT NULL,
        waktu_mulai_eksekusi TIMESTAMPTZ,
        waktu_selesai_eksekusi TIMESTAMPTZ
      );
    `);

    // Tambahkan kolom baru untuk backward compatibility
    await client.query(`
      ALTER TABLE kabar_ditolak 
      ADD COLUMN IF NOT EXISTS waktu_mulai_eksekusi TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS waktu_selesai_eksekusi TIMESTAMPTZ;
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS status_lunas (
        kode_billing VARCHAR(255) PRIMARY KEY
      );
    `);
    
    console.log('Database initialized successfully.');
  } catch (err) {
    console.error('Error initializing database', err);
  } finally {
    client.release();
  }
}

module.exports = {
  pool,
  initDB
};
