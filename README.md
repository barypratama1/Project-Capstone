# Implementasi Rekonsiliasi Kode Billing

Proyek ini adalah implementasi sistem rekonsiliasi kode billing (pembayaran) menggunakan **Node.js**, **PostgreSQL**, dan **RabbitMQ**. Sistem ini menerima informasi atau kabar pembayaran (pesan) melalui API, memasukkannya ke dalam antrean (Message Broker), dan memprosesnya menggunakan worker (konsumen) secara asinkron untuk mencatat status pelunasan.

## Arsitektur & Teknologi Utama
- **Node.js & Express**: Digunakan untuk API Server dan Dashboard.
- **PostgreSQL**: Database relasional untuk menyimpan data kabar pembayaran, status lunas, dan kabar yang ditolak.
- **RabbitMQ**: Message broker (antrean pesan) untuk mengelola antrean pemrosesan rekonsiliasi secara asinkron (menggunakan `amqplib`).
- **Docker Compose**: Digunakan untuk mempermudah setup database Postgres dan RabbitMQ secara lokal.

## Prasyarat
Sebelum menjalankan aplikasi, pastikan Anda telah menginstal:
- [Node.js](https://nodejs.org/) (versi 16 atau lebih baru direkomendasikan)
- [Docker](https://www.docker.com/) & Docker Compose (untuk menjalankan Postgres & RabbitMQ)

## Instalasi dan Setup

1. **Clone repository ini** (jika belum):
   ```bash
   git clone <url-repo-anda>
   cd "Project Capstone"
   ```

2. **Instal dependensi Node.js**:
   ```bash
   npm install
   ```

3. **Jalankan layanan infrastruktur (Database & Message Broker)**:
   Proyek ini sudah dilengkapi dengan file `docker-compose.yml`. Jalankan perintah berikut untuk menghidupkan PostgreSQL dan RabbitMQ di latar belakang:
   ```bash
   docker-compose up -d
   ```
   *(Tunggu beberapa saat hingga container siap).*

## Cara Menjalankan Aplikasi

1. **Jalankan API Server**:
   ```bash
   node server.js
   ```
   Server akan berjalan dan dapat diakses di `http://localhost:3000`. Server ini secara otomatis akan menginisialisasi tabel database (lewat `db.js`) pada saat diakses, atau Anda bisa memanggil endpoint jika dibutuhkan.

2. **Jalankan Worker (Consumer)**:
   Worker digunakan untuk memproses pesan yang masuk ke RabbitMQ. Anda bisa menjalankannya lewat dashboard API:
   - Hit endpoint: `POST http://localhost:3000/api/worker/start`
   - Atau jalankan secara manual di terminal terpisah: `node worker.js`

## Daftar API Endpoints (Skenario Utama)

Aplikasi ini menyediakan berbagai API untuk pengujian skenario rekonsiliasi.

### API Worker & Status
- `GET /api/status` : Melihat status worker dan jumlah data.
- `GET /api/kabar` : Melihat 50 data terbaru dari `kabar_pembayaran`.
- `GET /api/kabar_ditolak` : Melihat daftar kabar pembayaran yang ditolak.
- `GET /api/queue-status` : Mengecek status antrean di RabbitMQ.
- `POST /api/worker/start` : Menjalankan worker process.
- `POST /api/worker/stop` : Menghentikan worker process.
- `POST /api/reset` : Menghapus/mengosongkan seluruh isi tabel di database.

### API Publishing & Testing
- `POST /api/publish` : Mengirim 1 data kabar pembayaran secara manual.
  - Body: `{ "kabar_id": "...", "kode_billing": "...", "sumber": "...", "jumlah": 10000 }`
- `POST /api/test/u1` : Mengirim 20 pesan (N01-N20).
- `POST /api/test/u2` : Mengirim 5 pesan (G01-G05).
- `POST /api/test/u3` : Replay pengiriman N01-N05.
- `POST /api/test/u4` : Mengirim skenario pesan tanpa kode billing (ditolak).

## Struktur Database
Sistem ini menggunakan 3 tabel utama (dibuat secara otomatis oleh `db.js`):
1. `kabar_pembayaran`: Menyimpan histori pesan/kabar pembayaran yang masuk dan diproses.
2. `status_lunas`: Mencatat daftar `kode_billing` yang pembayarannya telah dinyatakan sukses/lunas.
7. `kabar_ditolak`: Mencatat daftar pembayaran yang gagal diproses beserta alasannya (contoh: tidak ada kode billing).

## Aturan Bisnis (Rules)
- Simpan tiap kabar apa adanya dengan `kabar_id` sebagai kunci unik, lalu turunkan status lunas per `kode_billing` di tabel terpisah. Satu tabel merekam yang datang, satu menyimpulkan keadaannya.
- Deduplikasi memakai `kabar_id`, bukan `kode_billing`. Kode yang sama dari dua sumber adalah dua kabar sah; yang tidak boleh ganda adalah status lunasnya.
- Fixture memuat pasangan rekap lalu langsung dan pasangan langsung lalu rekap untuk kode berbeda, sehingga hasil terbukti tidak bergantung urutan.

## Kontribusi & Pengembangan
Aplikasi ini dikembangkan untuk menguji kasus skenario (Scenario Testing) tentang bagaimana antrean pembayaran ditangani (Producer & Consumer) dan dicatat ke dalam database untuk rekonsiliasi yang konsisten.
