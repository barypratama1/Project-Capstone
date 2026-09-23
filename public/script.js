document.addEventListener('DOMContentLoaded', () => {
    
    // Element References
    const elKabarCount = document.getElementById('kabarCount');
    const elLunasCount = document.getElementById('lunasCount');
    const elWorkerStatusText = document.getElementById('workerStatusText');
    const elWorkerStatusIndicator = document.getElementById('workerStatusIndicator');
    const toastContainer = document.getElementById('toastContainer');
    
    // Controls
    const btnStartWorker = document.getElementById('btnStartWorker');
    const btnStopWorker = document.getElementById('btnStopWorker');
    const btnReset = document.getElementById('btnReset');

    // Scenarios
    const btnU1 = document.getElementById('btnU1');
    const resU1 = document.getElementById('resU1');
    
    const btnU2_stop = document.getElementById('btnU2_stop');
    const btnU2_pub = document.getElementById('btnU2_pub');
    const btnU2_start = document.getElementById('btnU2_start');
    const resU2 = document.getElementById('resU2');
    
    const btnU3 = document.getElementById('btnU3');
    const resU3 = document.getElementById('resU3');

    const btnU4 = document.getElementById('btnU4');
    const resU4 = document.getElementById('resU4');

    let run_id = localStorage.getItem('run_id') || `RUN-${Math.floor(Date.now() / 1000)}`;

    // Fetch Status Loop
    async function fetchStatus() {
        try {
            const res = await fetch('/api/status');
            const data = await res.json();
            
            elKabarCount.textContent = data.kabarCount;
            elLunasCount.textContent = data.lunasCount;
            
            if (data.workerRunning) {
                elWorkerStatusText.textContent = "Worker Online";
                elWorkerStatusIndicator.className = "dot online";
                btnStartWorker.disabled = true;
                btnStopWorker.disabled = false;
            } else {
                elWorkerStatusText.textContent = "Worker Offline";
                elWorkerStatusIndicator.className = "dot offline";
                btnStartWorker.disabled = false;
                btnStopWorker.disabled = true;
            }
            
            // Table Kabar Masuk
            const resKabar = await fetch('/api/kabar');
            const dataKabar = await resKabar.json();
            const tableBody = document.getElementById('tableBody');
            if (dataKabar.length === 0) {
                tableBody.innerHTML = '<tr><td colspan="6" class="text-center">Belum ada data</td></tr>';
            } else {
                tableBody.innerHTML = dataKabar.map(item => `
                    <tr>
                        <td>${item.kabar_id}</td>
                    <td>${item.kode_billing}</td>
                    <td><span class="badge badge-${item.sumber}">${item.sumber}</span></td>
                    <td class="text-right">Rp ${parseInt(item.jumlah).toLocaleString('id-ID')}</td>
                    <td>${item.waktu_mulai_eksekusi || '-'}</td>
                    <td>${item.waktu_selesai_eksekusi || '-'}</td>
                </tr>
                `).join('');
            }

            // Table Kabar Ditolak
            const resDitolak = await fetch('/api/kabar_ditolak');
            const dataDitolak = await resDitolak.json();
            const tableBodyDitolak = document.getElementById('tableBodyDitolak');
            if (dataDitolak.length === 0) {
                tableBodyDitolak.innerHTML = '<tr><td colspan="4" class="text-center">Belum ada data ditolak</td></tr>';
            } else {
                tableBodyDitolak.innerHTML = dataDitolak.map(item => `
                <tr>
                    <td>${item.kabar_id}</td>
                    <td><span style="color: #ef4444; font-weight: bold;">${item.alasan}</span></td>
                    <td>${item.waktu_mulai_eksekusi || '-'}</td>
                    <td>${item.waktu_selesai_eksekusi || '-'}</td>
                </tr>
                `).join('');
            }

            // RMQ Live Stats
            const resRmq = await fetch('/api/rabbitmq-stats');
            const dataRmq = await resRmq.json();
            document.getElementById('rmqReady').textContent = dataRmq.ready;
            document.getElementById('rmqUnacked').textContent = dataRmq.unacked;

            // Logs
            const resLogs = await fetch('/api/logs');
            const dataLogs = await resLogs.json();
            renderLogs(dataLogs);
            
            // Auto-fetch Antrean RabbitMQ
            const resMsg = await fetch('/api/rabbitmq-messages');
            const messages = await resMsg.json();
            const tableBodyAntrean = document.getElementById('tableBodyAntrean');
            if (tableBodyAntrean) {
                if (!Array.isArray(messages) || messages.length === 0) {
                    tableBodyAntrean.innerHTML = '<tr><td colspan="4" class="text-center">Antrean kosong</td></tr>';
                } else {
                    tableBodyAntrean.innerHTML = messages.map(msg => {
                        let payload = {};
                        try {
                            payload = JSON.parse(msg.payload);
                        } catch(e) {}
                        return `
                        <tr>
                            <td>${payload.kabar_id || '-'}</td>
                            <td>${payload.kode_billing || '-'}</td>
                            <td>${payload.sumber || '-'}</td>
                            <td class="text-right">Rp ${parseInt(payload.jumlah || 0).toLocaleString('id-ID')}</td>
                        </tr>
                        `;
                    }).join('');
                }
            }

            // Alerts
            const alertContainer = document.getElementById('alertContainer');
            if (alertContainer) {
                let alertHtml = '';
                if (!data.workerRunning) {
                    alertHtml += `<div class="alert-banner warning">⚠️ Worker sedang offline! Pesan akan menumpuk di antrean.</div>`;
                }
                if (dataRmq.ready > 0 || dataRmq.unacked > 0) {
                    alertHtml += `<div class="alert-banner">🚨 Peringatan: Ada ${dataRmq.ready + dataRmq.unacked} pesan yang belum sukses diproses oleh consumer!</div>`;
                }
                alertContainer.innerHTML = alertHtml;
            }

        } catch (err) {
            console.error('Error fetching status:', err);
        }
    }

    const terminalBody = document.getElementById('terminalBody');
    let lastLogCount = 0;

    function renderLogs(logs) {
        if (!terminalBody) return;
        if (logs.length === 0 && lastLogCount > 0) {
            terminalBody.innerHTML = '<div class="log-line system"><span class="log-info">Menunggu aktivitas sistem...</span></div>';
            lastLogCount = 0;
            return;
        }
        if (logs.length === lastLogCount) return;
        
        const isScrolledToBottom = terminalBody.scrollHeight - terminalBody.clientHeight <= terminalBody.scrollTop + 10;
        
        terminalBody.innerHTML = logs.map(log => {
            let colorClass = log.type === 'error' ? 'log-error' : 'log-info';
            let message = log.message;
            if (message.includes('[Service Consumer]')) colorClass = 'log-consumer';
            if (message.includes('[Service Producer]')) colorClass = 'log-producer';
            if (message.includes('[PostgreSQL]')) colorClass = 'log-postgres';
            if (message.includes('[RabbitMQ]')) colorClass = 'log-rabbitmq';
            
            return `<div class="log-line"><span class="log-time">[${log.timestamp}]</span><span class="${colorClass}">${message.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</span></div>`;
        }).join('');
        
        if (isScrolledToBottom || logs.length !== lastLogCount) {
            terminalBody.scrollTop = terminalBody.scrollHeight;
        }
        lastLogCount = logs.length;
    }

    // Export Log to TXT
    const btnExportLog = document.getElementById('btnExportLog');
    if (btnExportLog) {
        btnExportLog.addEventListener('click', () => {
            if (!terminalBody) return;
            const logLines = Array.from(terminalBody.querySelectorAll('.log-line'))
                                  .map(line => line.innerText || line.textContent)
                                  .join('\n');
            const blob = new Blob([logLines], { type: 'text/plain' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `Pipeline_Activity_Log_${new Date().toISOString().slice(0,10)}.txt`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        });
    }

    // Auto-fetch interval
    setInterval(fetchStatus, 1000);
    fetchStatus();

    function showToast(message) {
        const t = document.createElement('div');
        t.className = 'toast';
        t.textContent = message;
        toastContainer.appendChild(t);
        setTimeout(() => {
            if(toastContainer.contains(t)) toastContainer.removeChild(t);
        }, 3000);
    }

    const wait = (ms) => new Promise(res => setTimeout(res, ms));

    // Basic Controls
    btnStartWorker.addEventListener('click', async () => {
        await fetch('/api/worker/start', { method: 'POST' });
        showToast("Worker started");
        fetchStatus();
    });

    btnStopWorker.addEventListener('click', async () => {
        await fetch('/api/worker/stop', { method: 'POST' });
        showToast("Worker stopped");
        fetchStatus();
    });

    btnReset.addEventListener('click', async () => {
        if(confirm("Apakah Anda yakin ingin mereset database dan run_id?")) {
            await fetch('/api/reset', { method: 'POST' });
            run_id = `RUN-${Math.floor(Date.now() / 1000)}`;
            localStorage.setItem('run_id', run_id);
            resU1.innerHTML = "Menunggu...";
            resU2.innerHTML = "Menunggu...";
            resU3.innerHTML = "Menunggu...";
            resU4.innerHTML = "Menunggu...";
            btnU2_stop.disabled = false;
            btnU2_pub.disabled = true;
            btnU2_start.disabled = true;
            showToast("Database reset");
            fetchStatus();
        }
    });

    // SCENARIOS
    // U1
    btnU1.addEventListener('click', async () => {
        resU1.innerHTML = `<span style="color: #d97706; font-weight: 600;">Processing U1...</span>`;
        
        // Pastikan reset awal agar hitungannya pasti 20
        await fetch('/api/reset', { method: 'POST' });
        
        // Pastikan worker menyala
        await fetch('/api/worker/start', { method: 'POST' });
        
        await fetch('/api/test/u1', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ run_id })
        });
        
        // Wait for processing with polling up to 6 seconds
        let success = false;
        let finalLunasCount = 0;
        for (let i = 0; i < 6; i++) {
            await wait(1000);
            const res = await fetch('/api/status');
            const data = await res.json();
            finalLunasCount = data.lunasCount;
            if (data.kabarCount === 20 && data.lunasCount === 20) {
                success = true;
                break;
            }
        }
        
        if (success) {
            resU1.innerHTML = `<span style="color: #10b981">Sukses (U1): ${finalLunasCount} hasil unik.</span>`;
        } else {
            resU1.innerHTML = `<span style="color: #ef4444">Gagal: Diharapkan 20, DB memiliki ${finalLunasCount}</span>`;
        }
    });

    // U2
    btnU2_stop.addEventListener('click', async () => {
        await fetch('/api/worker/stop', { method: 'POST' });
        btnU2_stop.disabled = true;
        btnU2_pub.disabled = false;
        resU2.innerHTML = `<span style="color: #d97706; font-weight: 600;">Consumer dihentikan. Siap menembak G01-G05.</span>`;
    });

    btnU2_pub.addEventListener('click', async () => {
        await fetch('/api/test/u2', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ run_id })
        });
        await wait(500); // give broker time to enqueue
        
        const qRes = await fetch('/api/queue-status');
        const qData = await qRes.json();
        
        if (qData.messageCount >= 5) {
            resU2.innerHTML = `<span style="color: #d97706; font-weight: 600;">Bukti: ${qData.messageCount} pesan menunggu di antrean 'rekonsiliasi'. Siap dipulihkan.</span>`;
            btnU2_pub.disabled = true;
            btnU2_start.disabled = false;
        } else {
            resU2.innerHTML = `<span style="color: #ef4444">Gagal: Pesan tidak menunggu di queue! (count: ${qData.messageCount})</span>`;
        }
    });

    btnU2_start.addEventListener('click', async () => {
        await fetch('/api/worker/start', { method: 'POST' });
        resU2.innerHTML = `<span style="color: #d97706; font-weight: 600;">Consumer pulih. Memproses queue...</span>`;
        
        await wait(2000);
        const res = await fetch('/api/status');
        const data = await res.json();
        
        // Expected count after U1 (20) + U2 (5) = 25
        if (data.kabarCount === 25 && data.lunasCount === 25) {
            resU2.innerHTML = `<span style="color: #10b981">Sukses (U2): Kelima ID asli selesai tanpa dikirim manual. (Total lunas: 25)</span>`;
            btnU2_start.disabled = true;
            btnU2_stop.disabled = false; // reset state
        } else {
            resU2.innerHTML = `<span style="color: #ef4444">Gagal: Jumlah akhir lunas ${data.lunasCount} (Expected: 25)</span>`;
        }
    });

    // U3
    btnU3.addEventListener('click', async () => {
        resU3.innerHTML = `<span style="color: #d97706; font-weight: 600;">Processing U3 (Replay N01-N05)...</span>`;
        await fetch('/api/test/u3', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ run_id })
        });
        
        let success = false;
        let finalLunasCount = 0;
        for (let i = 0; i < 4; i++) {
            await wait(1000);
            const res = await fetch('/api/status');
            const data = await res.json();
            finalLunasCount = data.lunasCount;
            if (data.kabarCount === 25 && data.lunasCount === 25) {
                success = true;
                break;
            }
        }
        
        if (success) {
            resU3.innerHTML = `<span style="color: #10b981">Sukses (U3): Efek bisnis tidak bertambah. Hasil tetap 25.</span>`;
        } else {
            resU3.innerHTML = `<span style="color: #ef4444">Gagal: DB terubah. Lunas: ${finalLunasCount} (Expected: 25)</span>`;
        }
    });

    // U4
    btnU4.addEventListener('click', async () => {
        resU4.innerHTML = `<span style="color: #d97706; font-weight: 600;">Processing U4...</span>`;
        await fetch('/api/test/u4', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ run_id })
        });
        
        let success = false;
        let finalLunasCount = 0;
        for (let i = 0; i < 4; i++) {
            await wait(1000);
            const res = await fetch('/api/status');
            const data = await res.json();
            finalLunasCount = data.lunasCount;
            // Expected after U1(20) + U2(5) + U4(1 valid) = 26
            if (data.kabarCount === 26 && data.lunasCount === 26) {
                success = true;
                break;
            }
        }
        
        if (success) {
            resU4.innerHTML = `<span style="color: #10b981">Sukses (U4): X01 ditolak, V01 diproses. Total lunas: 26.</span>`;
        } else {
            resU4.innerHTML = `<span style="color: #ef4444">Gagal: Total Lunas: ${finalLunasCount} (Expected: 26)</span>`;
        }
    });

});
