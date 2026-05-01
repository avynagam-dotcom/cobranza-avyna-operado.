const cron = require('node-cron');
const { runBackup } = require('../scripts/backup');

function cleanOrphanedUploads(dataDir, uploadsDir) {
    try {
        const fs = require('fs');
        const path = require('path');
        const notasPath = path.join(dataDir, 'notas.json');
        if (!fs.existsSync(notasPath) || !fs.existsSync(uploadsDir)) return;

        const notas = JSON.parse(fs.readFileSync(notasPath, 'utf8') || '[]');
        const knownFiles = new Set(Array.isArray(notas) ? notas.map(n => n.filename).filter(Boolean) : []);
        const files = fs.readdirSync(uploadsDir);
        let cleaned = 0;
        for (const f of files) {
            if (!knownFiles.has(f)) {
                try {
                    fs.unlinkSync(path.join(uploadsDir, f));
                    cleaned++;
                } catch (_) {}
            }
        }
        if (cleaned > 0) console.log(`[Cleanup] ${cleaned} PDF(s) huérfano(s) eliminado(s)`);
    } catch (e) {
        console.error('[Cleanup] Error en limpieza de uploads:', e.message);
    }
}

function initScheduler({ dataDir, uploadsDir } = {}) {
    const fs = require('fs');
    const path = require('path');
    const DATA = dataDir || process.env.DATA_DIR || path.join(__dirname, '../data');
    const UPLOADS = uploadsDir || path.join(path.dirname(DATA), 'uploads');

    const SYSTEM_NAME = process.env.SYSTEM_NAME || 'Unknown System';
    console.log(`⏰ [Scheduler] [${SYSTEM_NAME}] Iniciando cron jobs...`);

    // 9:00 AM UTC (3:00 AM CDMX aprox) — backup diario + cleanup
    cron.schedule('0 9 * * *', async () => {
        console.log(`⏰ [Scheduler] [${SYSTEM_NAME}] Backup automático...`);
        try {
            await runBackup();
        } catch (err) {
            console.error(`❌ [Scheduler] [${SYSTEM_NAME}] Error en backup:`, err);
        }
        cleanOrphanedUploads(DATA, UPLOADS);
    });

    // 1ro de mes a media noche — snapshot mensual (bóveda)
    cron.schedule('0 0 1 * *', () => {
        console.log(`📅 [Scheduler] [${SYSTEM_NAME}] Snapshot mensual...`);
        try {
            const VAULT = path.join(DATA, 'backups/historicos');
            if (!fs.existsSync(VAULT)) fs.mkdirSync(VAULT, { recursive: true });

            const src = path.join(DATA, 'notas.json');
            if (fs.existsSync(src)) {
                const dest = path.join(VAULT, `${new Date().toISOString().slice(0, 7)}_notas.json`);
                fs.copyFileSync(src, dest);
                console.log(`✅ [VAULT] Snapshot guardado: ${dest}`);
            }
        } catch (e) {
            console.error('❌ [VAULT] Error en snapshot:', e);
        }
    });
}

module.exports = { initScheduler };
