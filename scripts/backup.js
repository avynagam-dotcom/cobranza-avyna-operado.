"use strict";

const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { spawnSync } = require("child_process");
const fs = require("fs");
const path = require("path");
try {
    require('dotenv').config();
} catch (e) {
    // Dotenv optional for Dry Run / CI
}

// Helper para asegurar logs visibles
const log = (msg) => console.log(msg);
const logError = (msg, err) => {
    console.error(msg);
    if (err) console.error(err);
};

async function runBackup() {
    log("🔵 [Backup] Iniciando respaldo...");

    try {
        const SYSTEM_NAME = process.env.SYSTEM_NAME || "avyna-desconocido";
        const R2_ENDPOINT = process.env.R2_ENDPOINT;
        const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
        const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
        const R2_BUCKET = process.env.R2_BUCKET;

        // Validate Env (Skip if Dry Run)
        if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
            if (process.env.DRY_RUN) {
                console.log("⚠️ [Dry Run] Detected missing R2 credentials. Proceeding with packaging ONLY.");
            } else {
                throw new Error("❌ FALTAN VARIABLES DE ENTORNO (R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET)");
            }
        }

        // FIX: Usar la variable exacta que ya está en Render
        const DATA_PATH = process.env.DATA_DIR || path.join(__dirname, '../data');

        console.log(`📂 [Backup Config] Buscando datos en: ${DATA_PATH}`);

        // Validación de seguridad para logs
        if (process.env.DATA_DIR) {
            console.log('✅ Modo Persistente Detectado (Render Disk)');
        } else {
            console.log('⚠️ Modo Local Detectado (Sin persistencia explicita)');
        }

        const SOURCE_ROOT = path.dirname(DATA_PATH);
        const DATA_SUBDIR = path.basename(DATA_PATH);

        // El usuario pidió explícitamente ver esta ruta
        const targetDataPath = DATA_PATH;
        log(`🔎 [Backup] Buscando archivos en ${targetDataPath}...`);

        if (!fs.existsSync(targetDataPath)) {
            console.log(`⚠️ [Backup] La carpeta ${targetDataPath} no existe. Creándola...`);
            fs.mkdirSync(targetDataPath, { recursive: true });
        }

        const date = new Date().toISOString().split("T")[0];
        const filename = `backup-${SYSTEM_NAME}-${date}.tar.gz`;
        const archivePath = path.join("/tmp", filename);

        // Validar integridad de archivos clave antes de empaquetar
        const hasNotes = fs.existsSync(path.join(targetDataPath, "notas.json"));
        const hasAudit = fs.existsSync(path.join(targetDataPath, "audit.jsonl"));
        log(`📝 [Backup] Integridad: notas.json [${hasNotes ? 'OK' : 'MISSING'}], audit.jsonl [${hasAudit ? 'OK' : 'MISSING'}]`);

        // Comprimimos 'data' y 'uploads' (si existe uploads)
        const targets = [DATA_SUBDIR];
        if (fs.existsSync(path.join(SOURCE_ROOT, "uploads"))) {
            targets.push("uploads");
            log(`📦 [Backup] Se incluirá carpeta 'uploads' (Evidencias)`);
        }

        log(`📦 [Backup] Generando archivo comprimido en ${archivePath}...`);
        const tarResult = spawnSync('tar', ['-czf', archivePath, '-C', SOURCE_ROOT, ...targets], {
            stdio: ['ignore', 'pipe', 'pipe']
        });
        if (tarResult.status !== 0) {
            const errMsg = tarResult.stderr ? tarResult.stderr.toString().trim() : 'tar command failed';
            throw new Error(`tar failed (status ${tarResult.status}): ${errMsg}`);
        }

        const stats = fs.statSync(archivePath);
        const sizeMB = (stats.size / 1024 / 1024).toFixed(2);
        log(`📦 [Backup] Archivo creado: ${sizeMB} MB (${stats.size} bytes)`);

        if (stats.size === 0) {
            throw new Error("❌ El archivo de respaldo se creó vacío (0 bytes). Abortando subida.");
        }

        // DRY RUN EXIT
        if (process.env.DRY_RUN) {
            console.log(`✅ [Dry Run] Simulation Complete. Archive ready at: ${archivePath}`);
            console.log(`🔒 [Dry Run] WOULD UPLOAD TO: ${R2_BUCKET}/${(process.env.SYSTEM_NAME || "sistema").toLowerCase()}/${filename}`);
            return;
        }

        log(`🚀 [Backup] Subiendo a Cloudflare R2 (${R2_BUCKET})...`);

        const s3 = new S3Client({
            region: "auto",
            endpoint: R2_ENDPOINT,
            credentials: {
                accessKeyId: R2_ACCESS_KEY_ID,
                secretAccessKey: R2_SECRET_ACCESS_KEY,
            },
        });

        const fileBuffer = fs.readFileSync(archivePath);
        const namespace = (process.env.SYSTEM_NAME || "sistema").toLowerCase();

        await s3.send(new PutObjectCommand({
            Bucket: R2_BUCKET,
            Key: `${namespace}/${filename}`,
            Body: fileBuffer,
            ContentType: "application/gzip",
        }));

        log(`✅ [Backup] ¡Respaldo exitoso en Cloudflare! (${namespace}/${filename})`);

        // Return ETag info if possible (S3Client doesn't easily return it in PutObject output without more config, but we can assume success)

        // Limpieza
        fs.unlinkSync(archivePath);

    } catch (error) {
        logError("🚨 ERROR CRÍTICO: BACKUP FALLIDO", error);
        throw error;
    }
}

// Si se ejecuta directo con 'node backup.js', corre la función
if (require.main === module) {
    runBackup();
}
module.exports = { runBackup };
