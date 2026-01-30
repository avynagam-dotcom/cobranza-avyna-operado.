"use strict";

const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

async function runBackup() {
    const PREFIX = "operado"; // ✅ Fixed prefix as requested
    const R2_ENDPOINT = process.env.R2_ENDPOINT;
    const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
    const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
    const R2_BUCKET = process.env.R2_BUCKET;

    // Carpeta de datos a respaldar (la del disco persistente)
    const DATA_DIR = "/var/data/cobranza";
    // Si no existe el disco persistente, intentamos la local (desarrollo)
    const SOURCE_DIR = fs.existsSync(DATA_DIR) ? DATA_DIR : path.join(__dirname, "..");

    if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
        console.warn("[Backup] ⚠️ Faltan credenciales R2. Saltando backup.");
        return; // Don't throw, just skip nicely if env is missing, or throw if critical? User said "no detenga el proceso". Warning is better for auto-backup.
    }

    const date = new Date().toISOString().split("T")[0];
    const filename = `backup-${date}.tar.gz`; // Clean filename
    const key = `${PREFIX}/${filename}`;
    const archivePath = path.join("/tmp", filename);

    try {
        console.log(`[Backup] ⏳ Iniciando respaldo para: ${PREFIX}`);
        console.log(`[Backup] 📂 Origen: ${SOURCE_DIR}`);

        // Comprimimos data y uploads si existen en el SOURCE_DIR
        const targets = [];
        if (fs.existsSync(path.join(SOURCE_DIR, "data"))) targets.push("data");
        if (fs.existsSync(path.join(SOURCE_DIR, "uploads"))) targets.push("uploads");

        if (targets.length === 0) {
            console.warn("[Backup] ⚠️ No hay carpetas 'data' o 'uploads' para respaldar en " + SOURCE_DIR);
            return;
        }

        console.log(`[Backup] 📦 Comprimiendo: ${targets.join(", ")} en ${archivePath}`);
        // cd SOURCE_DIR to avoid full paths in tar
        execSync(`tar -czf ${archivePath} -C ${SOURCE_DIR} ${targets.join(" ")}`);

        console.log(`[Backup] 🚀 Subiendo a Cloudflare R2: ${R2_BUCKET} -> ${key}`);
        const s3 = new S3Client({
            region: "auto",
            endpoint: R2_ENDPOINT,
            credentials: {
                accessKeyId: R2_ACCESS_KEY_ID,
                secretAccessKey: R2_SECRET_ACCESS_KEY,
            },
        });

        const fileBuffer = fs.readFileSync(archivePath);
        await s3.send(new PutObjectCommand({
            Bucket: R2_BUCKET,
            Key: key,
            Body: fileBuffer,
            ContentType: "application/gzip",
        }));

        console.log(`[Backup] ✅ ÉXITO. Backup guardado: ${key}`);

    } catch (error) {
        console.error(`[Backup] ❌ ERROR CRÍTICO:`, error.message);
        // Not re-throwing implies we swallow the error for the main process?
        // User said "que no detenga el proceso sin avisar".
        // If I swallow it, server.js won't see it (except in logs).
        // server.js expects to catch it. I should re-throw so server.js knows it failed, 
        // OR return false. 
        // But server.js uses .catch().
        // Let's rethrow but ensure we logged it well.
        throw error;
    } finally {
        // Limpieza
        if (fs.existsSync(archivePath)) {
            fs.unlinkSync(archivePath);
        }
    }
}

module.exports = runBackup;

if (require.main === module) {
    runBackup().catch((e) => {
        console.error("Manual Backup Failed:", e.message);
        process.exit(1);
    });
}
