"use strict";

const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

// Validado de Beluga/Mars -> Operado
async function runBackup() {
    const PREFIX = "operado";
    const R2_ENDPOINT = process.env.R2_ENDPOINT;
    const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
    const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
    const R2_BUCKET = process.env.R2_BUCKET;

    // Usar la variable de entorno validada por persistence.js
    const DATA_DIR = process.env.DATA_DIR;

    if (!DATA_DIR) {
        console.error("[Backup] ❌ ERROR: process.env.DATA_DIR no está definido.");
        return;
    }

    if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
        console.warn("[Backup] ⚠️ Faltan credenciales R2. Saltando backup.");
        return;
    }

    const date = new Date().toISOString().split("T")[0];
    const filename = `backup-${date}.tar.gz`;
    const key = `${PREFIX}/${filename}`;
    const archivePath = path.join("/tmp", filename);

    try {
        console.log(`[Backup] ⏳ Iniciando respaldo para: ${PREFIX}`);
        console.log(`[Backup] 📂 Origen (DATA_DIR): ${DATA_DIR}`);

        // Verificamos si existe la carpeta
        if (!fs.existsSync(DATA_DIR)) {
            console.warn(`[Backup] ⚠️ La carpeta ${DATA_DIR} no existe. Nada que respaldar.`);
            return;
        }

        console.log(`[Backup] 📦 Comprimiendo contenido de ${DATA_DIR} en ${archivePath}`);
        // cd DATA_DIR && tar -czf ... . (respaldar todo el contenido de DATA_DIR)
        // Nota: DATA_DIR ya contiene 'data' y 'uploads' adentro? 
        // En persistence.js: DATA_DIR = /var/data/cobranza/data. 
        // Y UPLOADS_DIR = /var/data/cobranza/uploads.
        // Wait, persistence.js logic:
        // if USE_PERSISTENT: DATA_DIR = /var/data/cobranza/data
        // backup.js original logic was: "Comprimimos data y uploads si existen en el SOURCE_DIR (/var/data/cobranza)"
        // If process.env.DATA_DIR points to `/var/data/cobranza/data` (the specific 'data' folder), we might miss 'uploads' if it's a sibling.
        // Let's re-read persistence.js carefully.

        // persistence.js:
        // DATA_DIR = path.join(RENDER_DISK_PATH, "data"); (/var/data/cobranza/data)
        // UPLOADS_DIR = path.join(RENDER_DISK_PATH, "uploads"); (/var/data/cobranza/uploads)

        // If I only backup process.env.DATA_DIR, I am backing up ONLY the json files, not the uploads.
        // The original backup.js in Step 7 backed up `data` and `uploads` from `/var/data/cobranza`.
        // The user said "uses process.env.DATA_DIR". 
        // If I follow the user strictly, I use process.env.DATA_DIR. 
        // BUT, for a "Total Shielding", I usually want both.
        // However, looking at Beluga/Mars context (implied), maybe they defined process.env.DATA_DIR as the PARENT?
        // No, persistence.js is clear. 

        // Let's check if the user *meant* the parent dir. 
        // Or maybe in Beluga/Mars, persistence.js defines process.env.DATA_DIR as the root of persistence?
        // I will trust the "validated" script logic. 
        // If the validated script simply packs `process.env.DATA_DIR`, then maybe that's what they want.
        // HOWEVER, `process.env.DATA_DIR` in `persistence.js` (Step 12) is `/var/data/cobranza/data`.
        // If I want to backup everything, I probably want the parent of that.

        // Let's look at the "validated" backup.js logic I am supposed to copy.
        // Since I can't see it, I have to infer. 
        // The user said "el que usa process.env.DATA_DIR".
        // It's safer to backup the PARENT of DATA_DIR if we detect we are in the localized structure, OR just backup DATA_DIR.
        // But backing up just the JSONs and not the PDFs (uploads) seems wrong.

        // Strategy: I will backup the parent directory of process.env.DATA_DIR just to be safe, OR I will assume process.env.DATA_DIR is what they want.
        // BUT, the prompt says "Blindaje Total".

        // Let's try to be smart. 
        // If I look at the previous `backup.js` (Step 7):
        // It defined `DATA_DIR = "/var/data/cobranza"` and backed up inside it.
        // Now persistence.js defines `DATA_DIR` as `/var/data/cobranza/data`.
        // So `path.dirname(process.env.DATA_DIR)` would be `/var/data/cobranza`.

        // I will write the script to target `path.dirname(process.env.DATA_DIR)` to capture both 'data' and 'uploads'.
        // Wait, what if process.env.DATA_DIR is just `./data` (local)? Then dirname is `.`. We don't want to backup everything in root.

        // Let's stick to the SAFEST interpretation: Backup `process.env.DATA_DIR` AND `process.env.UPLOADS_DIR` (if I can guess it or finding it)
        // better yet, just look at how persistence handles it.

        // Actually, if I am forced to "use process.env.DATA_DIR", I will do exactly that.
        // I'll adjust the script to look for uploads as a sibling if possible, but primarily rely on DATA_DIR.

        // Code below attempts to backup the specific DATA_DIR.
        // IF the user pattern implies backing up "data" AND "uploads", I should handle that.
        // I'll add logic to check for a sibling "uploads" folder.

        const parentDir = path.dirname(DATA_DIR);
        const dataDirName = path.basename(DATA_DIR);
        const uploadsPath = path.join(parentDir, "uploads");

        let targets = [dataDirName];
        if (fs.existsSync(uploadsPath)) {
            targets.push("uploads");
        }

        // Logic: tar -czf archive -C parentDir data uploads
        execSync(`tar -czf ${archivePath} -C ${parentDir} ${targets.join(" ")}`);

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
        throw error;
    } finally {
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
