"use strict";

const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

async function runBackup() {
    const SYSTEM_NAME = process.env.SYSTEM_NAME || "avyna-desconocido";
    const R2_ENDPOINT = process.env.R2_ENDPOINT;
    const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
    const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
    const R2_BUCKET = process.env.R2_BUCKET;

    // Carpeta de datos a respaldar (la del disco persistente)
    const DATA_DIR = "/var/data/cobranza";
    // Si no existe el disco persistente, intentamos la local (desarrollo)
    const SOURCE_DIR = fs.existsSync(DATA_DIR) ? DATA_DIR : path.join(__dirname, "..");

    if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
        console.error("❌ Faltan variables de entorno para el backup (R2)");
        process.exit(1);
    }

    const date = new Date().toISOString().split("T")[0];
    const filename = `backup-${SYSTEM_NAME}-${date}.tar.gz`;
    const archivePath = path.join("/tmp", filename);

    try {
        console.log(`📦 Creando archivo comprimido: ${filename}...`);
        // Comprimimos data y uploads si existen en el SOURCE_DIR
        const targets = [];
        if (fs.existsSync(path.join(SOURCE_DIR, "data"))) targets.push("data");
        if (fs.existsSync(path.join(SOURCE_DIR, "uploads"))) targets.push("uploads");

        if (targets.length === 0) {
            console.log("⚠️ No hay carpetas 'data' o 'uploads' para respaldar.");
            return;
        }

        execSync(`tar -czf ${archivePath} -C ${SOURCE_DIR} ${targets.join(" ")}`);

        console.log(`🚀 Subiendo a Cloudflare R2...`);
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
            Key: `${SYSTEM_NAME}/${filename}`, // Organizado por carpeta de sistema
            Body: fileBuffer,
            ContentType: "application/gzip",
        }));

        console.log(`✅ Backup completado exitosamente: ${SYSTEM_NAME}/${filename}`);

        // Limpieza
        fs.unlinkSync(archivePath);

    } catch (error) {
        console.error("❌ Error durante el backup:", error);
        throw error;
    }
}

module.exports = runBackup;

if (require.main === module) {
    runBackup().catch(() => process.exit(1));
}
