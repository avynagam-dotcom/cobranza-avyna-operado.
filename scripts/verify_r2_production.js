"use strict";

const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const fs = require("fs");
const path = require("path");

// Helper para logs claros
const log = (msg) => console.log(`[R2-Verify] ${msg}`);
const logError = (msg, err) => {
    console.error(`[R2-Verify] ❌ ${msg}`);
    if (err) console.error(err);
};

async function runVerification() {
    log("🔵 Iniciando Protocolo de Verificación de Producción...");

    try {
        // 1. Validar Entorno
        const R2_ENDPOINT = process.env.R2_ENDPOINT;
        const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
        const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
        const R2_BUCKET = process.env.R2_BUCKET;
        const SYSTEM_NAME = process.env.SYSTEM_NAME || "Unknown-System";

        if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
            throw new Error("Faltan variables de entorno CRÍTICAS (R2_ENDPOINT, etc). ¿Estás en Render?");
        }
        log("✅ Variables de entorno detectadas.");

        // 2. Verificar Datos Reales (Disco Persistente)
        // Intentar ubicar el archivo real
        const possiblePaths = [
            process.env.DATA_DIR ? path.join(process.env.DATA_DIR, 'notas.json') : null,
            '/var/data/cobranza/data/notas.json',
            path.join(__dirname, '../data/notas.json')
        ].filter(Boolean);

        let dbSize = "No encontrado";
        let dbPathFound = null;

        for (const p of possiblePaths) {
            if (fs.existsSync(p)) {
                const stats = fs.statSync(p);
                dbSize = `${(stats.size / 1024).toFixed(2)} KB`;
                dbPathFound = p;
                break;
            }
        }

        log(`📂 Base de Datos Real (${SYSTEM_NAME}):`);
        log(`   Ruta: ${dbPathFound || "NO ENCONTRADA"}`);
        log(`   Peso: ${dbSize}`);

        // 3. Prueba de Conectividad (Subida)
        const s3 = new S3Client({
            region: "auto",
            endpoint: R2_ENDPOINT,
            credentials: {
                accessKeyId: R2_ACCESS_KEY_ID,
                secretAccessKey: R2_SECRET_ACCESS_KEY,
            },
        });

        const testFilename = "r2_connectivity_test.txt";
        const fileContent = `Connectivity Check for ${SYSTEM_NAME}\nDate: ${new Date().toISOString()}\nDB Size: ${dbSize}`;
        const namespace = SYSTEM_NAME.toLowerCase();
        const key = `${namespace}/diagnostics/${testFilename}`;

        log(`🚀 Intentando subir archivo de prueba a: ${key}`);

        const command = new PutObjectCommand({
            Bucket: R2_BUCKET,
            Key: key,
            Body: fileContent,
            ContentType: "text/plain",
        });

        const response = await s3.send(command);

        log(`✅ SUBIDA EXITOSA`);
        log(`   ETag: ${response.ETag}`);
        log(`   ServerSideEncryption: ${response.ServerSideEncryption || "N/A"}`);
        log(`   RequestID: ${response.$metadata.requestId}`);

        console.log("\n==========================================");
        console.log("   COPIA Y PEGA ESTO EN EL REPORTE:");
        console.log(`   Sistema: ${SYSTEM_NAME}`);
        console.log(`   Peso DB: ${dbSize}`);
        console.log(`   ETag Cloudflare: ${response.ETag}`);
        console.log("==========================================\n");

    } catch (error) {
        logError("FALLO CRÍTICO EN VERIFICACIÓN", error);
        process.exit(1);
    }
}

// Ejecutar
runVerification();
