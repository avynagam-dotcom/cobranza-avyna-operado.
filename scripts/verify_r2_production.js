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

        // SYSTEM_NAME Critical Check
        const rawSystemName = process.env.SYSTEM_NAME;
        const SYSTEM_NAME = rawSystemName || "Unknown-System";
        const namespace = SYSTEM_NAME.toLowerCase();

        console.log("\n==========================================");
        console.log(`🔎 VALIDACIÓN DE IDENTIDAD`);
        console.log(`   Sistema:      [${SYSTEM_NAME}]`);
        console.log(`   Carpeta R2:   [${namespace}/]`);
        console.log("==========================================\n");

        if (!rawSystemName) {
            console.error("⚠️  [ADVERTENCIA CRÍTICA]: 'SYSTEM_NAME' NO ESTÁ CONFIGURADO.");
            console.error("    El backup caerá en 'unknown-system' y podría SOBREESCRIBIR otros datos.");
            console.error("    CONFIGURA LA VARIABLE EN RENDER INMEDIATAMENTE.\n");
            // We proceed just to test connectivity, but with giant warning
        }

        if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
            throw new Error("Faltan variables de entorno CRÍTICAS (R2_ENDPOINT, etc). ¿Estás en Render?");
        }
        log("✅ Credenciales R2 detectadas.");

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

        log(`📂 Base de Datos Real: ${dbSize} (${dbPathFound || "Ruta no hallada"})`);

        // 3. Prueba de Conectividad (Subida)
        const s3 = new S3Client({
            region: "auto",
            endpoint: R2_ENDPOINT,
            credentials: {
                accessKeyId: R2_ACCESS_KEY_ID,
                secretAccessKey: R2_SECRET_ACCESS_KEY,
            },
        });

        const testFilename = "identity_check.txt";
        // Force timestamp to avoid caching/collision during rapid testing
        const fileContent = `Identity Check\nSystem: ${SYSTEM_NAME}\nNamespace: ${namespace}\nDate: ${new Date().toISOString()}`;
        const key = `${namespace}/diagnostics/${testFilename}`;

        log(`🚀 Subiendo testigo a: [${key}] ...`);

        const command = new PutObjectCommand({
            Bucket: R2_BUCKET,
            Key: key,
            Body: fileContent,
            ContentType: "text/plain",
        });

        const response = await s3.send(command);

        console.log("\n✅ [PRUEBA EXITOSA] - RECIBO DE CONFIRMACIÓN:");
        console.log(`   Sistema:      ${SYSTEM_NAME}`);
        console.log(`   Ruta Final:   ${key}`);
        console.log(`   ETag:         ${response.ETag}`);
        console.log("\n");

    } catch (error) {
        logError("FALLO CRÍTICO EN VERIFICACIÓN", error);
        process.exit(1);
    }
}

// Ejecutar
runVerification();
