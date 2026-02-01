"use strict";
const fs = require("fs");
const path = require("path");

// 1. Configuración de Rutas y Persistencia
const ROOT_DIR = path.resolve(__dirname, "..");
const RENDER_DISK_PATH = "/var/data/cobranza";

// Determinar si usamos persistencia (Render Disk)
const USE_PERSISTENT = fs.existsSync(RENDER_DISK_PATH);

// Definir DATA_DIR
// Si USE_PERSISTENT es true, usamos RENDER_DISK_PATH + /data
// Si no, usamos process.env.DATA_DIR (si existe) o ROOT_DIR + /data
let DATA_DIR;
let UPLOADS_DIR;

if (USE_PERSISTENT) {
    console.log(`[Persistence] Detectado Persistent Disk en: ${RENDER_DISK_PATH}`);
    DATA_DIR = path.join(RENDER_DISK_PATH, "data");
    UPLOADS_DIR = path.join(RENDER_DISK_PATH, "uploads");
} else {
    console.log(`[Persistence] Usando almacenamiento local/ephemeral`);
    DATA_DIR = process.env.DATA_DIR || path.join(ROOT_DIR, "data");
    UPLOADS_DIR = path.join(ROOT_DIR, "uploads");
}

// Exportar rutas para uso global
process.env.DATA_DIR = DATA_DIR; // Asegurar que esté en env como pide la regla
const DB_FILE = path.join(DATA_DIR, "notas.json");
const AUDIT_FILE = path.join(DATA_DIR, "audit.jsonl");

// 2. Inicialización Segura
function init() {
    console.log(`[Persistence] Inicializando directorios...`);
    console.log(`  DATA_DIR: ${DATA_DIR}`);
    console.log(`  UPLOADS_DIR: ${UPLOADS_DIR}`);

    // Regla estricta: asegurar existencia antes de escritura
    if (!fs.existsSync(DATA_DIR)) {
        fs.mkdirSync(DATA_DIR, { recursive: true });
    }
    if (!fs.existsSync(UPLOADS_DIR)) {
        fs.mkdirSync(UPLOADS_DIR, { recursive: true });
    }
}

// 3. Helpers de Base de Datos
function loadDB() {
    try {
        if (!fs.existsSync(DB_FILE)) return [];
        const raw = fs.readFileSync(DB_FILE, "utf8");
        const data = JSON.parse(raw);
        return Array.isArray(data) ? data : [];
    } catch (err) {
        console.error(`[Persistence] Error cargando DB: ${err.message}`);
        return [];
    }
}

/**
 * Escritura Atómica con Auditoría
 * Reglas:
 * - Escribir a .tmp
 * - Validar peso > 0
 * - Rename
 * - Log auditoría
 */
function saveDB(data) {
    const tempFile = `${DB_FILE}.tmp`;

    try {
        // Serializar
        const content = JSON.stringify(data, null, 2);

        // 1. Escribir a temporal
        fs.writeFileSync(tempFile, content, "utf8");

        // 2. Validar integridad (peso > 0)
        const stats = fs.statSync(tempFile);
        if (stats.size === 0) {
            throw new Error("El archivo temporal está vacío (0 bytes). Abortando escritura.");
        }

        // 3. Reemplazo Atómico
        fs.renameSync(tempFile, DB_FILE);

        // 4. Auditoría
        auditChange("saveDB", { size: stats.size, timestamp: new Date().toISOString() });

    } catch (err) {
        console.error(`[Persistence] CRITICAL SAVE ERROR: ${err.message}`);
        // Intentar limpiar tmp si quedó corrupto
        if (fs.existsSync(tempFile)) {
            try { fs.unlinkSync(tempFile); } catch (e) { }
        }
        throw err; // Re-lanzar para que el endpoint sepa que falló
    }
}

function auditChange(action, details) {
    try {
        const entry = {
            timestamp: new Date().toISOString(),
            action,
            details
        };
        const line = JSON.stringify(entry) + "\n";
        fs.appendFileSync(AUDIT_FILE, line, "utf8");
    } catch (e) {
        console.error("[Persistence] Fallo escribiendo auditoría:", e.message);
    }
}

module.exports = {
    init,
    loadDB,
    saveDB,
    DATA_DIR,
    UPLOADS_DIR,
    USE_PERSISTENT
};
