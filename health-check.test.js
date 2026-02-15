const http = require('http');

/**
 * health-check.test.js
 * "Alarma de Pulso" - Valida que el servidor esté vivo y que la DB esté conectada.
 * 
 * Basado en la alarma del backend principal.
 */

const URL = process.env.SERVER_URL || 'http://localhost:4000';
const SYSTEM_NAME = process.env.SYSTEM_NAME || 'Clon Cobranza';

console.log(`📡 [${SYSTEM_NAME}] Iniciando Alarma de Pulso en: ${URL}...`);

function checkRoute(path) {
    return new Promise((resolve, reject) => {
        const fullUrl = `${URL}${path}`;
        const req = http.get(fullUrl, (res) => {
            const { statusCode } = res;
            if (statusCode === 200) {
                resolve(true);
            } else {
                reject(new Error(`Status ${statusCode} en ${path}`));
            }
        });
        req.on('error', (e) => reject(e));
        req.setTimeout(5000, () => {
            req.destroy();
            reject(new Error(`Timeout en ${path}`));
        });
    });
}

async function run() {
    try {
        // 1. Verificar Servidor (Status 200)
        await checkRoute('/');
        console.log('✅ [PULSO OK] - El servidor responde correctamente.');

        // 2. Verificar Base de Datos (Conexión JSON)
        // Se valida que el endpoint /api/notas responda, lo cual garantiza acceso al archivo de datos.
        await checkRoute('/api/notas');
        console.log('✅ [DB OK] - La base de datos está conectada y accesible.');

        console.log('\n🟢 SEMÁFORO EN VERDE: Sistema Saludable\n');
        process.exit(0);
    } catch (err) {
        console.error(`\n❌ [ERROR DE PULSO] - Detalle: ${err.message}`);
        console.log('🔴 SEMÁFORO EN ROJO: El sistema requiere atención.\n');
        process.exit(1);
    }
}

run();
