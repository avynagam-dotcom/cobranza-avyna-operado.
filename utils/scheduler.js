"use strict";

const cron = require("node-cron");
const runBackup = require("../scripts/backup");

function initScheduler() {
    console.log("[Scheduler] 🕒 Inicializando cron jobs...");

    // Programar tarea para las 09:00 UTC (03:00 AM CDMX)
    // Cron sintaxis: minuto hora dia mes dia_semana
    // 0 9 * * * = todos los días a las 09:00 UTC (dependiendo de la hora del servidor, node-cron usa la hora del sistema por defecto, si es UTC, es UTC)
    // Para asegurar UTC, podemos usar la opción de timezone, pero node-cron basic usa server time.
    // En Render, la hora suele ser UTC. Así que '0 9 * * *' es correcto para 09:00 UTC.

    cron.schedule("0 9 * * *", async () => {
        console.log("[Scheduler] ⏰ Ejecutando tarea programada: Backup Diario (09:00 UTC)");
        try {
            await runBackup();
        } catch (error) {
            console.error("[Scheduler] ❌ Error en backup programado:", error.message);
        }
    });

    console.log("[Scheduler] ✅ Scheduler activo (Backup: 09:00 UTC)");
}

module.exports = { initScheduler };
