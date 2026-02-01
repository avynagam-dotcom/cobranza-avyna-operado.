"use strict";

const express = require("express");
const cors = require("cors");
const multer = require("multer");
const pdfParse = require("pdf-parse");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

// ----- Persistence Integration
const persistence = require("./utils/persistence");
persistence.init();

const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, "public");

const { DATA_DIR, UPLOADS_DIR, USE_PERSISTENT, loadDB, saveDB } = persistence;

// ----- Status Persistence (Closing Control)
const STATUS_FILE = path.join(DATA_DIR, "status.json");
function loadStatus() {
  try {
    if (!fs.existsSync(STATUS_FILE)) return { lastReportClick: null };
    return JSON.parse(fs.readFileSync(STATUS_FILE, "utf8"));
  } catch (e) {
    return { lastReportClick: null };
  }
}
function saveStatus(data) {
  try {
    fs.writeFileSync(STATUS_FILE, JSON.stringify(data, null, 2));
  } catch (e) {
    console.error("Error saving status:", e);
  }
}

// ----- Backup Automático (Scheduler)
const { initScheduler } = require('./utils/scheduler');
initScheduler();

// Folders ensured by persistence.init()

// ----- Migration: Local -> Persistent (Idempotent)
if (USE_PERSISTENT) {
  try {
    const localDataDir = path.join(ROOT, "data");
    const localUploadsDir = path.join(ROOT, "uploads");

    function migrateFiles(srcDir, destDir) {
      if (!fs.existsSync(srcDir)) return;
      const files = fs.readdirSync(srcDir);
      let count = 0;
      for (const file of files) {
        if (file.startsWith(".")) continue;
        const srcPath = path.join(srcDir, file);
        const destPath = path.join(destDir, file);
        try {
          if (fs.statSync(srcPath).isFile() && !fs.existsSync(destPath)) {
            fs.copyFileSync(srcPath, destPath);
            count++;
          }
        } catch (e) {
          console.error(`[Migra] Error copiando ${file}:`, e.message);
        }
      }
      if (count > 0) console.log(`[Migra] Se migraron ${count} archivos de ${srcDir} a ${destDir}`);
    }
    migrateFiles(localDataDir, DATA_DIR);
    migrateFiles(localUploadsDir, UPLOADS_DIR);
  } catch (err) {
    console.error("[Migra] Fallo en proceso de migración de archivos:", err);
  }
}

// ----- Migration Schema V2 (pagado -> pagos[])
// Se ejecuta al inicio para asegurar consistencia
(function migrateSchemaV2() {
  try {
    const notas = loadDB();
    let changed = false;
    let totalDebtBefore = 0;

    // Calcular deuda total antes de migración
    notas.forEach(n => {
      const t = n.total || 0;
      const p = n.pagado || 0;
      totalDebtBefore += Math.max(t - p, 0);
    });

    notas.forEach(n => {
      // Si no tiene array de pagos pero tiene 'pagado' > 0
      if (!Array.isArray(n.pagos)) {
        n.pagos = [];
        const oldPagado = Number(n.pagado) || 0;
        if (oldPagado > 0) {
          // Migrar saldo total actual (pagado acumulado) a un registro legacy
          n.pagos.push({
            fecha: new Date().toISOString(), // Se usa fecha actual de corrida como marca
            monto: oldPagado,
            tipo: 'MIGRACIÓN/LEGACY'
          });
          changed = true;
        }
      }
      // Recalcular 'pagado' desde el array siempre para consistencia
      const computedPagado = n.pagos.reduce((acc, p) => acc + (Number(p.monto) || 0), 0);
      n.pagado = computedPagado;
    });

    if (changed) {
      console.log("[Schema V2] Migración preliminar ejecutada.");
      saveDB(notas);
    }

    // Validación de Balance
    let totalDebtAfter = 0;
    notas.forEach(n => {
      const t = n.total || 0;
      const p = n.pagado || 0;
      totalDebtAfter += Math.max(t - p, 0);
    });

    console.log(`[Schema V2] Validación de Balance:
      - Deuda Antes: $${totalDebtBefore.toFixed(2)}
      - Deuda Después: $${totalDebtAfter.toFixed(2)}
      - ${Math.abs(totalDebtAfter - totalDebtBefore) < 0.01 ? "✅ BALANCE CORRECTO" : "❌ ERROR DE BALANCE"}
    `);

  } catch (e) {
    console.error("[Schema V2] Error Critical en Migración:", e);
  }
})();


// ----- Batch helpers
function pad2(n) { return String(n).padStart(2, "0"); }
function ymd(d) { return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`; }

function getMexicoDate(date = new Date()) {
  const options = { timeZone: "America/Mexico_City", year: 'numeric', month: 'numeric', day: 'numeric', hour: 'numeric', minute: 'numeric', second: 'numeric' };
  const formatter = new Intl.DateTimeFormat([], options);
  const parts = formatter.formatToParts(date);
  const get = (type) => parts.find(p => p.type === type).value;
  return new Date(get('year'), get('month') - 1, get('day'), get('hour'), get('minute'), get('second'));
}

function getCurrentBatchKey(now = new Date()) {
  const mxDate = getMexicoDate(now);
  const day = mxDate.getDay();
  const daysSinceMonday = (day - 1 + 7) % 7;
  const d = new Date(mxDate);
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - daysSinceMonday);
  return ymd(d);
}

// ----- Date helpers
function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}
function iso(d) { return d ? new Date(d).toISOString() : null; }

// ----- VIP Logic & Algorithm
function isVIP(clienteName, allNotas) {
  if (!clienteName) return false;
  // Filtrar notas del cliente
  const clienteNotas = allNotas.filter(n => (n.cliente || "").toLowerCase() === clienteName.toLowerCase());

  // 1. Volumen Mensual > $10,000 (usando pagos de este mes actual o promedio? "volumen mensual > $10,000" suele ser facturación o pago)
  // Asumiremos Facturación Total Acumulada del mes en curso > 10,000
  // O mejor, el criterio simple: Total histórico mensual promedio > 10k?
  // "Activa el distintivo VIP ⭐ para clientes con volumen mensual >$10,000 MXN." -> Interpretación: En el mes actual o últimos 30 días.
  // Vamos a usar la facturación del mes corriente.

  const now = new Date();
  const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1);

  const monthVolume = clienteNotas
    .filter(n => new Date(n.uploadedAt) >= startOfMonth)
    .reduce((sum, n) => sum + (n.total || 0), 0);

  if (monthVolume <= 10000) return false;

  // 2. Criterio de Exclusión: Pagos tardíos (después del dueAt) desde Febrero 1, 2026
  const CUTOFF_DATE = new Date("2026-02-01T00:00:00");

  for (const n of clienteNotas) {
    if (!n.dueAt || !n.pagos) continue;
    const due = new Date(n.dueAt);

    // Revisar cada pago
    for (const p of n.pagos) {
      const payDate = new Date(p.fecha);
      // Solo penalizar si el pago fue DESPUÉS del dueAt Y DESPUÉS de la fecha de corte
      if (payDate > CUTOFF_DATE && payDate > due) {
        return false; // Perdió su estrella
      }
    }
  }

  return true;
}


// ----- Credit Status
function computeCredito(nota, now = new Date()) {
  const deliveredAt = nota.deliveredAt ? new Date(nota.deliveredAt) : null;
  const dueAt = nota.dueAt ? new Date(nota.dueAt) : null;
  const total = typeof nota.total === "number" && Number.isFinite(nota.total) ? nota.total : null;

  // Recalcular pagado desde array pagos
  const pagado = Array.isArray(nota.pagos)
    ? nota.pagos.reduce((acc, p) => acc + (Number(p.monto) || 0), 0)
    : (nota.pagado || 0);

  let saldo = null;
  if (total != null) saldo = Math.max(total - pagado, 0);

  let statusCredito = "PRE_ENTREGA";
  if (deliveredAt) {
    if (saldo === 0 && total != null) {
      statusCredito = "LIQUIDADO";
    } else if (dueAt) {
      const msNow = now.getTime();
      const msDue = dueAt.getTime();
      const threeDaysMs = 3 * 24 * 60 * 60 * 1000;
      if (msNow >= msDue) statusCredito = "VENCIDO";
      else if (msNow >= msDue - threeDaysMs) statusCredito = "POR_VENCER";
      else statusCredito = "EN_PLAZO";
    } else {
      statusCredito = "EN_PLAZO";
    }
  }

  return {
    deliveredAt: nota.deliveredAt || null,
    dueAt: nota.dueAt || null,
    saldo,
    pagado, // devuelto calculado
    statusCredito,
  };
}

// ----- Multer & Parsing
function parseMoney(raw) {
  if (!raw) return null;
  const s = String(raw).replace(/\s/g, "");
  const lastDot = s.lastIndexOf(".");
  const lastComma = s.lastIndexOf(",");
  const decPos = Math.max(lastDot, lastComma);
  let normalized;
  if (decPos === -1) normalized = s.replace(/[^\d]/g, "");
  else {
    const intPart = s.slice(0, decPos).replace(/[^\d]/g, "");
    const decPart = s.slice(decPos + 1).replace(/[^\d]/g, "").slice(0, 2);
    normalized = `${intPart}.${decPart}`;
  }
  const n = Number(normalized);
  return Number.isFinite(n) ? n : null;
}

function extractTotalFromText(text) {
  // ... (Keep existing extraction logic logic or simplified for brevity if redundant, but better keep it robust)
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const totalLines = lines.filter((l) => /total/i.test(l)).filter((l) => !/sub\s*total/i.test(l));
  const patterns = [
    /(TOTAL\s*A\s*PAGAR)\s*[:\-]?\s*\$?\s*([0-9][0-9.,\s]*)/i,
    /(IMPORTE\s*TOTAL)\s*[:\-]?\s*\$?\s*([0-9][0-9.,\s]*)/i,
    /(^|\b)(TOTAL)\s*[:\-]?\s*\$?\s*([0-9][0-9.,\s]*)/i,
  ];
  let candidates = [];
  for (const l of totalLines) {
    for (const p of patterns) {
      const m = l.match(p);
      if (m) {
        const val = parseMoney(m[m.length - 1]);
        if (val != null) candidates.push(val);
      }
    }
  }
  if (candidates.length === 0) {
    for (const p of patterns) {
      const all = [...text.matchAll(p)];
      if (all.length) {
        const last = all[all.length - 1];
        const val = parseMoney(last[last.length - 1]);
        if (val != null) candidates.push(val);
      }
    }
  }
  if (candidates.length === 0) return null;
  return Math.max(...candidates);
}

function extractClienteFromText(text) {
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const sameLine = [
    /(CLIENTE)\s*[:\-]\s*(.+)$/i,
    /(NOMBRE)\s*[:\-]\s*(.+)$/i,
    /(RAZ[ÓO]N\s+SOCIAL)\s*[:\-]\s*(.+)$/i
  ];
  for (const l of lines) {
    for (const p of sameLine) {
      const m = l.match(p);
      if (m && m[2] && m[2].trim().length >= 3) return m[2].trim();
    }
  }
  const nextLineLabels = [/^CLIENTE$/i, /^NOMBRE$/i, /^RAZ[ÓO]N\s+SOCIAL$/i];
  for (let i = 0; i < lines.length - 1; i++) {
    if (nextLineLabels.some((rx) => rx.test(lines[i]))) {
      const v = (lines[i + 1] || "").trim();
      if (v && v.length >= 3 && !/^(RFC|FECHA|FOLIO|TOTAL|SUBTOTAL)$/i.test(v)) return v;
    }
  }
  for (const l of lines) {
    const m = l.match(/^(\d{4,})\s*[-–—]\s*(.+)$/);
    if (m && m[2]) return `${m[1]} - ${m[2].trim()}`;
  }
  return null;
}

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 },
});

app.use(express.static(PUBLIC_DIR));

// ----- API: Check Report Status (Closing Control)
app.get("/api/status", (req, res) => {
  const status = loadStatus();
  const now = new Date();
  let remainingMs = 0;

  if (status.lastReportClick) {
    const clickTime = new Date(status.lastReportClick).getTime();
    const sixHoursMs = 6 * 60 * 60 * 1000;
    const elapsed = now.getTime() - clickTime;
    remainingMs = Math.max(sixHoursMs - elapsed, 0);
  }

  res.json({
    ok: true,
    reportOpen: remainingMs > 0,
    remainingMs,
    serverTime: now.toISOString()
  });
});

app.post("/api/status/open-report", (req, res) => {
  const status = loadStatus();
  // Si ya está abierto y no ha expirado, no reseteamos, solo devolvemos
  const now = new Date();
  if (status.lastReportClick) {
    const clickTime = new Date(status.lastReportClick).getTime();
    const sixHoursMs = 6 * 60 * 60 * 1000;
    if (now.getTime() - clickTime < sixHoursMs) {
      return res.json({ ok: true, message: "Reporte ya estaba abierto" });
    }
  }

  // Abrir nuevo (solo si es fin de mes? El frontend controla la visibilidad, 
  // pero el backend podría validar. Por ahora confiamos en el trigger del usuario).
  // El prompt dice "El botón aparece al final de mes... Al recibir el primer clic, se activa un temporizador"
  status.lastReportClick = now.toISOString();
  saveStatus(status);

  res.json({ ok: true, message: "Reporte abierto por 6 horas" });
});


// ----- API: Listar notas con VIP
app.get("/api/notas", (req, res) => {
  const notas = loadDB();
  const batchKey = getCurrentBatchKey();
  const now = new Date();

  const notasWithCredito = notas.map((n) => {
    const computed = computeCredito(n, now);
    // Inject VIP status
    const isVipClient = isVIP(n.cliente, notas);
    return {
      ...n,
      ...computed,
      pagado: computed.pagado, // ensure computed value
      isVip: isVipClient
    };
  });

  res.json({ batchKey, notas: notasWithCredito });
});

app.post("/api/upload", upload.single("pdf"), async (req, res) => {
  try {
    const batchKey = getCurrentBatchKey();
    if (!req.file || !req.file.buffer) return res.status(400).json({ ok: false, message: "No PDF" });

    const originalName = req.file.originalname || "nota.pdf";
    const notas = loadDB();

    const existingIdx = notas.findIndex(
      (n) => String(n.batchKey) === String(batchKey) &&
        String(n.originalName || "").toLowerCase() === String(originalName).toLowerCase()
    );

    const parsed = await pdfParse(req.file.buffer);
    const text = parsed && parsed.text ? parsed.text : "";
    const cliente = extractClienteFromText(text) || null;
    const total = extractTotalFromText(text);
    const uploadedAt = new Date().toISOString();

    if (existingIdx !== -1) {
      const ex = notas[existingIdx];
      if (ex.deliveredAt) return res.json({ ok: false, duplicate: true, message: "Nota duplicada (ya entregada)" });

      ex.cliente = cliente;
      ex.total = typeof total === "number" && Number.isFinite(total) ? total : null;
      ex.uploadedAt = uploadedAt;
      // Preserve existing filos/ids
      const filename = ex.filename || `${batchKey}__${ex.id}__${originalName}`.replace(/[^\w.\-() \u00C0-\u017F]/g, "_");
      ex.filename = filename;

      const filePath = path.join(UPLOADS_DIR, filename);
      fs.writeFileSync(filePath, req.file.buffer);

      notas[existingIdx] = ex;
      saveDB(notas);
      return res.json({ ok: true, replaced: true, nota: { ...ex, ...computeCredito(ex) } });
    }

    const id = crypto.randomUUID();
    const safeName = `${batchKey}__${id}__${originalName}`.replace(/[^\w.\-() \u00C0-\u017F]/g, "_");
    const filePath = path.join(UPLOADS_DIR, safeName);
    fs.writeFileSync(filePath, req.file.buffer);

    const nota = {
      id,
      batchKey,
      originalName,
      filename: safeName,
      cliente,
      total: typeof total === "number" && Number.isFinite(total) ? total : null,
      pagos: [], // NEW SCHEMA V2
      pagado: 0, // NEW SCHEMA V2 (cache)
      deliveredAt: null,
      dueAt: null,
      firstPaymentAt: null,
      uploadedAt,
    };

    notas.push(nota);
    saveDB(notas);
    return res.json({ ok: true, nota: { ...nota, ...computeCredito(nota) } });
  } catch (e) {
    console.error("UPLOAD ERROR:", e);
    return res.status(500).json({ ok: false, message: "Error upload" });
  }
});

app.post("/api/entregar", (req, res) => {
  try {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, message: "Falta id" });

    const notas = loadDB();
    const idx = notas.findIndex((n) => String(n.id) === String(id));
    if (idx === -1) return res.status(404).json({ ok: false, message: "Nota no encontrada" });

    const n = notas[idx];
    if (!n.deliveredAt) {
      const now = new Date();
      n.deliveredAt = iso(now);
      n.dueAt = iso(addDays(now, 15));
    }

    notas[idx] = n;
    saveDB(notas);
    return res.json({ ok: true, nota: { ...n, ...computeCredito(n) } });
  } catch (e) {
    console.error("ENTREGAR ERROR:", e);
    return res.status(500).json({ ok: false });
  }
});

// ----- API: V2 Registrar Pago (Array push)
app.post("/api/pago", (req, res) => {
  try {
    const { id, monto } = req.body || {};
    const val = Number(monto);

    if (!id || !Number.isFinite(val) || val <= 0) {
      return res.status(400).json({ ok: false, message: "Datos inválidos" });
    }

    const notas = loadDB();
    const idx = notas.findIndex((n) => String(n.id) === String(id));
    if (idx === -1) return res.status(404).json({ ok: false, message: "Nota no encontrada" });

    const n = notas[idx];

    // Ensure V2 structure
    if (!Array.isArray(n.pagos)) n.pagos = [];

    // Push new payment with SERVER DATE (Independencia Cronológica)
    // El prompt dice: "Asegura que los nuevos pagos registrados siempre tomen 
    // la fecha real del servidor, sin importar el estado del botón de cierre."
    n.pagos.push({
      fecha: new Date().toISOString(),
      monto: val,
      tipo: 'ABONO'
    });

    // Update cache
    n.pagado = n.pagos.reduce((acc, p) => acc + (Number(p.monto) || 0), 0);

    if (n.deliveredAt && !n.firstPaymentAt) {
      n.firstPaymentAt = new Date().toISOString();
    }

    notas[idx] = n;
    saveDB(notas);

    return res.json({ ok: true, nota: { ...n, ...computeCredito(n) } });
  } catch (e) {
    console.error("PAGO ERROR:", e);
    return res.status(500).json({ ok: false, message: "Error al registrar pago" });
  }
});

app.get("/api/kpis", (req, res) => {
  const notas = loadDB();
  const entregadas = notas.filter((n) => !!n.deliveredAt);

  let totalCobrable = 0;
  let totalCobrado = 0;

  for (const n of entregadas) {
    const total = typeof n.total === "number" && Number.isFinite(n.total) ? n.total : 0;
    // Use V2 computed pagado
    const computado = computeCredito(n);
    const pagado = computado.pagado || 0;

    totalCobrable += total;
    totalCobrado += Math.min(pagado, total);
  }

  const totalSaldo = Math.max(totalCobrable - totalCobrado, 0);
  const pctCobranza = totalCobrable > 0 ? totalCobrado / totalCobrable : 0;

  const utilidadCobrada = totalCobrado * 0.4;
  const utilidadPorCobrar = totalSaldo * 0.4;

  res.json({
    ok: true,
    totalCobrable,
    totalCobrado,
    totalSaldo,
    pctCobranza,
    utilidadCobrada,
    utilidadPorCobrar,
  });
});

app.get("/api/faltantes", (req, res) => {
  const notas = loadDB();
  const now = new Date();

  const faltantes = notas
    .filter((n) => !!n.deliveredAt)
    .map((n) => ({ ...n, ...computeCredito(n, now) }))
    .filter((n) => (typeof n.saldo === "number" ? n.saldo > 0 : true))
    .sort((a, b) => {
      const rank = (s) => s === "VENCIDO" ? 0 : s === "POR_VENCER" ? 1 : s === "EN_PLAZO" ? 2 : 3;
      const ra = rank(a.statusCredito);
      const rb = rank(b.statusCredito);
      if (ra !== rb) return ra - rb;
      const da = a.dueAt ? new Date(a.dueAt).getTime() : Number.POSITIVE_INFINITY;
      const db = b.dueAt ? new Date(b.dueAt).getTime() : Number.POSITIVE_INFINITY;
      return da - db;
    });

  res.json({ ok: true, faltantes });
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en http://localhost:${PORT}`);
  console.log(`Batch actual (lunes 00:00): ${getCurrentBatchKey()}`);
});