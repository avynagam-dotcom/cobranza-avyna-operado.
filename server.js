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

// ----- Paths
// ----- Paths configuration (Render Persistent Disk Support)
const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, "public");

// Detectar Persistent Disk de Render
const RENDER_DISK_PATH = "/var/data/cobranza";
// Usamos el disco solo si existe físicamente
const USE_PERSISTENT = fs.existsSync(RENDER_DISK_PATH);

let DATA_DIR, UPLOADS_DIR;

if (USE_PERSISTENT) {
  console.log(`[System] Usando Persistent Disk en: ${RENDER_DISK_PATH}`);
  DATA_DIR = path.join(RENDER_DISK_PATH, "data");
  UPLOADS_DIR = path.join(RENDER_DISK_PATH, "uploads");
} else {
  console.log(`[System] Usando almacenamiento local (ephemeral/local)`);
  DATA_DIR = process.env.DATA_DIR || path.join(ROOT, "data");
  UPLOADS_DIR = path.join(ROOT, "uploads");
}

const DB_FILE = path.join(DATA_DIR, "notas.json");

// ----- Backup Automático cada 24h a R2
const R2_ENABLED = process.env.R2_ENDPOINT && process.env.R2_ACCESS_KEY_ID;
if (R2_ENABLED) {
  const backup = require("./scripts/backup");
  // Ejecutar uno al iniciar (después de 30s para no saturar el arranque)
  setTimeout(() => {
    backup().catch(err => console.error("[AutoBackup] Fallo inicial:", err.message));
  }, 30000);
  // Y luego cada 24 horas
  setInterval(() => {
    backup().catch(err => console.error("[AutoBackup] Fallo periódico:", err.message));
  }, 24 * 60 * 60 * 1000);
}

// Ensure folders exist (Critical for new locations)
for (const dir of [DATA_DIR, UPLOADS_DIR]) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

// ----- Migration: Local -> Persistent (Idempotent)
// Se ejecuta solo si estamos en Render (Persistent) y detectamos archivos locales que no están en el disco
if (USE_PERSISTENT) {
  try {
    const localDataDir = path.join(ROOT, "data");
    const localUploadsDir = path.join(ROOT, "uploads");

    function migrateFiles(srcDir, destDir) {
      if (!fs.existsSync(srcDir)) return;

      const files = fs.readdirSync(srcDir);
      let count = 0;

      for (const file of files) {
        if (file.startsWith(".")) continue; // Ignorar .DS_Store, etc

        const srcPath = path.join(srcDir, file);
        const destPath = path.join(destDir, file);

        try {
          // Solo copiamos si es archivo y NO existe en destino
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
    console.error("[Migra] Fallo en proceso de migración:", err);
  }
}

// ----- DB helpers
function loadDB() {
  try {
    const raw = fs.readFileSync(DB_FILE, "utf8");
    const data = JSON.parse(raw);
    return Array.isArray(data) ? data : [];
  } catch {
    return [];
  }
}
function saveDB(notas) {
  fs.writeFileSync(DB_FILE, JSON.stringify(notas, null, 2), "utf8");
}

// ----- Batch (lunes 00:00)
function pad2(n) {
  return String(n).padStart(2, "0");
}
function ymd(d) {
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

function getMexicoDate(date = new Date()) {
  const options = { timeZone: "America/Mexico_City", year: 'numeric', month: 'numeric', day: 'numeric', hour: 'numeric', minute: 'numeric', second: 'numeric' };
  const formatter = new Intl.DateTimeFormat([], options);
  const parts = formatter.formatToParts(date);
  const get = (type) => parts.find(p => p.type === type).value;

  return new Date(get('year'), get('month') - 1, get('day'), get('hour'), get('minute'), get('second'));
}

function getCurrentBatchKey(now = new Date()) {
  // Usar hora de México para determinar el día
  const mxDate = getMexicoDate(now);

  // lunes más reciente a las 00:00 (hora México)
  // JS: 0=Dom,1=Lun,2=Mar,3=Mié...
  const day = mxDate.getDay();
  const daysSinceMonday = (day - 1 + 7) % 7;

  const d = new Date(mxDate);
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - daysSinceMonday);
  return ymd(d);
}

// ----- Date helpers (crédito)
function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}
function iso(d) {
  return d ? new Date(d).toISOString() : null;
}

// ----- Extraction helpers
function parseMoney(raw) {
  if (!raw) return null;
  const s = String(raw).replace(/\s/g, "");

  const lastDot = s.lastIndexOf(".");
  const lastComma = s.lastIndexOf(",");
  const decPos = Math.max(lastDot, lastComma);

  let normalized;
  if (decPos === -1) {
    normalized = s.replace(/[^\d]/g, "");
  } else {
    const intPart = s.slice(0, decPos).replace(/[^\d]/g, "");
    const decPart = s.slice(decPos + 1).replace(/[^\d]/g, "").slice(0, 2);
    normalized = `${intPart}.${decPart}`;
  }

  const n = Number(normalized);
  return Number.isFinite(n) ? n : null;
}

function extractTotalFromText(text) {
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);

  // líneas con TOTAL pero NO SUBTOTAL
  const totalLines = lines
    .filter((l) => /total/i.test(l))
    .filter((l) => !/sub\s*total/i.test(l));

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
        const moneyStr = m[m.length - 1];
        const val = parseMoney(moneyStr);
        if (val != null) candidates.push(val);
      }
    }
  }

  // fallback: todo el texto (última ocurrencia)
  if (candidates.length === 0) {
    for (const p of patterns) {
      const all = [...text.matchAll(p)];
      if (all.length) {
        const last = all[all.length - 1];
        const moneyStr = last[last.length - 1];
        const val = parseMoney(moneyStr);
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
    /(RAZ[ÓO]N\s+SOCIAL)\s*[:\-]\s*(.+)$/i,
  ];
  for (const l of lines) {
    for (const p of sameLine) {
      const m = l.match(p);
      if (m && m[2]) {
        const v = m[2].trim();
        if (v && v.length >= 3) return v;
      }
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

// ----- Crédito (estado en TIEMPO REAL)
function computeCredito(nota, now = new Date()) {
  const deliveredAt = nota.deliveredAt ? new Date(nota.deliveredAt) : null;
  const dueAt = nota.dueAt ? new Date(nota.dueAt) : null;

  const total = typeof nota.total === "number" && Number.isFinite(nota.total) ? nota.total : null;
  const pagado = typeof nota.pagado === "number" && Number.isFinite(nota.pagado) ? nota.pagado : 0;

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
    statusCredito,
  };
}

// ----- Multer (PDF upload)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }, // 15MB
});

// ----- Static
app.use(express.static(PUBLIC_DIR));

// ----- API: listar notas
app.get("/api/notas", (req, res) => {
  const notas = loadDB();
  const batchKey = getCurrentBatchKey();
  const now = new Date();
  const notasWithCredito = notas.map((n) => ({ ...n, ...computeCredito(n, now) }));
  res.json({ batchKey, notas: notasWithCredito });
});

// ----- API: subir PDF
app.post("/api/upload", upload.single("pdf"), async (req, res) => {
  try {
    const batchKey = getCurrentBatchKey();

    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ ok: false, message: "No se recibió PDF" });
    }

    const originalName = req.file.originalname || "nota.pdf";
    const notas = loadDB();

    // ✅ Regla nueva:
    // Si hay una nota con mismo nombre EN EL BATCH:
    // - si NO está entregada => sustituir (mismo id, mismo filename, sobreescribe PDF y actualiza cliente/total)
    // - si YA está entregada => bloquear (duplicado)
    const existingIdx = notas.findIndex(
      (n) =>
        String(n.batchKey) === String(batchKey) &&
        String(n.originalName || "").toLowerCase() === String(originalName).toLowerCase()
    );

    // Parse PDF (siempre parseamos porque para sustituir necesitamos nuevo total/cliente)
    const parsed = await pdfParse(req.file.buffer);
    const text = parsed && parsed.text ? parsed.text : "";
    const cliente = extractClienteFromText(text) || null;
    const total = extractTotalFromText(text);
    const uploadedAt = new Date().toISOString();

    if (existingIdx !== -1) {
      const ex = notas[existingIdx];

      // Si ya está entregada: NO se sustituye
      if (ex.deliveredAt) {
        return res.json({ ok: false, duplicate: true, message: "Nota duplicada (ya entregada)" });
      }

      // ✅ Sustituir (pre-entrega)
      // Mantener: id, pagado, deliveredAt(null), dueAt(null), firstPaymentAt, batchKey
      // Actualizar: cliente, total, uploadedAt
      ex.cliente = cliente;
      ex.total = typeof total === "number" && Number.isFinite(total) ? total : null;
      ex.uploadedAt = uploadedAt;

      // Guardar / sobreescribir el PDF usando el mismo filename de esa nota
      // (Esto mantiene tu historial limpio y evita crear 2 notas)
      const filename = ex.filename || `${batchKey}__${ex.id}__${originalName}`.replace(
        /[^\w.\-() \u00C0-\u017F]/g,
        "_"
      );
      ex.filename = filename;

      const filePath = path.join(UPLOADS_DIR, filename);
      fs.writeFileSync(filePath, req.file.buffer);

      notas[existingIdx] = ex;
      saveDB(notas);

      return res.json({ ok: true, replaced: true, nota: { ...ex, ...computeCredito(ex) } });
    }

    // ✅ Nueva nota (no existe)
    const id = crypto.randomUUID();

    const safeName = `${batchKey}__${id}__${originalName}`.replace(
      /[^\w.\-() \u00C0-\u017F]/g,
      "_"
    );
    const filePath = path.join(UPLOADS_DIR, safeName);
    fs.writeFileSync(filePath, req.file.buffer);

    const nota = {
      id,
      batchKey,
      originalName,
      filename: safeName,
      cliente,
      total: typeof total === "number" && Number.isFinite(total) ? total : null,
      pagado: 0,
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
    return res.status(500).json({ ok: false, message: "Error al subir PDF" });
  }
});

// ----- API: marcar ENTREGADO (inicio crédito)
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
      // ✅ 15 días (como quedamos)
      n.dueAt = iso(addDays(now, 15));
    }

    notas[idx] = n;
    saveDB(notas);

    return res.json({ ok: true, nota: { ...n, ...computeCredito(n) } });
  } catch (e) {
    console.error("ENTREGAR ERROR:", e);
    return res.status(500).json({ ok: false, message: "Error al marcar entregado" });
  }
});

// ----- API: registrar pago
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
    n.pagado = Number(n.pagado || 0) + val;

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

// ----- KPIs globales (SOLO ENTREGADAS) ✅ consistencia y utilidades
app.get("/api/kpis", (req, res) => {
  const notas = loadDB();
  const entregadas = notas.filter((n) => !!n.deliveredAt);

  let totalCobrable = 0;
  let totalCobrado = 0;

  for (const n of entregadas) {
    const total = typeof n.total === "number" && Number.isFinite(n.total) ? n.total : 0;
    const pagado = typeof n.pagado === "number" && Number.isFinite(n.pagado) ? n.pagado : 0;

    totalCobrable += total;
    totalCobrado += Math.min(pagado, total);
  }

  // ✅ saldo = cobrable - cobrado (evita discrepancias)
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

// ----- quién falta por pagar (entregadas con saldo)
app.get("/api/faltantes", (req, res) => {
  const notas = loadDB();
  const now = new Date();

  const faltantes = notas
    .filter((n) => !!n.deliveredAt)
    .map((n) => ({ ...n, ...computeCredito(n, now) }))
    .filter((n) => (typeof n.saldo === "number" ? n.saldo > 0 : true))
    .sort((a, b) => {
      const rank = (s) =>
        s === "VENCIDO" ? 0 : s === "POR_VENCER" ? 1 : s === "EN_PLAZO" ? 2 : 3;
      const ra = rank(a.statusCredito);
      const rb = rank(b.statusCredito);
      if (ra !== rb) return ra - rb;

      const da = a.dueAt ? new Date(a.dueAt).getTime() : Number.POSITIVE_INFINITY;
      const db = b.dueAt ? new Date(b.dueAt).getTime() : Number.POSITIVE_INFINITY;
      return da - db;
    });

  res.json({ ok: true, faltantes });
});

// ----- Start
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en http://localhost:${PORT}`);
  console.log(`Batch actual (lunes 00:00): ${getCurrentBatchKey()}`);
});