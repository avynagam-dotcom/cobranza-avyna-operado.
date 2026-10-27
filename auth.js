"use strict";

// Acceso a los tableros de cobranza: la persona escribe su correo (solo los de
// auth-allow.json), recibe un código de 6 dígitos y entra. Ya adentro puede
// activar Face ID / Touch ID (passkeys WebAuthn) en su teléfono para entrar con
// un toque la próxima vez. Mismo patrón que Distribución Total.
// Sin base de datos ni contraseñas: la sesión es una cookie firmada y las
// llaves públicas de Face ID viven en <dataDir>/auth.json (disco de Render).

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const {
  generateRegistrationOptions,
  verifyRegistrationResponse,
  generateAuthenticationOptions,
  verifyAuthenticationResponse,
} = require("@simplewebauthn/server");

const COOKIE = "avyna_sesion";
const SESSION_MS = 14 * 24 * 60 * 60 * 1000;
const CHALLENGE_MS = 5 * 60 * 1000;
const CODE_MS = 15 * 60 * 1000;
const CODE_MAX_FAILS = 5;
const CODE_MIN_GAP_MS = 60 * 1000;
const CODE_MAX_PER_HOUR = 5;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const sha256 = (v) => crypto.createHash("sha256").update(v).digest();
const norm = (e) => String(e || "").trim().toLowerCase();

function sameString(a, b) {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}

// ----- Envío del código por correo con Resend (misma convención que Distribución
// Total: RESEND_API_KEY y CORREO_REMITENTE en el entorno del servicio).
function resendSender({ apiKey, remitente, rpName = "Avyna Cobranza", fetchFn = fetch }) {
  return async ({ to, code }) => {
    const text = `Tu código para entrar a ${rpName}: ${code}\n\nEscríbelo en la misma pantalla donde lo pediste. Sirve una sola vez y vence en 15 minutos.\nSi no lo pediste, ignora este correo.`;
    const html = `<!doctype html><html lang="es"><body style="font-family:system-ui,sans-serif;color:#1c1b18;max-width:480px;margin:auto;padding:24px"><p style="font-size:18px;font-weight:700;margin:0 0 16px">${rpName}</p><p>Tu código para entrar:</p><p style="font-size:32px;font-weight:700;letter-spacing:6px;margin:8px 0 4px">${code}</p><p style="font-size:13px;color:#6b665c">Escríbelo en la misma pantalla donde lo pediste. Sirve una sola vez y vence en 15 minutos. Si no lo pediste, ignora este correo.</p></body></html>`;
    const r = await fetchFn("https://api.resend.com/emails", {
      method: "POST",
      headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
      body: JSON.stringify({ from: remitente, to: [to], subject: `${code} es tu código para entrar a ${rpName}`, text, html }),
    });
    if (!r.ok) throw new Error(`Resend respondió ${r.status}`);
  };
}

function createAuth(opts) {
  const {
    dataDir,
    rpName = "Avyna Cobranza",
    disabled = false,
    now = () => Date.now(),
    log = (m) => console.log(m),
  } = opts;
  const allow = (opts.allow || []).map(norm);
  const isAllowed = (email) => allow.includes(norm(email));
  // sendMail: función({to, code}). undefined = usar Resend si hay llaves en el
  // entorno; null = sin correo: el código se escribe en el log del servidor
  // (solo lo ve quien tenga acceso a Render).
  const sendMail =
    opts.sendMail !== undefined
      ? opts.sendMail
      : process.env.RESEND_API_KEY && process.env.CORREO_REMITENTE
        ? resendSender({ apiKey: process.env.RESEND_API_KEY, remitente: process.env.CORREO_REMITENTE, rpName })
        : null;
  const file = path.join(dataDir, "auth.json");
  const loginHtml = fs.readFileSync(path.join(__dirname, "auth-login.html"), "utf8");

  // ----- Estado en disco
  let state = { secret: null, users: {} };
  try {
    state = { ...state, ...JSON.parse(fs.readFileSync(file, "utf8")) };
  } catch {
    /* primera vez */
  }
  function save() {
    fs.mkdirSync(dataDir, { recursive: true });
    const tmp = `${file}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(state, null, 2), { mode: 0o600 });
    fs.renameSync(tmp, file);
  }
  if (!state.secret) {
    state.secret = crypto.randomBytes(32).toString("base64url");
    save();
  }
  const credsOf = (email) => (state.users[norm(email)] && state.users[norm(email)].credentials) || [];

  // ----- Sesión (cookie firmada, sin estado en servidor)
  const sign = (payload) => crypto.createHmac("sha256", state.secret).update(payload).digest("base64url");

  function cookies(req) {
    const out = {};
    for (const part of String(req.headers.cookie || "").split(";")) {
      const i = part.indexOf("=");
      if (i > 0) out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
    }
    return out;
  }

  function readSession(req) {
    const raw = cookies(req)[COOKIE];
    if (!raw) return null;
    const [payload, sig] = raw.split(".");
    if (!payload || !sig || !sameString(sig, sign(payload))) return null;
    try {
      const { e, x } = JSON.parse(Buffer.from(payload, "base64url").toString());
      if (!e || !x || x * 1000 <= now()) return null;
      return isAllowed(e) ? norm(e) : null;
    } catch {
      return null;
    }
  }

  function isSecure(req) {
    return req.secure || String(req.headers["x-forwarded-proto"] || "").split(",")[0] === "https";
  }

  function setSession(req, res, email) {
    const payload = Buffer.from(JSON.stringify({ e: norm(email), x: Math.floor((now() + SESSION_MS) / 1000) })).toString("base64url");
    const flags = ["Path=/", "HttpOnly", "SameSite=Lax", `Max-Age=${SESSION_MS / 1000}`];
    if (isSecure(req)) flags.push("Secure");
    res.setHeader("Set-Cookie", `${COOKIE}=${payload}.${sign(payload)}; ${flags.join("; ")}`);
  }

  // ----- Retos WebAuthn de un solo uso (en memoria, 5 min)
  const challenges = new Map();
  function putChallenge(key, data) {
    for (const [k, v] of challenges) if (v.exp <= now()) challenges.delete(k);
    challenges.set(key, { ...data, exp: now() + CHALLENGE_MS });
  }
  function takeChallenge(key, type) {
    const entry = challenges.get(key);
    if (!entry) return null;
    challenges.delete(key);
    return entry.type === type && entry.exp > now() ? { key, ...entry } : null;
  }
  function takeFromResponse(response, type) {
    try {
      const cd = JSON.parse(Buffer.from(response.response.clientDataJSON, "base64url").toString());
      return takeChallenge(cd.challenge, type);
    } catch {
      return null;
    }
  }

  const rpIDOf = (req) => opts.rpID || process.env.RENDER_EXTERNAL_HOSTNAME || req.hostname;
  const originOf = (req) => opts.origin || `https://${rpIDOf(req)}`;
  const fail = (res, code, msg) => res.status(code).json({ error: msg });

  // ----- Rutas
  const router = express.Router();
  router.get("/healthz", (req, res) => res.type("text").send("ok"));

  if (disabled) return { middleware: router, isAllowed };

  router.get("/auth/dispositivo", (req, res) => res.type("html").send(loginHtml));

  router.get("/auth/me", (req, res) => {
    const email = readSession(req);
    return email ? res.json({ email }) : fail(res, 401, "Sin sesión");
  });

  router.post("/auth/logout", (req, res) => {
    res.setHeader("Set-Cookie", `${COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`);
    res.json({ ok: true });
  });

  // -- Entrar con código por correo
  const codes = new Map(); // email -> { hash, exp, fails, sent: [timestamps] }
  const hashCode = (code) => crypto.createHmac("sha256", state.secret).update(String(code)).digest("hex");
  const digits = (v) => String(v || "").replace(/\D/g, "");

  router.post("/auth/codigo/pedir", async (req, res) => {
    const generic = { ok: true, mensaje: "Si tu correo tiene acceso, te llegó un código." };
    const email = norm(req.body && req.body.email);
    if (!EMAIL_RE.test(email) || !isAllowed(email)) return res.json(generic);

    const entry = codes.get(email) || { sent: [] };
    entry.sent = entry.sent.filter((t) => now() - t < 60 * 60 * 1000);
    const last = entry.sent[entry.sent.length - 1];
    if ((last !== undefined && now() - last < CODE_MIN_GAP_MS) || entry.sent.length >= CODE_MAX_PER_HOUR) {
      codes.set(email, entry);
      return res.json(generic);
    }
    const code = String(crypto.randomInt(0, 1000000)).padStart(6, "0");
    entry.hash = hashCode(code);
    entry.exp = now() + CODE_MS;
    entry.fails = 0;
    entry.sent.push(now());
    codes.set(email, entry);
    try {
      if (sendMail) await sendMail({ to: email, code });
      else log(`[AUTH] código de acceso para ${email}: ${code} (sin servicio de correo configurado)`);
    } catch (err) {
      log(`[AUTH] no se pudo enviar el código a ${email}: ${err.message}`);
    }
    res.json(generic);
  });

  router.post("/auth/codigo/entrar", (req, res) => {
    const email = norm(req.body && req.body.email);
    const code = digits(req.body && req.body.code);
    const entry = codes.get(email);
    const bad = () => fail(res, 401, "Código incorrecto o vencido");
    if (!isAllowed(email) || !entry || !entry.hash || entry.exp <= now() || code.length !== 6) return bad();
    if (!sameString(hashCode(code), entry.hash)) {
      entry.fails += 1;
      if (entry.fails >= CODE_MAX_FAILS) entry.hash = null;
      return bad();
    }
    entry.hash = null; // un solo uso
    setSession(req, res, email);
    res.json({ ok: true, email });
  });

  // -- Face ID: solo se activa con la sesión ya iniciada
  router.post("/auth/register/options", async (req, res) => {
    try {
      const email = norm(req.body && req.body.email);
      if (!isAllowed(email) || readSession(req) !== email) return fail(res, 403, "Primero entra con tu correo");
      const options = await generateRegistrationOptions({
        rpName,
        rpID: rpIDOf(req),
        userName: email,
        userID: sha256(email),
        attestationType: "none",
        excludeCredentials: credsOf(email).map((c) => ({ id: c.id, transports: c.transports })),
        authenticatorSelection: { authenticatorAttachment: "platform", residentKey: "required", userVerification: "required" },
      });
      putChallenge(options.challenge, { type: "reg", email });
      res.json(options);
    } catch (err) {
      log(`[AUTH] error en register/options: ${err.message}`);
      fail(res, 500, "No se pudo iniciar la activación");
    }
  });

  router.post("/auth/register/verify", async (req, res) => {
    try {
      const email = norm(req.body && req.body.email);
      const response = req.body && req.body.response;
      if (!isAllowed(email) || readSession(req) !== email) return fail(res, 403, "Primero entra con tu correo");
      const entry = response && takeFromResponse(response, "reg");
      if (!entry || entry.email !== email) return fail(res, 403, "Registro no válido");

      const v = await verifyRegistrationResponse({
        response,
        expectedChallenge: entry.key,
        expectedOrigin: originOf(req),
        expectedRPID: rpIDOf(req),
        requireUserVerification: true,
      });
      if (!v.verified) return fail(res, 403, "Registro no válido");

      const cred = v.registrationInfo.credential;
      const user = (state.users[email] = state.users[email] || { credentials: [] });
      user.credentials.push({
        id: cred.id,
        publicKey: Buffer.from(cred.publicKey).toString("base64url"),
        counter: cred.counter,
        transports: cred.transports || response.response.transports || [],
        createdAt: new Date(now()).toISOString(),
      });
      save();
      log(`[AUTH] Face ID activado para ${email}`);
      res.json({ ok: true, email });
    } catch (err) {
      log(`[AUTH] error en register/verify: ${err.message}`);
      fail(res, 403, "Registro no válido");
    }
  });

  // -- Entrar con Face ID
  router.post("/auth/login/options", async (req, res) => {
    try {
      const options = await generateAuthenticationOptions({ rpID: rpIDOf(req), userVerification: "required" });
      putChallenge(options.challenge, { type: "auth" });
      res.json(options);
    } catch (err) {
      log(`[AUTH] error en login/options: ${err.message}`);
      fail(res, 500, "No se pudo iniciar el acceso");
    }
  });

  router.post("/auth/login/verify", async (req, res) => {
    try {
      const response = req.body && req.body.response;
      const entry = response && takeFromResponse(response, "auth");
      if (!entry) return fail(res, 401, "No se pudo verificar");

      let owner = null;
      let cred = null;
      for (const [email, u] of Object.entries(state.users)) {
        const found = u.credentials.find((c) => c.id === response.id);
        if (found) {
          owner = email;
          cred = found;
        }
      }
      if (!cred) return fail(res, 401, "No se pudo verificar");

      const v = await verifyAuthenticationResponse({
        response,
        expectedChallenge: entry.key,
        expectedOrigin: originOf(req),
        expectedRPID: rpIDOf(req),
        credential: {
          id: cred.id,
          publicKey: new Uint8Array(Buffer.from(cred.publicKey, "base64url")),
          counter: cred.counter,
          transports: cred.transports,
        },
        requireUserVerification: true,
      });
      if (!v.verified) return fail(res, 401, "No se pudo verificar");
      if (!isAllowed(owner)) return fail(res, 403, "Sin acceso a este tablero");

      cred.counter = v.authenticationInfo.newCounter;
      save();
      setSession(req, res, owner);
      res.json({ ok: true, email: owner });
    } catch (err) {
      log(`[AUTH] error en login/verify: ${err.message}`);
      fail(res, 401, "No se pudo verificar");
    }
  });

  // ----- Candado: todo lo demás exige sesión
  router.use((req, res, next) => {
    if (req.path.startsWith("/auth/")) return next();
    const email = readSession(req);
    if (email) {
      req.authUser = email;
      return next();
    }
    if (req.method === "GET" && req.path === "/apple-touch-icon.png") return next();
    if (req.method === "GET" && (req.path === "/" || req.path === "/index.html")) {
      return res.type("html").send(loginHtml);
    }
    res.status(401).json({ error: "Inicia sesión" });
  });

  return { middleware: router, isAllowed };
}

module.exports = { createAuth };
