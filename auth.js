"use strict";

// Login con Face ID / Touch ID (passkeys WebAuthn) para los tableros de cobranza.
// Un solo archivo, sin base de datos: las llaves públicas viven en
// <dataDir>/auth.json (disco persistente de Render). Nunca guarda contraseñas
// ni llaves privadas: la llave privada se queda dentro del teléfono.

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
const INVITE_MS = 24 * 60 * 60 * 1000;
const CHALLENGE_MS = 5 * 60 * 1000;
const LOCK_MS = 15 * 60 * 1000;
const MAX_FAILS = 5;
const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

const sha256 = (v) => crypto.createHash("sha256").update(v).digest();
const norm = (e) => String(e || "").trim().toLowerCase();

function newCode() {
  let s = "";
  for (let i = 0; i < 8; i++) s += ALPHABET[crypto.randomInt(ALPHABET.length)];
  return `${s.slice(0, 4)}-${s.slice(4)}`;
}

const normCode = (c) => String(c || "").toUpperCase().replace(/[^A-Z0-9]/g, "");

function sameString(a, b) {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
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
  const file = path.join(dataDir, "auth.json");
  const loginHtml = fs.readFileSync(path.join(__dirname, "auth-login.html"), "utf8");

  // ----- Estado en disco
  let state = { secret: null, users: {}, invites: {} };
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

  // ----- Invitaciones (un solo uso, 24 h, atadas a un correo permitido)
  function baseUrl(req) {
    if (opts.baseUrl) return opts.baseUrl;
    if (process.env.RENDER_EXTERNAL_URL) return process.env.RENDER_EXTERNAL_URL;
    return req ? `${req.protocol}://${req.get("host")}` : "";
  }

  function issueInvite(email) {
    const e = norm(email);
    const code = newCode();
    state.invites[e] = { code, exp: now() + INVITE_MS, fails: 0, lockedUntil: 0 };
    save();
    log(`[AUTH] Invitación para ${e}: ${baseUrl()}/auth/registro?email=${encodeURIComponent(e)}&code=${code}`);
    return code;
  }

  for (const email of allow) {
    const inv = state.invites[email];
    if (credsOf(email).length === 0) {
      if (!inv || inv.exp <= now()) issueInvite(email);
      else log(`[AUTH] Invitación vigente para ${email}: ${baseUrl()}/auth/registro?email=${encodeURIComponent(email)}&code=${inv.code}`);
    }
  }

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

  // ----- Retos WebAuthn (en memoria, 5 min, un solo uso)
  const challenges = new Map();
  function putChallenge(challenge, data) {
    for (const [k, v] of challenges) if (v.exp <= now()) challenges.delete(k);
    challenges.set(challenge, { ...data, exp: now() + CHALLENGE_MS });
  }
  function takeChallenge(response, type) {
    try {
      const cd = JSON.parse(Buffer.from(response.response.clientDataJSON, "base64url").toString());
      const entry = challenges.get(cd.challenge);
      if (!entry) return null;
      challenges.delete(cd.challenge);
      return entry.type === type && entry.exp > now() ? { challenge: cd.challenge, ...entry } : null;
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

  if (disabled) {
    router.use((req, res, next) => next());
    return { middleware: router, issueInvite, isAllowed };
  }

  router.get("/auth/registro", (req, res) => res.type("html").send(loginHtml));

  router.get("/auth/me", (req, res) => {
    const email = readSession(req);
    return email ? res.json({ email }) : fail(res, 401, "Sin sesión");
  });

  router.post("/auth/logout", (req, res) => {
    res.setHeader("Set-Cookie", `${COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`);
    res.json({ ok: true });
  });

  router.post("/auth/register/options", async (req, res) => {
    try {
      const email = norm(req.body && req.body.email);
      if (!isAllowed(email)) return fail(res, 403, "Invitación no válida");

      const session = readSession(req);
      const viaSession = session === email;
      if (!viaSession) {
        const inv = state.invites[email];
        if (inv && inv.lockedUntil > now()) return fail(res, 429, "Demasiados intentos. Espera unos minutos.");
        const ok = inv && inv.exp > now() && sameString(normCode(inv.code), normCode(req.body.code));
        if (!ok) {
          if (inv) {
            inv.fails += 1;
            if (inv.fails >= MAX_FAILS) {
              inv.fails = 0;
              inv.lockedUntil = now() + LOCK_MS;
            }
            save();
          }
          return fail(res, 403, "Invitación no válida");
        }
      }

      const options = await generateRegistrationOptions({
        rpName,
        rpID: rpIDOf(req),
        userName: email,
        userID: sha256(email),
        attestationType: "none",
        excludeCredentials: credsOf(email).map((c) => ({ id: c.id, transports: c.transports })),
        authenticatorSelection: { residentKey: "required", userVerification: "required" },
      });
      putChallenge(options.challenge, { type: "reg", email, viaSession });
      res.json(options);
    } catch (err) {
      log(`[AUTH] error en register/options: ${err.message}`);
      fail(res, 500, "No se pudo iniciar el registro");
    }
  });

  router.post("/auth/register/verify", async (req, res) => {
    try {
      const email = norm(req.body && req.body.email);
      const response = req.body && req.body.response;
      const entry = response && takeChallenge(response, "reg");
      if (!entry || entry.email !== email || !isAllowed(email)) return fail(res, 403, "Registro no válido");
      if (!entry.viaSession) {
        const inv = state.invites[email];
        if (!inv || inv.exp <= now()) return fail(res, 403, "Invitación no válida");
      }

      const v = await verifyRegistrationResponse({
        response,
        expectedChallenge: entry.challenge,
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
      delete state.invites[email];
      save();
      setSession(req, res, email);
      log(`[AUTH] Face ID registrado para ${email}`);
      res.json({ ok: true, email });
    } catch (err) {
      log(`[AUTH] error en register/verify: ${err.message}`);
      fail(res, 403, "Registro no válido");
    }
  });

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
      const entry = response && takeChallenge(response, "auth");
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
        expectedChallenge: entry.challenge,
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
    res.status(401).json({ error: "Inicia sesión con Face ID" });
  });

  return { middleware: router, issueInvite, isAllowed };
}

module.exports = { createAuth };
