"use strict";

const { test, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const request = require("supertest");

const { createAuth } = require("../auth");
const { VirtualAuthenticator } = require("./_virtual-authenticator");

const RP_ID = "localhost";
const ORIGIN = "http://localhost";
const MAR = "mar@example.com";
const NETIE = "netie@example.com";

let dir, clock, auth, app, mails, logs;

function build({ allow = [MAR, NETIE], disabled = false, noSender = false } = {}) {
  dir = dir || fs.mkdtempSync(path.join(os.tmpdir(), "auth-test-"));
  mails = [];
  logs = [];
  auth = createAuth({
    dataDir: dir,
    allow,
    rpName: "Test",
    rpID: RP_ID,
    origin: ORIGIN,
    sendMail: noSender ? null : async (m) => mails.push(m),
    disabled,
    now: () => clock,
    log: (m) => logs.push(m),
  });
  app = express();
  app.use(express.json());
  app.use(auth.middleware);
  app.get("/api/secret", (req, res) => res.json({ who: req.authUser }));
  app.get("/", (req, res) => res.send("TABLERO-REAL"));
}

beforeEach(() => {
  dir = null;
  clock = 1_800_000_000_000;
  build();
});

async function pedirCodigo(agent, email) {
  return agent.post("/auth/codigo/pedir").send({ email });
}

const lastCode = () => mails[mails.length - 1] && mails[mails.length - 1].code;

async function emailLogin(agent, email, { code } = {}) {
  await pedirCodigo(agent, email);
  return agent.post("/auth/codigo/entrar").send({ email, code: code || lastCode() });
}

async function enrollFaceId(agent, email, device) {
  const g = await emailLogin(agent, email);
  assert.equal(g.status, 200, JSON.stringify(g.body));
  const o = await agent.post("/auth/register/options").send({ email });
  assert.equal(o.status, 200, JSON.stringify(o.body));
  const v = await agent.post("/auth/register/verify").send({ email, response: device.create(o.body) });
  return { options: o, verify: v };
}

async function faceIdLogin(agent, device, opts) {
  const o = await agent.post("/auth/login/options").send({});
  assert.equal(o.status, 200);
  return agent.post("/auth/login/verify").send({ response: device.get(o.body, opts) });
}

// ---------- Candado general

test("/healthz es público", async () => {
  assert.equal((await request(app).get("/healthz")).status, 200);
});

test("sin sesión: /api responde 401 y no filtra datos", async () => {
  const r = await request(app).get("/api/secret");
  assert.equal(r.status, 401);
  assert.ok(!JSON.stringify(r.body).includes("who"));
});

test("sin sesión: la raíz muestra la pantalla de entrada, no el tablero", async () => {
  const r = await request(app).get("/");
  assert.equal(r.status, 200);
  assert.match(r.text, /código/);
  assert.ok(!r.text.includes("TABLERO-REAL"));
});

test("sin sesión: /auth/me responde 401", async () => {
  assert.equal((await request(app).get("/auth/me")).status, 401);
});

test("AUTH_DISABLED deja pasar todo (interruptor de emergencia)", async () => {
  build({ disabled: true });
  assert.equal((await request(app).get("/api/secret")).status, 200);
});

// ---------- Entrar con código por correo

test("pedir código: correo permitido recibe un código de 6 dígitos", async () => {
  const r = await pedirCodigo(request(app), MAR);
  assert.equal(r.status, 200);
  assert.equal(mails.length, 1);
  assert.equal(mails[0].to, MAR);
  assert.match(mails[0].code, /^\d{6}$/);
});

test("pedir código: correo fuera de la lista responde igual y NO manda nada", async () => {
  const ok = await pedirCodigo(request(app), MAR);
  const no = await pedirCodigo(request(app), "intruso@example.com");
  assert.equal(no.status, 200);
  assert.deepEqual(no.body, ok.body);
  assert.equal(mails.length, 1);
  const raro = await pedirCodigo(request(app), "esto no es un correo");
  assert.equal(raro.status, 200);
  assert.equal(mails.length, 1);
});

test("pedir código: no manda más de 1 por minuto ni más de 5 por hora", async () => {
  await pedirCodigo(request(app), MAR);
  await pedirCodigo(request(app), MAR);
  assert.equal(mails.length, 1);
  for (let i = 0; i < 6; i++) {
    clock += 61 * 1000;
    await pedirCodigo(request(app), MAR);
  }
  assert.equal(mails.length, 5);
});

test("entrar con el código correcto: sesión iniciada e identificada", async () => {
  const agent = request.agent(app);
  const r = await emailLogin(agent, MAR);
  assert.equal(r.status, 200, JSON.stringify(r.body));
  assert.ok(String(r.headers["set-cookie"]).includes("HttpOnly"));
  assert.equal((await agent.get("/auth/me")).body.email, MAR);
  assert.equal((await agent.get("/api/secret")).body.who, MAR);
});

test("entrar: el correo se compara sin importar mayúsculas y espacios en el código", async () => {
  const agent = request.agent(app);
  await pedirCodigo(agent, "MAR@Example.com");
  const c = lastCode();
  const r = await agent.post("/auth/codigo/entrar").send({ email: "mar@example.com", code: ` ${c.slice(0, 3)} ${c.slice(3)} ` });
  assert.equal(r.status, 200);
});

test("entrar: código incorrecto → 401, y 5 fallos queman el código", async () => {
  await pedirCodigo(request(app), MAR);
  const bueno = lastCode();
  const malo = bueno === "000000" ? "111111" : "000000";
  for (let i = 0; i < 5; i++) {
    assert.equal((await request(app).post("/auth/codigo/entrar").send({ email: MAR, code: malo })).status, 401);
  }
  assert.equal((await request(app).post("/auth/codigo/entrar").send({ email: MAR, code: bueno })).status, 401);
});

test("entrar: el código sirve una sola vez", async () => {
  await pedirCodigo(request(app), MAR);
  const c = lastCode();
  assert.equal((await request(app).post("/auth/codigo/entrar").send({ email: MAR, code: c })).status, 200);
  assert.equal((await request(app).post("/auth/codigo/entrar").send({ email: MAR, code: c })).status, 401);
});

test("entrar: el código vence a los 15 minutos", async () => {
  await pedirCodigo(request(app), MAR);
  const c = lastCode();
  clock += 16 * 60 * 1000;
  assert.equal((await request(app).post("/auth/codigo/entrar").send({ email: MAR, code: c })).status, 401);
});

test("entrar: el código de Mar no sirve para entrar como Netie", async () => {
  await pedirCodigo(request(app), MAR);
  const c = lastCode();
  assert.equal((await request(app).post("/auth/codigo/entrar").send({ email: NETIE, code: c })).status, 401);
});

test("entrar: sin correo o sin código → no entra", async () => {
  assert.notEqual((await request(app).post("/auth/codigo/entrar").send({})).status, 200);
  assert.notEqual((await request(app).post("/auth/codigo/entrar").send({ email: MAR })).status, 200);
});

test("un código nuevo invalida el anterior", async () => {
  await pedirCodigo(request(app), MAR);
  const viejo = lastCode();
  clock += 61 * 1000;
  await pedirCodigo(request(app), MAR);
  const nuevo = lastCode();
  if (viejo !== nuevo) assert.equal((await request(app).post("/auth/codigo/entrar").send({ email: MAR, code: viejo })).status, 401);
  assert.equal((await request(app).post("/auth/codigo/entrar").send({ email: MAR, code: nuevo })).status, 200);
});

test("sin servicio de correo configurado el código queda en el log del servidor (arranque)", async () => {
  build({ noSender: true });
  const r = await pedirCodigo(request(app), MAR);
  assert.equal(r.status, 200);
  const line = logs.find((l) => l.includes(MAR) && /\d{6}/.test(l));
  assert.ok(line, "debe registrar el código en el log");
  const code = /(\d{6})/.exec(line)[1];
  assert.equal((await request(app).post("/auth/codigo/entrar").send({ email: MAR, code })).status, 200);
});

test("un fallo del servicio de correo no revela si el correo existe", async () => {
  build();
  auth = null;
  dir = null;
  const a = createAuth({ dataDir: fs.mkdtempSync(path.join(os.tmpdir(), "auth-t2-")), allow: [MAR], rpID: RP_ID, origin: ORIGIN, sendMail: async () => { throw new Error("resend caído"); }, now: () => clock, log: () => {} });
  const ap = express();
  ap.use(express.json());
  ap.use(a.middleware);
  const ok = await request(ap).post("/auth/codigo/pedir").send({ email: MAR });
  const no = await request(ap).post("/auth/codigo/pedir").send({ email: "otro@example.com" });
  assert.equal(ok.status, no.status);
  assert.deepEqual(ok.body, no.body);
});

// ---------- Sesión

test("cookie manipulada o inventada → 401", async () => {
  const agent = request.agent(app);
  const g = await emailLogin(agent, MAR);
  const cookie = String(g.headers["set-cookie"][0]).split(";")[0];
  const [name, value] = cookie.split("=");
  const [payload, sig] = value.split(".");
  const forged = Buffer.from(JSON.stringify({ e: NETIE, x: Math.floor(clock / 1000) + 99999 })).toString("base64url");
  assert.equal((await request(app).get("/api/secret").set("Cookie", `${name}=${forged}.${sig}`)).status, 401);
  assert.equal((await request(app).get("/api/secret").set("Cookie", `${name}=${payload}.AAAA`)).status, 401);
  assert.equal((await request(app).get("/api/secret").set("Cookie", `${name}=basura`)).status, 401);
});

test("la sesión caduca a los 14 días", async () => {
  const agent = request.agent(app);
  await emailLogin(agent, MAR);
  assert.equal((await agent.get("/api/secret")).status, 200);
  clock += 15 * 24 * 60 * 60 * 1000;
  assert.equal((await agent.get("/api/secret")).status, 401);
});

test("cerrar sesión borra la cookie", async () => {
  const agent = request.agent(app);
  await emailLogin(agent, MAR);
  await agent.post("/auth/logout");
  assert.equal((await agent.get("/api/secret")).status, 401);
});

test("si quitan a alguien de la lista, su sesión deja de servir", async () => {
  const g = await emailLogin(request.agent(app), MAR);
  const cookie = String(g.headers["set-cookie"][0]).split(";")[0];
  build({ allow: [NETIE] });
  assert.equal((await request(app).get("/api/secret").set("Cookie", cookie)).status, 401);
  assert.equal((await emailLogin(request.agent(app), MAR)).status, 401); // ya no se le manda código
  assert.equal(mails.length, 0);
});

// ---------- Face ID (se activa ya dentro, con sesión)

test("activar Face ID sin sesión → 403", async () => {
  const r = await request(app).post("/auth/register/options").send({ email: MAR });
  assert.equal(r.status, 403);
});

test("con la sesión de Mar no se puede registrar Face ID a nombre de Netie", async () => {
  const agent = request.agent(app);
  await emailLogin(agent, MAR);
  assert.equal((await agent.post("/auth/register/options").send({ email: NETIE })).status, 403);
});

test("activar Face ID: pide la llave del propio teléfono (platform), no una llave externa ni QR", async () => {
  const agent = request.agent(app);
  await emailLogin(agent, MAR);
  const o = await agent.post("/auth/register/options").send({ email: MAR });
  assert.equal(o.body.authenticatorSelection.authenticatorAttachment, "platform");
  assert.equal(o.body.authenticatorSelection.userVerification, "required");
});

test("activar Face ID y después entrar solo con Face ID", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const { verify } = await enrollFaceId(request.agent(app), MAR, device);
  assert.equal(verify.status, 200, JSON.stringify(verify.body));

  const fresh = request.agent(app);
  assert.equal((await fresh.get("/api/secret")).status, 401);
  const l = await faceIdLogin(fresh, device);
  assert.equal(l.status, 200, JSON.stringify(l.body));
  assert.equal((await fresh.get("/api/secret")).body.who, MAR);
});

test("el registro verifica el reto: un reto ajeno no sirve", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const agent = request.agent(app);
  await emailLogin(agent, MAR);
  const o = await agent.post("/auth/register/options").send({ email: MAR });
  const forged = device.create({ ...o.body, challenge: crypto.randomBytes(32).toString("base64url") });
  const v = await agent.post("/auth/register/verify").send({ email: MAR, response: forged });
  assert.notEqual(v.status, 200);
});

test("el registro de Face ID exige sesión también al verificar", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const agent = request.agent(app);
  await emailLogin(agent, MAR);
  const o = await agent.post("/auth/register/options").send({ email: MAR });
  const v = await request(app).post("/auth/register/verify").send({ email: MAR, response: device.create(o.body) });
  assert.equal(v.status, 403);
});

test("un reto de Face ID no se puede reusar (replay)", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enrollFaceId(request.agent(app), MAR, device);
  const o = await request(app).post("/auth/login/options").send({});
  const response = device.get(o.body);
  assert.equal((await request(app).post("/auth/login/verify").send({ response })).status, 200);
  assert.equal((await request(app).post("/auth/login/verify").send({ response })).status, 401);
});

test("una llave distinta que finge ser la de Mar → 401", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enrollFaceId(request.agent(app), MAR, device);
  const attacker = crypto.generateKeyPairSync("ec", { namedCurve: "P-256" }).privateKey;
  assert.equal((await faceIdLogin(request.agent(app), device, { privateKey: attacker })).status, 401);
});

test("si quitan a alguien de la lista, su Face ID deja de servir", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enrollFaceId(request.agent(app), MAR, device);
  build({ allow: [NETIE] });
  assert.equal((await faceIdLogin(request.agent(app), device)).status, 403);
});

test("las llaves sobreviven un reinicio", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enrollFaceId(request.agent(app), MAR, device);
  build(); // mismo disco
  assert.equal((await faceIdLogin(request.agent(app), device)).status, 200);
});

test("se puede activar Face ID en un segundo dispositivo", async () => {
  const phone = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const mac = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enrollFaceId(request.agent(app), MAR, phone);
  clock += 61 * 1000; // el siguiente código se puede pedir pasado 1 minuto
  await enrollFaceId(request.agent(app), MAR, mac);
  assert.equal((await faceIdLogin(request.agent(app), phone)).status, 200);
  assert.equal((await faceIdLogin(request.agent(app), mac)).status, 200);
});

test("el archivo de llaves no guarda llave privada ni código de invitación", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enrollFaceId(request.agent(app), MAR, device);
  const raw = fs.readFileSync(path.join(dir, "auth.json"), "utf8");
  assert.ok(!raw.includes("PRIVATE"));
  assert.ok(raw.includes(MAR));
});

