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

let dir, logs, clock, auth, app;

function build({ allow = [MAR, NETIE], disabled = false } = {}) {
  dir = dir || fs.mkdtempSync(path.join(os.tmpdir(), "auth-test-"));
  logs = [];
  auth = createAuth({
    dataDir: dir,
    allow,
    rpName: "Test",
    rpID: RP_ID,
    origin: ORIGIN,
    baseUrl: "http://localhost",
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

const codeFor = (email) => {
  const line = logs.find((l) => l.includes(encodeURIComponent(email)));
  return line && /code=([A-Z0-9-]+)/.exec(line)[1];
};

async function enroll(agent, email, device) {
  const code = codeFor(email);
  const o = await agent.post("/auth/register/options").send({ email, code });
  assert.equal(o.status, 200, JSON.stringify(o.body));
  const v = await agent.post("/auth/register/verify").send({ email, response: device.create(o.body) });
  return { options: o, verify: v, code };
}

async function login(agent, device, opts) {
  const o = await agent.post("/auth/login/options").send({});
  assert.equal(o.status, 200);
  const response = device.get(o.body, opts);
  return agent.post("/auth/login/verify").send({ response });
}

test("/healthz es público", async () => {
  const r = await request(app).get("/healthz");
  assert.equal(r.status, 200);
});

test("sin sesión: /api responde 401 y no filtra datos", async () => {
  const r = await request(app).get("/api/secret");
  assert.equal(r.status, 401);
  assert.ok(!JSON.stringify(r.body).includes("who"));
});

test("sin sesión: la raíz muestra la pantalla de Face ID, no el tablero", async () => {
  const r = await request(app).get("/");
  assert.equal(r.status, 200);
  assert.match(r.text, /Face ID/);
  assert.ok(!r.text.includes("TABLERO-REAL"));
});

test("sin sesión: /auth/me responde 401", async () => {
  const r = await request(app).get("/auth/me");
  assert.equal(r.status, 401);
});

test("al arrancar emite invitación con enlace para cada correo sin Face ID", () => {
  assert.ok(codeFor(MAR), "falta invitación de Mar");
  assert.ok(codeFor(NETIE), "falta invitación de Netie");
  assert.match(logs.join("\n"), /\/auth\/registro\?email=/);
});

test("registro sin código o con código malo → 403", async () => {
  const sin = await request(app).post("/auth/register/options").send({ email: MAR });
  assert.equal(sin.status, 403);
  const malo = await request(app).post("/auth/register/options").send({ email: MAR, code: "AAAA-AAAA" });
  assert.equal(malo.status, 403);
});

test("registro con correo fuera de la lista → 403", async () => {
  const r = await request(app).post("/auth/register/options").send({ email: "intruso@example.com", code: codeFor(MAR) });
  assert.equal(r.status, 403);
});

test("el código de Mar no sirve para registrar a Netie", async () => {
  const r = await request(app).post("/auth/register/options").send({ email: NETIE, code: codeFor(MAR) });
  assert.equal(r.status, 403);
});

test("5 intentos con código malo bloquean aun con el código correcto → 429", async () => {
  const good = codeFor(MAR);
  for (let i = 0; i < 5; i++) {
    await request(app).post("/auth/register/options").send({ email: MAR, code: "ZZZZ-ZZZZ" });
  }
  const r = await request(app).post("/auth/register/options").send({ email: MAR, code: good });
  assert.equal(r.status, 429);
  clock += 16 * 60 * 1000;
  const later = await request(app).post("/auth/register/options").send({ email: MAR, code: good });
  assert.equal(later.status, 200);
});

test("invitación vencida (más de 24 h) → 403", async () => {
  const code = codeFor(MAR);
  clock += 25 * 60 * 60 * 1000;
  const r = await request(app).post("/auth/register/options").send({ email: MAR, code });
  assert.equal(r.status, 403);
});

test("flujo completo: registrar, entrar con la sesión, la invitación se consume", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const agent = request.agent(app);
  const { verify, code } = await enroll(agent, MAR, device);
  assert.equal(verify.status, 200, JSON.stringify(verify.body));
  assert.ok(String(verify.headers["set-cookie"]).includes("HttpOnly"));

  const me = await agent.get("/auth/me");
  assert.equal(me.status, 200);
  assert.equal(me.body.email, MAR);

  const secret = await agent.get("/api/secret");
  assert.equal(secret.body.who, MAR);

  const reuse = await request(app).post("/auth/register/options").send({ email: MAR, code });
  assert.equal(reuse.status, 403);
});

test("el registro verifica el reto: un reto ajeno no sirve", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const o = await request(app).post("/auth/register/options").send({ email: MAR, code: codeFor(MAR) });
  const forged = device.create({ ...o.body, challenge: crypto.randomBytes(32).toString("base64url") });
  const v = await request(app).post("/auth/register/verify").send({ email: MAR, response: forged });
  assert.notEqual(v.status, 200);
});

test("entrar con Face ID después de cerrar sesión", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const first = request.agent(app);
  await enroll(first, MAR, device);

  const fresh = request.agent(app);
  assert.equal((await fresh.get("/api/secret")).status, 401);
  const l = await login(fresh, device);
  assert.equal(l.status, 200, JSON.stringify(l.body));
  assert.equal((await fresh.get("/api/secret")).body.who, MAR);

  await fresh.post("/auth/logout");
  assert.equal((await fresh.get("/api/secret")).status, 401);
});

test("un reto de login no se puede reusar (replay)", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enroll(request.agent(app), MAR, device);
  const o = await request(app).post("/auth/login/options").send({});
  const response = device.get(o.body);
  const a = await request(app).post("/auth/login/verify").send({ response });
  assert.equal(a.status, 200);
  const b = await request(app).post("/auth/login/verify").send({ response });
  assert.equal(b.status, 401);
});

test("una llave distinta que finge ser la de Mar → 401", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enroll(request.agent(app), MAR, device);
  const attacker = crypto.generateKeyPairSync("ec", { namedCurve: "P-256" }).privateKey;
  const l = await login(request.agent(app), device, { privateKey: attacker });
  assert.equal(l.status, 401);
});

test("cookie manipulada o inventada → 401", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const agent = request.agent(app);
  const { verify } = await enroll(agent, MAR, device);
  const cookie = String(verify.headers["set-cookie"][0]).split(";")[0];
  const [name, value] = cookie.split("=");
  const [payload, sig] = value.split(".");
  const forgedPayload = Buffer.from(JSON.stringify({ e: NETIE, x: Math.floor(clock / 1000) + 99999 })).toString("base64url");
  const r1 = await request(app).get("/api/secret").set("Cookie", `${name}=${forgedPayload}.${sig}`);
  assert.equal(r1.status, 401);
  const r2 = await request(app).get("/api/secret").set("Cookie", `${name}=${payload}.AAAA`);
  assert.equal(r2.status, 401);
  const r3 = await request(app).get("/api/secret").set("Cookie", `${name}=basura`);
  assert.equal(r3.status, 401);
});

test("la sesión caduca a los 14 días", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const agent = request.agent(app);
  await enroll(agent, MAR, device);
  assert.equal((await agent.get("/api/secret")).status, 200);
  clock += 15 * 24 * 60 * 60 * 1000;
  assert.equal((await agent.get("/api/secret")).status, 401);
});

test("si quitan a alguien de la lista, su sesión y su llave dejan de servir", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const { verify } = await enroll(request.agent(app), MAR, device);
  const cookie = String(verify.headers["set-cookie"][0]).split(";")[0];
  build({ allow: [NETIE] }); // mismo disco, Mar ya no está permitida
  const session = await request(app).get("/api/secret").set("Cookie", cookie);
  assert.equal(session.status, 401);
  const l = await login(request.agent(app), device);
  assert.equal(l.status, 403);
});

test("las llaves sobreviven un reinicio y ya no se vuelve a invitar a quien se registró", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enroll(request.agent(app), MAR, device);
  build(); // reinicio con el mismo disco
  assert.equal(codeFor(MAR), undefined, "Mar ya tiene Face ID, no necesita invitación");
  assert.ok(codeFor(NETIE), "Netie sigue pendiente");
  const l = await login(request.agent(app), device);
  assert.equal(l.status, 200);
});

test("agregar un segundo dispositivo requiere sesión; sin sesión → 403", async () => {
  const phone = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const agent = request.agent(app);
  await enroll(agent, MAR, phone);

  const sinSesion = await request(app).post("/auth/register/options").send({ email: MAR });
  assert.equal(sinSesion.status, 403);

  const mac = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  const o = await agent.post("/auth/register/options").send({ email: MAR });
  assert.equal(o.status, 200);
  const v = await agent.post("/auth/register/verify").send({ email: MAR, response: mac.create(o.body) });
  assert.equal(v.status, 200);
  assert.equal((await login(request.agent(app), mac)).status, 200);
});

test("AUTH_DISABLED deja pasar todo (interruptor de emergencia)", async () => {
  build({ disabled: true });
  const r = await request(app).get("/api/secret");
  assert.equal(r.status, 200);
});

test("el archivo de llaves no guarda la llave privada ni contraseñas", async () => {
  const device = new VirtualAuthenticator({ rpID: RP_ID, origin: ORIGIN });
  await enroll(request.agent(app), MAR, device);
  const raw = fs.readFileSync(path.join(dir, "auth.json"), "utf8");
  assert.ok(!raw.includes("PRIVATE"));
  assert.ok(raw.includes(MAR));
});
