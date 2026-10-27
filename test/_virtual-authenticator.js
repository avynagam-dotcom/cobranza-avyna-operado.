"use strict";

// Autenticador virtual para pruebas: hace lo que haría el Face ID del iPhone
// (crea una llave ES256 y firma retos) para poder probar el flujo WebAuthn
// completo contra el servidor sin un dispositivo real.

const crypto = require("crypto");

function head(major, n) {
  if (n < 24) return Buffer.from([(major << 5) | n]);
  if (n < 256) return Buffer.from([(major << 5) | 24, n]);
  if (n < 65536) {
    const b = Buffer.alloc(3);
    b[0] = (major << 5) | 25;
    b.writeUInt16BE(n, 1);
    return b;
  }
  const b = Buffer.alloc(5);
  b[0] = (major << 5) | 26;
  b.writeUInt32BE(n, 1);
  return b;
}

function cbor(v) {
  if (Buffer.isBuffer(v)) return Buffer.concat([head(2, v.length), v]);
  if (typeof v === "string") {
    const b = Buffer.from(v);
    return Buffer.concat([head(3, b.length), b]);
  }
  if (typeof v === "number") return v >= 0 ? head(0, v) : head(1, -1 - v);
  const entries = v instanceof Map ? [...v.entries()] : Object.entries(v);
  const parts = [head(5, entries.length)];
  for (const [k, x] of entries) parts.push(cbor(k), cbor(x));
  return Buffer.concat(parts);
}

const sha256 = (b) => crypto.createHash("sha256").update(b).digest();
const b64u = (b) => Buffer.from(b).toString("base64url");

class VirtualAuthenticator {
  constructor({ rpID, origin }) {
    this.rpID = rpID;
    this.origin = origin;
    this.counter = 0;
    this.credId = crypto.randomBytes(16);
    const { publicKey, privateKey } = crypto.generateKeyPairSync("ec", { namedCurve: "P-256" });
    this.privateKey = privateKey;
    this.publicJwk = publicKey.export({ format: "jwk" });
  }

  // Equivale a navigator.credentials.create() con las opciones del servidor.
  create(options) {
    const cose = new Map([
      [1, 2],
      [3, -7],
      [-1, 1],
      [-2, Buffer.from(this.publicJwk.x, "base64url")],
      [-3, Buffer.from(this.publicJwk.y, "base64url")],
    ]);
    const credIdLen = Buffer.alloc(2);
    credIdLen.writeUInt16BE(this.credId.length);
    const counter = Buffer.alloc(4);
    const authData = Buffer.concat([
      sha256(this.rpID),
      Buffer.from([0x45]), // UP + UV + AT
      counter,
      Buffer.alloc(16), // aaguid
      credIdLen,
      this.credId,
      cbor(cose),
    ]);
    const clientDataJSON = Buffer.from(
      JSON.stringify({ type: "webauthn.create", challenge: options.challenge, origin: this.origin, crossOrigin: false })
    );
    const attestationObject = cbor({ fmt: "none", attStmt: {}, authData });
    return {
      id: b64u(this.credId),
      rawId: b64u(this.credId),
      type: "public-key",
      response: { clientDataJSON: b64u(clientDataJSON), attestationObject: b64u(attestationObject), transports: ["internal"] },
      clientExtensionResults: {},
      authenticatorAttachment: "platform",
    };
  }

  // Equivale a navigator.credentials.get() con las opciones del servidor.
  get(options, { privateKey } = {}) {
    this.counter += 1;
    const counter = Buffer.alloc(4);
    counter.writeUInt32BE(this.counter);
    const authData = Buffer.concat([sha256(this.rpID), Buffer.from([0x05]), counter]); // UP + UV
    const clientDataJSON = Buffer.from(
      JSON.stringify({ type: "webauthn.get", challenge: options.challenge, origin: this.origin, crossOrigin: false })
    );
    const signature = crypto.sign("sha256", Buffer.concat([authData, sha256(clientDataJSON)]), privateKey || this.privateKey);
    return {
      id: b64u(this.credId),
      rawId: b64u(this.credId),
      type: "public-key",
      response: {
        clientDataJSON: b64u(clientDataJSON),
        authenticatorData: b64u(authData),
        signature: b64u(signature),
        userHandle: null,
      },
      clientExtensionResults: {},
      authenticatorAttachment: "platform",
    };
  }
}

module.exports = { VirtualAuthenticator };
