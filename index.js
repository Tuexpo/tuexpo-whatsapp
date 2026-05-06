require('dotenv').config()
function tlog(tenantId, ...args) {
  if (tenantId) {
    console.log(`[tenant ${tenantId}]`, ...args)
  } else {
    console.log(...args)
  }
}

const express = require("express")
const P = require("pino")
const qrcode = require("qrcode-terminal")
const qrcodePng = require("qrcode")
const axios = require("axios")
const fs = require("fs")
const path = require("path")
const mysql = require("mysql2/promise")
const { resetPeerSession } = require("./resetPeerSession")

let makeWASocket
let useMultiFileAuthState
let fetchLatestBaileysVersion
let DisconnectReason
let downloadMediaMessage
/** Asignado en loadBaileys; alinea claves de caché con receipts/retry de Baileys (52 vs 521, c.us, etc.). */
let jidNormalizedUser = null

async function loadBaileys() {
  if (makeWASocket) return
  const baileys = await import("@whiskeysockets/baileys")
  makeWASocket = baileys.makeWASocket || baileys.default
  useMultiFileAuthState = baileys.useMultiFileAuthState
  fetchLatestBaileysVersion = baileys.fetchLatestBaileysVersion
  DisconnectReason = baileys.DisconnectReason
  downloadMediaMessage = baileys.downloadMediaMessage
  jidNormalizedUser = baileys.jidNormalizedUser || null
}

function cacheNormalizeJid(jid) {
  if (!jid) return ""
  try {
    if (typeof jidNormalizedUser === "function") {
      const n = jidNormalizedUser(String(jid))
      if (n) return n
    }
  } catch (_) {}
  return String(jid).trim()
}

const app = express()
/** Inbound JSON (p. ej. media_base64); mínimo 25mb para audio WhatsApp sin cortes silenciosos. */
const JSON_BODY_LIMIT = process.env.WHATSAPP_JSON_LIMIT || "25mb"
app.use(express.json({ limit: JSON_BODY_LIMIT }))

/** Debe coincidir con Gunicorn (p. ej. tuexpo.service -b 127.0.0.1:5000). Antes estaba 5001 y Flask nunca recibía el webhook. */
const TUEXPO_WHATSAPP_INCOMING_URL =
  process.env.TUEXPO_WHATSAPP_INCOMING_URL ||
  "http://127.0.0.1:5000/whatsapp/incoming"
const BAILEYS_MESSAGE_UPDATE_WEBHOOK_URL =
  process.env.BAILEYS_MESSAGE_UPDATE_WEBHOOK_URL ||
  "http://127.0.0.1:5000/webhook/baileys/message-update"
const BAILEYS_WEBHOOK_SECRET = String(process.env.BAILEYS_WEBHOOK_SECRET || "").trim()
const DISABLE_AUTO_REPLY = String(process.env.DISABLE_AUTO_REPLY || "").trim() === "1"

function panelBaseUrlFromIncoming() {
  const incoming = String(TUEXPO_WHATSAPP_INCOMING_URL || "").trim()
  return incoming.replace(/\/whatsapp\/incoming\/?$/i, "")
}

/** Fusiona en MySQL mensajes/estado guardados bajo LID hacia MSISDN (panel /whatsapp/lid-merge). */
async function postLidMergeToPanel(tenantId, lidDigits, msisdnDigits) {
  const base = panelBaseUrlFromIncoming()
  if (!base) return
  const lid = String(lidDigits || "").replace(/\D/g, "")
  const msisdn = normalizeMxDigits(String(msisdnDigits || "").replace(/\D/g, ""))
  if (!lid || !msisdn || lid === msisdn || lid.length < 11) return
  const url = `${base.replace(/\/$/, "")}/whatsapp/lid-merge`
  const headers = { "Content-Type": "application/json" }
  if (BAILEYS_WEBHOOK_SECRET) headers["X-Baileys-Secret"] = BAILEYS_WEBHOOK_SECRET
  try {
    const res = await axios.post(url, { tenant_id: tenantId, lid, msisdn }, { headers, timeout: 8000 })
    tlog(tenantId, "[LID_MERGE] panel", res.data?.merged || res.data)
  } catch (e) {
    console.warn("[LID_MERGE] panel failed", e?.response?.data || e?.message || e)
  }
}

function parseTenantList(raw, fallback = [2]) {
  const src = String(raw || "").trim()
  if (!src) return [...fallback]
  const out = []
  for (const part of src.split(",")) {
    const n = parseInt(String(part || "").trim(), 10)
    if (Number.isFinite(n) && n > 0 && !out.includes(n)) out.push(n)
  }
  return out.length ? out : [...fallback]
}

function discoverTenantsFromSessions(baseDir = path.join(__dirname, "sessions")) {
  try {
    return fs
      .readdirSync(baseDir)
      .filter((name) => /^\d+$/.test(String(name || "")))
      .map((name) => parseInt(String(name), 10))
      .filter((n) => Number.isFinite(n) && n > 0)
      .sort((a, b) => a - b)
  } catch (_) {
    return []
  }
}

/**
 * Lista blanca opcional. Si no defines CONNECTOR_TENANTS o pones "*", cualquier tenant con sesión puede usar /send y sockets.
 * Para restringir: CONNECTOR_TENANTS=2,4
 */
const _connectorTenantsRaw = String(process.env.CONNECTOR_TENANTS ?? "*").trim()
const CONNECTOR_ALLOW_ALL = _connectorTenantsRaw === "" || _connectorTenantsRaw === "*"
const CONNECTOR_TENANTS = CONNECTOR_ALLOW_ALL ? null : parseTenantList(_connectorTenantsRaw, [2, 4])
/**
 * Arranque automático al levantar el proceso. Por defecto 0: cero QR en consola hasta POST /connect o GET /qr
 * (o lazy start en /send). Poner CONNECTOR_AUTO_START=1 para el comportamiento anterior.
 */
const CONNECTOR_AUTO_START = String(process.env.CONNECTOR_AUTO_START ?? "0").trim() === "1"
/** Si CONNECTOR_AUTO_START=1: al arrancar, levantar estas sesiones. */
const DISCOVERED_SESSION_TENANTS = discoverTenantsFromSessions(path.join(__dirname, "sessions"))
const CONNECTOR_BOOTSTRAP_TENANTS = CONNECTOR_ALLOW_ALL
  ? (String(process.env.CONNECTOR_BOOTSTRAP_TENANTS || "").trim()
      ? parseTenantList(process.env.CONNECTOR_BOOTSTRAP_TENANTS || "", DISCOVERED_SESSION_TENANTS)
      : DISCOVERED_SESSION_TENANTS)
  : CONNECTOR_TENANTS

const ENABLE_AUTO_REPLY_TENANTS = parseTenantList(process.env.ENABLE_AUTO_REPLY_TENANTS || "2,4", [2, 4])

function isTenantEnabled(tenantId) {
  const n = parseInt(String(tenantId || 0), 10)
  if (!Number.isFinite(n) || n <= 0) return false
  if (CONNECTOR_TENANTS == null) return true
  return CONNECTOR_TENANTS.includes(n)
}

function isAutoReplyEnabledForTenant(tenantId) {
  if (DISABLE_AUTO_REPLY) return false
  const n = parseInt(String(tenantId || 0), 10)
  return Number.isFinite(n) && ENABLE_AUTO_REPLY_TENANTS.includes(n)
}

function panelBaileysSecretFromEnvFile() {
  try {
    const p = "/opt/tuexpo_panel/.env"
    if (!fs.existsSync(p)) return ""
    const raw = fs.readFileSync(p, "utf8")
    const line = String(raw || "")
      .split(/\r?\n/)
      .find((ln) => String(ln || "").trim().startsWith("BAILEYS_WEBHOOK_SECRET="))
    if (!line) return ""
    return String(line.split("=").slice(1).join("=") || "").trim()
  } catch (_) {
    return ""
  }
}

// Multi-tenant runtime stores.
const sessions = {} // { [companyId]: sock }
const qrStore = {} // { [companyId]: qrString|null }
const statusStore = {} // { [companyId]: connected|disconnected|waiting_qr|connecting|unknown }
const qrUpdatedAtStore = {} // { [companyId]: msTimestamp }
/**
 * QR_POLICY_GLOBAL:
 * - QR solo se permite cuando un humano presiona "Reconectar / Generar QR" (/connect).
 * - Autostart/reconnect/lazy-start pueden recuperar sesión existente, pero no abrir QR infinito.
 */
const qrAllowedUntilStore = {} // { [companyId]: msTimestamp }
const reconnectBlockedUntilStore = {} // { [companyId]: msTimestamp }
const connectInProgress = {} // { [companyId]: boolean }
const reconnectTimers = {} // { [companyId]: timeoutHandle|null }
const socketGeneration = {} // { [companyId]: number }
/** Intentos de reconexión seguidos por tenant (se resetea al abrir sesión). */
const reconnectAttemptByTenant = {}
/** 0 = no reconectar solo al cerrar (evita bucles; reconectar con POST /connect). Por defecto 1. */
const BAILEYS_AUTO_RECONNECT = String(process.env.BAILEYS_AUTO_RECONNECT ?? "1").trim() === "1"
const inboundSeen = new Map()
const INBOUND_SEEN_TTL_MS = 10 * 60 * 1000
const catalogMessageIdsByTenant = {} // { [companyId]: stanzaId[] }
const lidMapping = new Map()

/** WA WebMessageInfo.StubType: mensaje no descifrable / sesión peer corrupta. */
const STUB_CIPHERTEXT = 2
const STUB_PAYMENT_CIPHERTEXT = 47

/** message-receipt.update: muchos eventos para el mismo mensaje (bucle retry / ack raro). */
const receiptBurstByKey = new Map()
const RECEIPT_BURST_WINDOW_MS = 22_000
const RECEIPT_BURST_THRESHOLD = 14

// Inbound text debounce: evita 2-3 respuestas cuando el cliente manda varios mensajes seguidos.
// Solo aplica a inbound texto normal. No aplica a media/audio, quoted/swipe, fromMe ni eventos especiales.
const inboundDebounceByKey = new Map()
const INBOUND_DEBOUNCE_MS = Number(process.env.INBOUND_DEBOUNCE_MS || 6500)

function debounceIncomingToFlask(key, flaskBody, postFn) {
  if (!key || !flaskBody || flaskBody.fromMe || flaskBody.media_type || flaskBody.quoted_id) {
    return postFn(flaskBody)
  }

  const incomingText = String(flaskBody.text || flaskBody.message || "").trim()
  if (!incomingText) return postFn(flaskBody)

  let slot = inboundDebounceByKey.get(key)

  if (slot) {
    clearTimeout(slot.timer)
    for (const resolve of slot.waiters || []) {
      resolve({ data: { response: null, debounced: true } })
    }
    slot.waiters = []
  } else {
    slot = { parts: [], waiters: [], timer: null, lastBody: null }
  }

  slot.parts.push(incomingText)
  slot.lastBody = {
    ...flaskBody,
    text: slot.parts.join("\n"),
    message: slot.parts.join("\n"),
  }

  const promise = new Promise((resolve, reject) => {
    slot.waiters.push(resolve)
    slot.timer = setTimeout(async () => {
      inboundDebounceByKey.delete(key)
      try {
        console.log("[INBOUND DEBOUNCE FLUSH]", {
          key,
          parts: slot.parts.length,
          chars: String(slot.lastBody?.text || "").length,
        })
        const res = await postFn(slot.lastBody)
        resolve(res)
      } catch (e) {
        reject(e)
      }
    }, INBOUND_DEBOUNCE_MS)
  })

  inboundDebounceByKey.set(key, slot)

  if (inboundDebounceByKey.size > 2000) {
    const keys = Array.from(inboundDebounceByKey.keys()).slice(0, 500)
    for (const k of keys) inboundDebounceByKey.delete(k)
  }

  return promise
}

/** connection.update close: varios cierres seguidos (WhatsApp o sesión inestable). */
const closeTimestampsByTenant = new Map()
const CLOSE_STORM_WINDOW_MS = 120_000
const CLOSE_STORM_THRESHOLD = 4

/** Baileys usa esto en reintentos / ack con phash; sin caché el 2º mensaje puede quedar "esperando descargar". */
const sentMessageCacheByTenant = {} // { [tenantId]: Map<string, IMessage> }

function stableMsgCacheKey(key) {
  if (!key || !key.id) return ""
  const rj = cacheNormalizeJid(key.remoteJid)
  const part = key.participant ? cacheNormalizeJid(key.participant) : null
  return JSON.stringify({
    remoteJid: rj,
    id: String(key.id || ""),
    fromMe: key.fromMe !== false,
    participant: part || null,
  })
}

/** Solo cuerpos que Baileys puede re-enviar en relay/retry (evita stubs que rompen sesión / "connection closed"). */
function messageOkForGetMessageRetry(msg) {
  if (!msg || typeof msg !== "object") return false
  if (msg.messageStubType != null) return false
  if (msg.protocolMessage) return false
  if (msg.senderKeyDistributionMessage) return false
  if (msg.appStateSyncKeyShare) return false
  const nested = msg.deviceSentMessage?.message
  if (nested) return messageOkForGetMessageRetry(nested)
  const keys = Object.keys(msg).filter((k) => k !== "messageContextInfo")
  return keys.length > 0
}

function rememberSentProtoMessage(tenantId, waMaybe) {
  try {
    const n = parseInt(String(tenantId), 10)
    if (!Number.isFinite(n) || n <= 0) return
    const key = waMaybe?.key
    const msg = waMaybe?.message
    if (!key?.id || !msg || !messageOkForGetMessageRetry(msg)) return
    if (!sentMessageCacheByTenant[n]) sentMessageCacheByTenant[n] = new Map()
    const map = sentMessageCacheByTenant[n]
    const setKey = (kstr) => {
      if (kstr) map.set(kstr, msg)
    }
    setKey(stableMsgCacheKey(key))
    setKey(stableMsgCacheKey({ remoteJid: key.remoteJid, id: key.id, fromMe: key.fromMe, participant: null }))
    const rj = String(key.remoteJid || "")
    if (rj.endsWith("@s.whatsapp.net")) {
      const digits = rj.replace(/\D/g, "")
      for (const cand of buildWhatsAppPhoneCandidates(digits)) {
        const base = { id: key.id, fromMe: key.fromMe, participant: key.participant }
        setKey(stableMsgCacheKey({ ...base, remoteJid: `${cand}@s.whatsapp.net` }))
        setKey(stableMsgCacheKey({ ...base, remoteJid: `${cand}@s.whatsapp.net`, participant: null }))
      }
    }
    if (key.fromMe !== false && key.id) {
      setKey(`__id__:${String(key.id)}`)
    }
    while (map.size > 5000) {
      map.delete(map.keys().next().value)
    }
  } catch (_) {}
}

async function getMessageFromTenantCache(tenantId, key) {
  const debugMiss = String(process.env.BAILEYS_GETMESSAGE_DEBUG || "").trim() === "1"
  try {
    const n = parseInt(String(tenantId), 10)
    if (!Number.isFinite(n) || n <= 0 || !key?.id) return undefined
    const map = sentMessageCacheByTenant[n]
    if (!map) {
      if (debugMiss) console.warn("[getMessage] miss: no map", { tenantId: n, id: key.id })
      return undefined
    }
    const pick = (k) => map.get(stableMsgCacheKey(k))
    let hit = pick(key)
    if (!hit) {
      hit = pick({ remoteJid: key.remoteJid, id: key.id, fromMe: key.fromMe, participant: null })
    }
    const rj = String(key.remoteJid || "")
    if (!hit && rj.endsWith("@s.whatsapp.net")) {
      const digits = rj.replace(/\D/g, "")
      for (const cand of buildWhatsAppPhoneCandidates(digits)) {
        const base = { id: key.id, fromMe: key.fromMe, participant: key.participant }
        hit = pick({ ...base, remoteJid: `${cand}@s.whatsapp.net` })
        if (hit) break
        hit = pick({ ...base, remoteJid: `${cand}@s.whatsapp.net`, participant: null })
        if (hit) break
      }
    }
    if (!hit && key.fromMe !== false && key.id) {
      hit = map.get(`__id__:${String(key.id)}`)
    }
    if (!hit && debugMiss) {
      console.warn("[getMessage] miss after variants", {
        tenantId: n,
        id: key.id,
        remoteJid: key.remoteJid,
        fromMe: key.fromMe,
        participant: key.participant,
        mapSize: map.size,
      })
    }
    return hit || undefined
  } catch (e) {
    console.warn("[getMessage] cache:", e?.message || e)
    return undefined
  }
}

function markInboundSeen(tenantId, keyId, remoteJid) {
  const id = String(keyId || "").trim()
  const rj = String(remoteJid || "").trim()
  if (!id || !rj) return false
  const k = `${tenantId}:${rj}:${id}`
  const now = Date.now()
  const prev = inboundSeen.get(k) || 0
  if (now - prev < INBOUND_SEEN_TTL_MS) return true
  inboundSeen.set(k, now)
  if (inboundSeen.size > 20000) {
    for (const [kk, ts] of inboundSeen.entries()) {
      if (now - ts > INBOUND_SEEN_TTL_MS) inboundSeen.delete(kk)
    }
  }
  return false
}

function normalizeMxDigits(phone) {
  const p = String(phone || "").replace(/\D/g, "")
  if (p.startsWith("521") && p.length === 13) {
    return "52" + p.slice(3)
  }
  return p
}

/** Dígitos del local de un JID @lid (ej. 1650…:2@lid → 1650…). Misma noción que Flask _digits_from_wa_remote_jid. */
function waLocalDigitsFromLidJid(rj) {
  const s = String(rj || "").trim()
  if (!s.endsWith("@lid")) return ""
  const base = s.split("@")[0].split(":")[0]
  return String(base).replace(/\D/g, "")
}

function lidJidForLidToMsisdnMap(remoteJid, canonicalRemoteJid) {
  const a = String(remoteJid || "").trim()
  if (a.endsWith("@lid")) return a
  const b = String(canonicalRemoteJid || "").trim()
  if (b.endsWith("@lid")) return b
  return ""
}

/**
 * MSISDN desde senderPn / participantPn / *Alt. Alineado con @lid en chat: prioridad la define el caller.
 * (México/Latam: normaliza 521→52, descarta ruido tipo 1650…@s.whatsapp.net.)
 */
function tryExtractLatamMsisdnFromPnField(jid) {
  const s = String(jid || "").trim()
  if (!s) return ""
  if (!s.includes("@")) {
    const d0 = normalizeMxDigits(s.replace(/\D/g, ""))
    if (
      /^\d{10,15}$/.test(d0) &&
      !/^(1\d{14}|[2678]\d{10,14})$/.test(d0) &&
      d0.startsWith("5")
    ) {
      return d0
    }
    return ""
  }
  if (!s.endsWith("@s.whatsapp.net") || s.includes(":")) return ""
  const digits = normalizeMxDigits(s.replace("@s.whatsapp.net", "").replace(/\D/g, ""))
  if (
    /^\d{10,15}$/.test(digits) &&
    !/^(1\d{14}|[2678]\d{10,14})$/.test(digits) &&
    digits.startsWith("5")
  ) {
    return digits
  }
  return ""
}

async function persistJidMapLidToPhone(tenantId, lidJid, msisdnDigits) {
  const lidJid0 = String(lidJid || "").trim()
  const ph = String(msisdnDigits || "").replace(/\D/g, "")
  if (!lidJid0.endsWith("@lid") || !/^\d{10,20}$/.test(ph)) return
  try {
    const db = await mysql.createConnection({
      host: process.env.DB_HOST || "127.0.0.1",
      user: process.env.DB_USER || "root",
      password: process.env.MYSQL_PASSWORD || "PasswordSeguro123",
      database: process.env.DB_NAME || "tuexpo_voice"
    })
    await db.query(
      "INSERT INTO jid_map (remote_jid, phone) VALUES (?, ?) ON DUPLICATE KEY UPDATE phone = VALUES(phone)",
      [lidJid0, ph]
    )
    await db.end()
  } catch (e) {
    console.warn("[jid_map persist failed]", e?.message)
  }
  tlog(tenantId, "[jid_map PERSISTED]", { lid: lidJid0, phone: ph })
}

function resolveCanonicalChatJid(remoteJid, companyId) {
  const jid = String(remoteJid || "").trim()
  if (!jid) return jid
  if (lidMapping.has(jid)) return lidMapping.get(jid)
  if (jid.endsWith("@s.whatsapp.net")) return jid
  if (!jid.endsWith("@lid")) return jid

  const lid = jid.replace("@lid", "").trim()
  if (!lid) return jid

  // Baileys session mapping: lid-mapping-<lid>_reverse.json contains the phone number.
  const reversePath = path.join(__dirname, "sessions", String(companyId), `lid-mapping-${lid}_reverse.json`)
  try {
    if (fs.existsSync(reversePath)) {
      const raw = fs.readFileSync(reversePath, "utf8").trim()
      const phone = String(JSON.parse(raw) || "").replace(/\D/g, "")
      if (phone) return `${phone}@s.whatsapp.net`
    }
  } catch (e) {
    console.log("WARN lid reverse mapping failed:", e.message)
  }
  return jid
}

function isCanonicalMsisdnJid(jid) {
  const src = String(jid || "").trim()
  if (!src.endsWith("@s.whatsapp.net")) return false
  const digits = src.replace(/\D/g, "")
  return digits.length >= 10
}

/**
 * Variantes @s.whatsapp.net para entrega (México 52/521, Brasil móvil con/sin 9 tras DDD).
 */
function buildWhatsAppPhoneCandidates(digitsRaw) {
  const digits = String(digitsRaw || "").replace(/\D/g, "")
  if (!digits) return []
  const out = []
  const pushUnique = (d) => {
    const x = String(d || "").replace(/\D/g, "")
    if (x && !out.includes(x)) out.push(x)
  }
  pushUnique(digits)

  if (digits.startsWith("52") && digits.length === 12) {
    pushUnique("521" + digits.slice(2))
  } else if (digits.startsWith("521") && digits.length === 13) {
    pushUnique("52" + digits.slice(3))
  }

  if (digits.startsWith("55") && digits.length >= 12) {
    const rest = digits.slice(2)
    if (rest.length >= 2) {
      const area = rest.slice(0, 2)
      const subscriber = rest.slice(2)
      if (subscriber.length >= 8 && subscriber[0] !== "9") {
        pushUnique("55" + area + "9" + subscriber)
      }
      if (subscriber.length >= 9 && subscriber[0] === "9") {
        pushUnique("55" + area + subscriber.slice(1))
      }
    }
  }
  return out
}

/** JID que podemos usar para saliente: @lid o @s.whatsapp.net con MSISDN 52/55 (no IDs internos 1650…@s). */
function isUsableLatamRelayJid(jid) {
  const raw = String(jid || "").trim()
  if (!raw) return false
  if (raw.endsWith("@lid")) return true
  if (!raw.endsWith("@s.whatsapp.net")) return false
  const base = raw.split("@")[0] || ""
  if (base.includes(":")) return false
  const d = normalizeMxDigits(base)
  if (!d) return false
  if (!d.startsWith("52") && !d.startsWith("55")) return false
  if (d.startsWith("52")) return d.length === 12 || d.length === 13
  return d.length >= 12 && d.length <= 14
}

function outboundJidFromRequestBody(body) {
  const raw = String(body?.remoteJid || body?.remote_jid || "").trim()

  if (!raw) return ""

  // IMPORTANT:
  // Preserve same-thread addressing if the Inbox provides a LID JID.
  // Do NOT convert @lid to MSISDN here; canonical_phone is only fallback/storage.
  if (raw.endsWith("@lid")) {
    return raw
  }

  // Block unsupported chat types
  if (raw.includes("@g.us") || raw.includes("newsletter")) return ""

  // Ignore device-scoped relay JIDs
  if (raw.includes(":")) return ""

  if (!raw.includes("@")) return ""

  // Allow normal MSISDN JIDs
  return raw
}

function resolveOutboundCanonicalJid(sockInstance, msgData, remoteJid, participant, sender, companyId, preferredPhone = "") {
  const contextParticipantCandidates = [
    msgData?.message?.extendedTextMessage?.contextInfo?.participant,
    msgData?.message?.audioMessage?.contextInfo?.participant,
    msgData?.message?.imageMessage?.contextInfo?.participant,
    msgData?.message?.videoMessage?.contextInfo?.participant,
  ]

  const contactCandidates = []
  const contactMessage = msgData?.message?.contactMessage
  if (contactMessage?.vcard) {
    const matches = String(contactMessage.vcard).match(/\+?\d[\d\s-]{8,}\d/g) || []
    for (const match of matches) {
      const digits = normalizeMxDigits(match.replace(/\D/g, ""))
      if (digits) contactCandidates.push(`${digits}@s.whatsapp.net`)
    }
  }

  const storeContactCandidates = []
  const contactsStore = sockInstance?.store?.contacts || sockInstance?.contacts || {}
  for (const key of [participant, remoteJid, `${sender}@s.whatsapp.net`]) {
    const entry = key ? contactsStore[key] : null
    const id = entry?.id || entry?.jid || entry?.notify
    if (id) storeContactCandidates.push(String(id).trim())
  }

  const lidCandidates = []
  for (const key of [participant, remoteJid]) {
    const mapped = key ? lidMapping.get(String(key).trim()) : null
    if (mapped) lidCandidates.push(mapped)
  }

  const candidates = [
    preferredPhone ? `${normalizeMxDigits(String(preferredPhone).replace(/\D/g, ""))}@s.whatsapp.net` : "",
    participant,
    ...contextParticipantCandidates,
    ...contactCandidates,
    ...storeContactCandidates,
    ...lidCandidates,
  ]

  for (const candidate of candidates) {
    const resolved = resolveCanonicalChatJid(candidate, companyId)
    if (isCanonicalMsisdnJid(resolved)) return resolved
  }

  return null
}

/** 408 / "Timed Out" en getUSyncDevices→relayMessage: a veces el primer intento falla con el socket vivo. */
function isOutboundTransientTimeout(e) {
  const c = e?.output?.statusCode ?? e?.statusCode
  if (c === 408) return true
  return /timed?\s*out/i.test(String(e?.message || e || ""))
}

function normalizeOutboundJid(jid, fallbackPhone) {
  const raw = String(jid || "").trim()
  if (!raw) return null

  if (raw.endsWith("@lid")) {
    return raw
  }

  if (raw === "status@broadcast") {
    return null
  }

  const numeric = raw.replace("@s.whatsapp.net", "")
  if (/^\d{16,}$/.test(numeric)) {
    const fb = String(fallbackPhone || "").replace(/\D/g, "")
    if (!fb) return null
    return `${fb}@s.whatsapp.net`
  }

  return raw
}

/**
 * Inbox / panel must not send quotedMessage or contextInfo to Baileys — breaks Signal ratchet / ghost threads.
 * Strips reply linkage from any outbound content object before sock.sendMessage.
 */
function stripQuotedContextFromOutboundSendContent(content) {
  if (!content || typeof content !== "object" || Buffer.isBuffer(content)) return content
  const out = { ...content }
  delete out.contextInfo
  delete out.quotedMessage
  delete out.quoted
  delete out.quotedMessageId
  for (const key of Object.keys(out)) {
    const v = out[key]
    if (!v || typeof v !== "object" || Buffer.isBuffer(v)) continue
    if (key === "text" && typeof v === "string") continue
    const inner = { ...v }
    delete inner.contextInfo
    delete inner.quotedMessage
    delete inner.quotedMessageId
    out[key] = inner
  }
  return out
}

/** Coerce Inbox/panel `message` to plain string (never forward proto / quoted JSON as body). */
function plainTextForConnectorSend(message) {
  if (message == null) return ""
  if (typeof message === "string") {
    const t = message.trim()
    if (t.startsWith("{") && t.endsWith("}")) {
      try {
        return plainTextForConnectorSend(JSON.parse(t))
      } catch (_) {
        return message
      }
    }
    return message
  }
  if (typeof message === "object") {
    if (typeof message.text === "string") return message.text
    const et = message.extendedTextMessage
    if (et && typeof et.text === "string") return et.text
    if (typeof message.conversation === "string") return message.conversation
  }
  return String(message)
}

function isDecryptPeerSendError(err) {
  const m = String(err?.message || err || "")
  return (
    m.includes("Closing session") ||
    m.includes("Bad MAC") ||
    m.includes("No session") ||
    m.includes("Cannot decrypt") ||
    m.includes("failed to decrypt") ||
    m.includes("decrypt message") ||
    m.includes("Signal error")
  )
}

async function resetPeerSignalSession(jid, tenantId) {
  try {
    const tid = parseInt(String(tenantId), 10)
    if (!Number.isFinite(tid) || tid <= 0) return
    const number = String(jid || "").split("@")[0].replace(/\D/g, "")
    if (!number) return

    const base = path.resolve(path.join(__dirname, "sessions"), String(tid))
    if (!base.startsWith(path.join(__dirname, "sessions") + "/")) return
    if (!fs.existsSync(base)) return

    const files = fs.readdirSync(base)
    files.forEach((file) => {
      const name = String(file || "")
      if (!name.includes(number)) return
      if (!(name.startsWith("session-") || name.startsWith("sender-key-"))) return
      const full = path.resolve(base, name)
      if (!full.startsWith(base + path.sep)) return
      fs.unlinkSync(full)
      tlog(tenantId, "[SESSION RESET FILE REMOVED]", name)
    })
  } catch (err) {
    tlog(tenantId, "[SESSION RESET FAILED]", err?.message || err)
  }
}

function recoverPeerSignalSession(sock, tenantId, jid, reason) {
  const j = String(jid || "").trim()
  const tid = parseInt(String(tenantId), 10)
  if (!sock || !j) return
  tlog(tenantId, "[SELF-HEAL] signal session reset", jid, reason)
  console.warn("⚠️ Resetting peer signal session:", j, reason ? `(${reason})` : "")
  if (Number.isFinite(tid) && tid > 0) {
    try {
      resetPeerSession(path.join(__dirname, "sessions", String(tid)), j)
    } catch (e) {
      console.warn("[recoverPeerSignalSession] resetPeerSession", e?.message || e)
    }
  }
  try {
    sock.ev.emit("creds.update", {})
  } catch (_) {}
}

function trackReceiptBurst(tenantId, remoteJid, msgId) {
  const mid = String(msgId || "").trim()
  const rj = String(remoteJid || "").trim()
  if (!mid || !rj) return false
  const key = `${tenantId}:${rj}:${mid}`
  const now = Date.now()
  let slot = receiptBurstByKey.get(key)
  if (!slot || now - slot.t0 > RECEIPT_BURST_WINDOW_MS) {
    slot = { n: 0, t0: now }
  }
  slot.n += 1
  receiptBurstByKey.set(key, slot)
  if (receiptBurstByKey.size > 8000) {
    for (const [kk, s] of receiptBurstByKey.entries()) {
      if (now - s.t0 > RECEIPT_BURST_WINDOW_MS * 2) receiptBurstByKey.delete(kk)
    }
  }
  return slot.n >= RECEIPT_BURST_THRESHOLD
}

function clearReceiptBurst(tenantId, remoteJid, msgId) {
  const key = `${tenantId}:${String(remoteJid || "").trim()}:${String(msgId || "").trim()}`
  receiptBurstByKey.delete(key)
}

/**
 * Reintentos con backoff (env BAILEYS_SEND_408_RETRIES, por defecto 4).
 * Ante errores de sesión signal (Bad MAC / decrypt) limpia peer + emite creds.update y reintenta una vez.
 * @param {Record<string, unknown>} [opts] opciones Baileys para sendMessage (tercer argumento)
 */
async function sendMessageReliable(sock, jid, content, opts = {}, fallbackPhone = "") {
  const n = Math.max(1, parseInt(process.env.BAILEYS_SEND_408_RETRIES || "4", 10) || 4)
  let last = null
  const tenantId = sock?.__connectorTenantId
  const jidOriginal = String(jid || "").trim()

  async function doSend() {
    let normalizedJid = normalizeOutboundJid(jidOriginal, fallbackPhone)
    if (normalizedJid && normalizedJid.endsWith("@s.whatsapp.net")) {
      const digits = String(normalizedJid).replace("@s.whatsapp.net", "").replace(/\D/g, "")
      if (digits.length > 15) {
        const fb = String(fallbackPhone || "").replace(/\D/g, "")
        normalizedJid = fb ? `${fb}@s.whatsapp.net` : null
      }
    }
    tlog(tenantId, "[OUTBOUND ROUTING]", {
      original: jidOriginal,
      normalized: normalizedJid,
      fallbackPhone: String(fallbackPhone || "").replace(/\D/g, ""),
      tenantId: tenantId,
    })
    if (!normalizedJid) {
      tlog(tenantId, "[BLOCKED INVALID OUTBOUND TARGET]", {
        original: jidOriginal,
        fallbackPhone: String(fallbackPhone || "").replace(/\D/g, ""),
        tenantId: tenantId,
      })
      return null
    }
    const safeContent = stripQuotedContextFromOutboundSendContent(content)
    return await sock.sendMessage(normalizedJid, safeContent, opts)
  }

  for (let i = 0; i < n; i++) {
    try {
      const sent = await doSend()
      if (!sent) return null
      return sent
    } catch (e) {
      last = e
      if (isDecryptPeerSendError(e)) {
        const normForReset = normalizeOutboundJid(jidOriginal, fallbackPhone) || jidOriginal
        tlog(tenantId, "[SESSION DRIFT DETECTED]", normForReset)
        await resetPeerSignalSession(normForReset, tenantId)
        console.warn("[sendMessageReliable] decrypt/session error → recover + retry once", {
          jid: String(normForReset).slice(0, 72),
          detail: String(e?.message || e).slice(0, 160),
        })
        if (!normForReset || normForReset.endsWith("@lid")) {
          return
        }
        recoverPeerSignalSession(sock, tenantId, normForReset, "sendMessageReliable")
        await new Promise((r) => setTimeout(r, 280))
        try {
          const sent2 = await doSend()
          if (!sent2) return null
          return sent2
        } catch (e2) {
          throw e2
        }
      }
      if (i < n - 1 && isOutboundTransientTimeout(e)) {
        const delay = Math.min(12000, 2000 * (i + 1))
        console.warn("[sendMessageReliable] timeout/408, reintento", {
          attempt: i + 1,
          max: n,
          delayMs: delay,
          jid: String(jid).slice(0, 56),
        })
        await new Promise((r) => setTimeout(r, delay))
        continue
      }
      throw e
    }
  }
  throw last
}

/**
 * @param {Function} [sendOne] (jid, content) => Promise
 */
async function sendMessageWithMxFallback(sockInstance, targetJid, content, sendOne, tenantIdForMessageCache) {
  const send =
    typeof sendOne === "function"
      ? sendOne
      : (j, c) => sendMessageReliable(sockInstance, j, c, {}, String(j || "").replace(/\D/g, ""))
  const baseJid = String(targetJid || "").trim()
  if (!baseJid) throw new Error("missing jid")
  if (!baseJid.endsWith("@s.whatsapp.net")) {
    const sent = await send(baseJid, content)
    if (tenantIdForMessageCache) rememberSentProtoMessage(tenantIdForMessageCache, sent)
    return sent
  }

  const digits = baseJid.replace("@s.whatsapp.net", "").replace(/\D/g, "")
  if (!digits) {
    const sent = await send(baseJid, content)
    if (tenantIdForMessageCache) rememberSentProtoMessage(tenantIdForMessageCache, sent)
    return sent
  }

  const candidates = buildWhatsAppPhoneCandidates(digits)
  let lastErr = null
  for (const cand of candidates) {
    const jid = `${cand}@s.whatsapp.net`
    try {
      const sent = await send(jid, content)
      if (tenantIdForMessageCache) rememberSentProtoMessage(tenantIdForMessageCache, sent)
      tlog(tenantIdForMessageCache, "Outbound delivered via candidate JID:", { requested: baseJid, used: jid })
      return sent
    } catch (e) {
      lastErr = e
    }
  }
  throw lastErr || new Error("sendMessage candidate fallback failed")
}

/**
 * 1) remoteJid del panel (hilo real / LID-PN).
 * 2) Si lanza error, variantes sobre `number` del body (MX/BR vía buildWhatsAppPhoneCandidates).
 */
async function sendToPreferredOrMsisdnVariants(
  sockInstance,
  preferredJid,
  content,
  sendOne,
  fallbackDigits,
  tenantIdForMessageCache
) {
  const pref = String(preferredJid || "").trim()
  const d = String(fallbackDigits || "").replace(/\D/g, "")
  const canonicalPhone = d

  const send =
    typeof sendOne === "function"
      ? async (j, c) => {
          const sent = await sendOne(j, c)
          if (tenantIdForMessageCache) rememberSentProtoMessage(tenantIdForMessageCache, sent)
          return sent
        }
      : async (j, c) => {
          const targetJid = j

          tlog(tenantIdForMessageCache, "[OUTBOUND FINAL TARGET]", targetJid)
          const sent = await sendMessageReliable(sockInstance, targetJid, c, {}, d)
          if (tenantIdForMessageCache) rememberSentProtoMessage(tenantIdForMessageCache, sent)
          return sent
        }

  const tryMsisdnVariants = () =>
    sendMessageWithMxFallback(
      sockInstance,
      `${d}@s.whatsapp.net`,
      content,
      sendOne,
      tenantIdForMessageCache
    )

  /**
   * Inbox same-thread: inbound @lid → transportar respuesta al mismo hilo (canonical MSISDN solo para DB/merge).
   * Si falla el envío al @lid y hay MSISDN, probar variantes numéricas (MX/BR).
   */
  if (pref.endsWith("@lid")) {
    try {
      const sent = await send(pref, content)
      tlog(tenantIdForMessageCache, "[OUTBOUND ok @lid same-thread]", String(pref).slice(0, 56))
      return sent
    } catch (e) {
      if (!d) {
        console.warn("[OUTBOUND @lid same-thread falló, no fallback PN]", String(pref).slice(0, 40), e?.message || e)
        throw e
      }
      console.warn("[OUTBOUND @lid failed, MSISDN variants]", e?.message || e)
      return tryMsisdnVariants()
    }
  }

  if (pref && !pref.includes("@lid")) {
    try {
      const sent = await send(pref, content)
      tlog(tenantIdForMessageCache, "[OUTBOUND ok via relay]", pref.length > 48 ? pref.slice(0, 48) + "…" : pref)
      return sent
    } catch (eDirect) {
      console.warn("[OUTBOUND relay failed, number/variants]", eDirect?.message || eDirect)
      if (!d) throw eDirect
      return sendMessageWithMxFallback(
        sockInstance,
        `${d}@s.whatsapp.net`,
        content,
        sendOne,
        tenantIdForMessageCache
      )
    }
  }

  if (!d) throw new Error("missing destination (no remoteJid / number)")
  return sendMessageWithMxFallback(sockInstance, `${d}@s.whatsapp.net`, content, sendOne, tenantIdForMessageCache)
}

function inferAffiliateBrandFromCaption(caption) {
  const t = String(caption || "")
  if (/mizuno/i.test(t)) return "mizuno"
  if (/olympikus/i.test(t)) return "olympikus"
  return "unknown"
}

function parseAffiliateProductCaption(caption) {
  const lines = String(caption || "")
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean)
  const model = lines[0] || ""
  let price = ""
  let link = ""
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i]
    if (/^r\$\s*/i.test(line)) {
      price = line.replace(/^r\$\s*/i, "").trim()
    } else if (/^https?:\/\//i.test(line)) {
      link = line
    }
  }
  return { model, price, link }
}

async function saveMessageToDb(
  phone,
  direction,
  body,
  source,
  tenantId,
  mediaType = null,
  mediaUrl = null,
  metaJson = null
) {

  if (!phone) return

  const msgBody =
    body != null && String(body).trim() !== ""
      ? String(body)
      : mediaUrl
        ? "[media]"
        : ""

  if (!msgBody && !mediaUrl) return

  let db
  try {
    db = await mysql.createConnection({
      host: "localhost",
      user: "root",
      password: process.env.MYSQL_PASSWORD || "PasswordSeguro123",
      database: "tuexpo_voice",
    })
    try {
      await db.execute(
        `INSERT INTO messages (phone, tenant_id, company_id, direction, message, source, media_type, media_url, meta_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [phone, tenantId, tenantId, direction, msgBody, source, mediaType, mediaUrl, metaJson]
      )
    } catch (eInsert) {
      // Legacy fallback if company_id is not available in this environment.
      if (String(eInsert?.message || "").toLowerCase().includes("company_id")) {
        await db.execute(
          `INSERT INTO messages (phone, tenant_id, direction, message, source, media_type, media_url, meta_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [phone, tenantId, direction, msgBody, source, mediaType, mediaUrl, metaJson]
        )
      } else {
        throw eInsert
      }
    }
  } catch (err) {
    console.warn(`${direction}bound message DB save failed`, err.message || err)
  } finally {
    if (db) await db.end()
  }
}

function mediaTypeFromMime(mime) {
  const mm = String(mime || "").toLowerCase()
  if (mm.startsWith("image/")) return "image"
  if (mm.startsWith("video/")) return "video"
  if (mm.startsWith("audio/")) return "audio"
  return "document"
}

function extensionFromMime(mime) {
  const mm = String(mime || "").toLowerCase()
  if (mm.includes("jpeg")) return "jpg"
  if (mm.includes("png")) return "png"
  if (mm.includes("webp")) return "webp"
  if (mm.includes("webm")) return "webm"
  if (mm.includes("ogg") || mm.includes("opus")) return "ogg"
  if (mm.includes("mp3")) return "mp3"
  if (mm.includes("m4a")) return "m4a"
  if (mm.includes("mp4")) return "mp4"
  if (mm.includes("pdf")) return "pdf"
  return "bin"
}

async function persistOutboundMediaBuffer(buffer, mime, tenantId, prefix = "out") {
  if (!buffer || !Buffer.isBuffer(buffer) || buffer.length === 0) return null
  const absDir = path.join("/opt/tuexpo_panel/static", "wa_media", String(tenantId))
  await fs.promises.mkdir(absDir, { recursive: true })
  const ext = extensionFromMime(mime)
  const fname = `${Date.now()}-${prefix}-${Math.random().toString(16).slice(2)}.${ext}`
  const absPath = path.join(absDir, fname)
  await fs.promises.writeFile(absPath, buffer)
  return `/static/wa_media/${tenantId}/${fname}`
}

async function streamToBuffer(stream) {
  const chunks = []
  for await (const chunk of stream) {
    chunks.push(chunk)
  }
  return Buffer.concat(chunks)
}

async function persistIncomingMedia(sockInstance, msgData, tenantId) {
  const empty = {
    mediaType: null,
    mediaUrl: null,
    mimeType: null,
    ptt: null,
    mediaBase64: null,
  }
  try {
    const m = msgData?.message
    if (!m) return { ...empty }

    let mediaType = null
    let content = null
    if (m.imageMessage) {
      mediaType = "image"
      content = m.imageMessage
    } else if (m.stickerMessage) {
      mediaType = "sticker"
      content = m.stickerMessage
    } else if (m.ptvMessage) {
      mediaType = "video"
      content = m.ptvMessage
    } else if (m.videoMessage) {
      mediaType = "video"
      content = m.videoMessage
    } else if (m.pttMessage) {
      mediaType = "audio"
      content = m.pttMessage
    } else if (m.audioMessage) {
      mediaType = "audio"
      content = m.audioMessage
    } else if (m.documentMessage) {
      mediaType = "document"
      content = m.documentMessage
    } else {
      return { ...empty }
    }

    const absDir = path.join("/opt/tuexpo_panel/static", "wa_media", String(tenantId))
    await fs.promises.mkdir(absDir, { recursive: true })

    const extFromMime = (mime) => {
      const mm = String(mime || "").toLowerCase()
      if (mm.includes("jpeg")) return "jpg"
      if (mm.includes("png")) return "png"
      if (mm.includes("webp")) return "webp"
      if (mm.includes("ogg") || mm.includes("opus")) return "ogg"
      if (mm.includes("mp3")) return "mp3"
      if (mm.includes("mp4")) return "mp4"
      if (mm.includes("pdf")) return "pdf"
      return "bin"
    }

    const mime = content?.mimetype
    const ext = extFromMime(mime)
    const fname = `${Date.now()}-${Math.random().toString(16).slice(2)}.${ext}`
    const absPath = path.join(absDir, fname)

    const dlLogger = sockInstance?.logger || P({ level: "silent" })
    const reupload =
      typeof sockInstance?.updateMediaMessage === "function" ? sockInstance.updateMediaMessage.bind(sockInstance) : undefined

    let buffer = null
    try {
      buffer = await downloadMediaMessage(
        msgData,
        "buffer",
        {},
        { logger: dlLogger, reuploadRequest: reupload }
      )
    } catch (e1) {
      if (isDecryptPeerSendError(e1) && sockInstance && msgData?.key?.remoteJid) {
        console.warn("[WATCHDOG] downloadMediaMessage decrypt → recover peer", msgData.key.remoteJid)
        recoverPeerSignalSession(
          sockInstance,
          tenantId,
          String(msgData.key.remoteJid),
          "downloadMediaMessage"
        )
        await new Promise((r) => setTimeout(r, 280))
        try {
          buffer = await downloadMediaMessage(
            msgData,
            "buffer",
            {},
            { logger: dlLogger, reuploadRequest: reupload }
          )
        } catch (e1b) {
          console.warn("downloadMediaMessage failed after recover:", e1b?.message || e1b)
        }
      }
      if (buffer) {
        /* recovered */
      } else {
      // Algunos mensajes solo traen directPath (sin url); downloadMediaMessage falla antes de descargar.
      console.warn("downloadMediaMessage failed, fallback directPath:", e1?.message || e1)
      const { downloadContentFromMessage } = await import("@whiskeysockets/baileys/lib/Utils/messages-media.js")
      const dlKind =
        mediaType === "sticker"
          ? "sticker"
          : mediaType === "video" && m.ptvMessage
            ? "ptv"
            : mediaType === "audio" && content?.ptt
              ? "ptt"
              : mediaType === "image"
                ? "image"
                : mediaType === "video"
                  ? "video"
                  : mediaType === "audio"
                    ? "audio"
                    : mediaType === "document"
                      ? "document"
                      : "image"
      const stream = await downloadContentFromMessage(content, dlKind, {})
      buffer = await streamToBuffer(stream)
      }
    }

    await fs.promises.writeFile(absPath, buffer)

    const mediaUrl = `/static/wa_media/${tenantId}/${fname}`

    let mimeType = null
    let ptt = null
    let mediaBase64 = null
    if (mediaType === "audio" && buffer) {
      mimeType = String(content?.mimetype || "audio/ogg; codecs=opus").trim() || "audio/ogg; codecs=opus"
      ptt = !!(content?.ptt || m.pttMessage)
      mediaBase64 = buffer.toString("base64")
    }

    return { mediaType, mediaUrl, mimeType, ptt, mediaBase64 }
  } catch (e) {
    console.warn("persistIncomingMedia failed", e?.message || e)
    return { ...empty }
  }
}

async function saveContactPushName(phone, pushName, tenantId) {
  if (!phone || !pushName) return
  let db
  try {
    db = await mysql.createConnection({
      host: "localhost",
      user: "root",
      password: process.env.MYSQL_PASSWORD || "PasswordSeguro123",
      database: "tuexpo_voice",
    })
    await db.execute(
      `INSERT INTO contacts (phone, tenant_id, push_name, name)
       VALUES (?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         tenant_id = VALUES(tenant_id),
         push_name = VALUES(push_name),
         name = IF(CHAR_LENGTH(TRIM(COALESCE(name, ''))) = 0, VALUES(name), name)`,
      [phone, tenantId, pushName, pushName]
    )
  } catch (err) {
    console.warn("contact push_name save failed", err.message || err)
  } finally {
    if (db) await db.end()
  }
}

/** Evita pedir la foto a WA en cada mensaje (mismo contacto). */
const profilePicFetchCooldown = new Map()
const PROFILE_PIC_COOLDOWN_MS = 6 * 60 * 60 * 1000

async function saveContactProfilePicUrl(sockInstance, phone, jidForPicture, tenantId) {
  if (!phone || !jidForPicture || !sockInstance) return
  const jPic = String(jidForPicture || "").trim()
  // Solo MSISDN estable; @lid / otros suelen provocar timeouts y compiten con usync del envío.
  if (!jPic.endsWith("@s.whatsapp.net") || jPic.includes(":")) return

  const now = Date.now()
  const lastOk = profilePicFetchCooldown.get(phone)
  if (lastOk && now - lastOk < PROFILE_PIC_COOLDOWN_MS) return

  let url
  try {
    url = await sockInstance.profilePictureUrl(jPic, "image")
  } catch (err) {
    console.warn("profilePictureUrl failed", phone, err.message || err)
    return
  }

  if (!url) return

  profilePicFetchCooldown.set(phone, now)

  let db
  try {
    db = await mysql.createConnection({
      host: "localhost",
      user: "root",
      password: process.env.MYSQL_PASSWORD || "PasswordSeguro123",
      database: "tuexpo_voice",
    })
    await db.execute(
      `INSERT INTO contacts (phone, tenant_id, profile_pic_url)
       VALUES (?, ?, ?)
       ON DUPLICATE KEY UPDATE
         tenant_id = VALUES(tenant_id),
         profile_pic_url = VALUES(profile_pic_url)`,
      [phone, tenantId, url]
    )
  } catch (err) {
    console.warn("contact profile_pic_url save failed", err.message || err)
  } finally {
    if (db) await db.end()
  }
}

function tenantFromRequest(req) {
  const raw = req.body?.company_id ?? req.query?.company_id ?? 1
  const n = parseInt(String(raw), 10)
  return Number.isFinite(n) && n > 0 ? n : 1
}

async function startWhatsApp(companyId, forceNew = false) {
  await loadBaileys()
  const tenantId = parseInt(String(companyId), 10) || 1

  if (!forceNew) {
    delete qrAllowedUntilStore[tenantId]
  }
  if (!isTenantEnabled(tenantId)) {
    statusStore[tenantId] = "disabled"
    return null
  }
  const gen = (socketGeneration[tenantId] || 0) + 1
  socketGeneration[tenantId] = gen
  const sessionDir = path.join(__dirname, "sessions", String(tenantId))
  await fs.promises.mkdir(sessionDir, { recursive: true })

  if (forceNew) {
    try {
      await fs.promises.unlink(path.join(sessionDir, "creds.json"))
    } catch (e) {
      // ignore if creds do not exist
    }
  }

  if (sessions[tenantId]) {
    try {
      await sessions[tenantId].logout()
    } catch (e) {
      // ignore
    }
    sessions[tenantId] = null
    delete sentMessageCacheByTenant[tenantId]
  }

  const { state, saveCreds } = await useMultiFileAuthState(sessionDir)

  const { version } = await fetchLatestBaileysVersion()

  const sock = makeWASocket({
    version,
    auth: state,
    getMessage: async (_key) => {
      return {
        conversation: "",
      }
    },
    logger: P({ level: "silent" }),
    browser: ["Tuexpo Voice", "Chrome", "1.0.0"],
    defaultQueryTimeoutMs: parseInt(process.env.BAILEYS_QUERY_TIMEOUT_MS || "240000", 10) || 240000,
    retryRequestDelayMs: parseInt(process.env.BAILEYS_RETRY_REQUEST_DELAY_MS || "400", 10) || 400,
    markOnlineOnConnect: String(process.env.BAILEYS_MARK_ONLINE || "").trim() === "1",
  })

  sessions[tenantId] = sock
  sock.__connectorTenantId = tenantId
  statusStore[tenantId] = statusStore[tenantId] || "connecting"
  sock.ev.on("creds.update", saveCreds)

  /** WhatsApp comparte LID ↔ PN; persiste para resolveCanonicalChatJid tras reinicio. */
  sock.ev.on("chats.phoneNumberShare", (evt) => {
    try {
      const lid = String(evt?.lid || "").trim()
      const jid = String(evt?.jid || "").trim()
      if (!lid.endsWith("@lid") || !jid.endsWith("@s.whatsapp.net") || jid.includes(":")) return
      lidMapping.set(lid, jid)
      const lidPlain = lid.replace(/@lid$/i, "").trim()
      if (lidPlain) {
        const phoneDigits = jid.replace("@s.whatsapp.net", "").replace(/\D/g, "")
        const reversePath = path.join(sessionDir, `lid-mapping-${lidPlain}_reverse.json`)
        fs.writeFileSync(reversePath, JSON.stringify(phoneDigits), "utf8")
        tlog(tenantId, "[LID↔PN] chats.phoneNumberShare", { lid, jid: jid.slice(0, 28) + "…" })
        const lidDigitsOnly = lid.replace(/@lid$/i, "").replace(/\D/g, "")
        void postLidMergeToPanel(tenantId, lidDigitsOnly, phoneDigits)
      }
    } catch (e) {
      console.warn("[LID↔PN] handler", e?.message || e)
    }
  })

  sock.ev.on("connection.update", (update) => {
    if (socketGeneration[tenantId] !== gen) return

    const { connection, qr, lastDisconnect } = update

    if (qr) {
      const qrAllowedUntil = Number(qrAllowedUntilStore[tenantId] || 0)
      const qrAllowed = qrAllowedUntil > Date.now()

      if (!qrAllowed) {
        qrStore[tenantId] = null
        qrUpdatedAtStore[tenantId] = Date.now()
        statusStore[tenantId] = "disconnected"
        reconnectBlockedUntilStore[tenantId] = Date.now() + 10 * 60 * 1000

        console.warn(
          `[QR_POLICY] tenant ${tenantId}: QR bloqueado porque no fue disparado por POST /connect. Esperando botón Reconectar / Generar QR.`
        )

        try { sock.ws?.close() } catch (_) {}
        try { sock.end?.() } catch (_) {}

        return
      }

      qrStore[tenantId] = qr
      qrUpdatedAtStore[tenantId] = Date.now()
      statusStore[tenantId] = "waiting_qr"
      tlog(tenantId, "\nESCANEA ESTE QR\n")
      qrcode.generate(qr, { small: true })
    }

    if (connection === "open") {
      tlog(tenantId, `WhatsApp conectado correctamente (tenant ${tenantId})`)
      statusStore[tenantId] = "connected"
      qrStore[tenantId] = null
      qrUpdatedAtStore[tenantId] = Date.now()
      reconnectAttemptByTenant[tenantId] = 0
      delete qrAllowedUntilStore[tenantId]
      delete reconnectBlockedUntilStore[tenantId]
      if (reconnectTimers[tenantId]) {
        clearTimeout(reconnectTimers[tenantId])
        reconnectTimers[tenantId] = null
      }
    }

    if (connection === "close") {
      const statusCode = lastDisconnect?.error?.output?.statusCode
      const errMsg = String(
        lastDisconnect?.error?.message ||
          lastDisconnect?.error?.output?.payload?.message ||
          lastDisconnect?.error?.data ||
          ""
      )

      const reconnectBlockedUntil = Number(reconnectBlockedUntilStore[tenantId] || 0)
      if (reconnectBlockedUntil > Date.now()) {
        tlog(tenantId, `Conexión cerrada (tenant ${tenantId}) QR bloqueado; sin auto-reconnect`)
        statusStore[tenantId] = "disconnected"
        return
      }

      const qrRefsAttemptsEnded = (
        statusCode === 408 &&
        /QR refs attempts ended/i.test(errMsg)
      )

      if (qrRefsAttemptsEnded) {
        qrStore[tenantId] = null
        qrUpdatedAtStore[tenantId] = Date.now()
        statusStore[tenantId] = "disconnected"
        delete qrAllowedUntilStore[tenantId]
        reconnectBlockedUntilStore[tenantId] = Date.now() + 10 * 60 * 1000
        console.warn(
          `[QR_POLICY] tenant ${tenantId}: QR expiró/no fue escaneado. No se auto-reconecta; esperar nuevo POST /connect.`
        )
        return
      }

      const shouldReconnect =
        statusCode !== DisconnectReason.loggedOut

      tlog(tenantId, `Conexión cerrada (tenant ${tenantId})`)
      statusStore[tenantId] = "disconnected"

      const boomMsg = String(
        lastDisconnect?.error?.message ||
          lastDisconnect?.error?.output?.payload?.message ||
          lastDisconnect?.error?.data ||
          ""
      )
      const cryptoClose = /Bad MAC|Cannot decrypt|No session|failed to decrypt|decrypt message|Signal error/i.test(
        boomMsg
      )
      const nowClose = Date.now()
      const prevCloses = (closeTimestampsByTenant.get(tenantId) || []).filter((t) => nowClose - t < CLOSE_STORM_WINDOW_MS)
      prevCloses.push(nowClose)
      closeTimestampsByTenant.set(tenantId, prevCloses)
      const closeStorm = prevCloses.length >= CLOSE_STORM_THRESHOLD
      if ((cryptoClose || closeStorm) && sock?.ev) {
        console.warn("[WATCHDOG] connection close storm / crypto → creds.update", {
          tenantId,
          cryptoClose,
          closeStorm,
          boomMsg: boomMsg.slice(0, 140),
        })
        try {
          sock.ev.emit("creds.update", {})
        } catch (_) {}
      }

      if (shouldReconnect && BAILEYS_AUTO_RECONNECT) {
        if (reconnectTimers[tenantId]) clearTimeout(reconnectTimers[tenantId])
        const prev = reconnectAttemptByTenant[tenantId] || 0
        reconnectAttemptByTenant[tenantId] = prev + 1
        const attempt = reconnectAttemptByTenant[tenantId]
        const baseMs = parseInt(process.env.BAILEYS_RECONNECT_BASE_MS || "30000", 10) || 30000
        const maxMs = parseInt(process.env.BAILEYS_RECONNECT_MAX_MS || "900000", 10) || 900000
        const exp = Math.min(attempt - 1, 10)
        const delayMs = Math.min(maxMs, baseMs * 2 ** exp)
        console.warn(
          `[RECONNECT] tenant ${tenantId}: reintento ${attempt} en ${Math.round(delayMs / 1000)}s (max ${Math.round(
            maxMs / 1000
          )}s). Ajusta BAILEYS_RECONNECT_* o BAILEYS_AUTO_RECONNECT=0 si WhatsApp limita.`
        )
        reconnectTimers[tenantId] = setTimeout(() => {
          if (socketGeneration[tenantId] !== gen) return
          startWhatsApp(tenantId, false).catch((err) => {
            console.error(`reconnect failed tenant ${tenantId}:`, err.message || err)
          })
        }, delayMs)
      } else if (shouldReconnect && !BAILEYS_AUTO_RECONNECT) {
        console.warn(
          `[RECONNECT] tenant ${tenantId}: auto-reconexión desactivada (BAILEYS_AUTO_RECONNECT=0). Usar POST /connect cuando toque.`
        )
      }
    }
  })

  sock.ev.on("messages.update", async (updates) => {
    if (!Array.isArray(updates) || updates.length === 0) return
    for (const upd of updates) {
      try {
        const key = upd?.key || {}
        const keyId = String(key?.id || "").trim()
        const remoteJid = String(key?.remoteJid || "").trim()
        if (!keyId || !remoteJid) continue
        const status = upd?.update?.status ?? upd?.status
        if (status == null) continue

        const canonicalRemoteJid = resolveCanonicalChatJid(remoteJid, tenantId)
        const phone = normalizeMxDigits(String(canonicalRemoteJid).replace(/\D/g, ""))
        if (!phone) continue

        const headers = {}
        if (BAILEYS_WEBHOOK_SECRET) headers["X-Baileys-Secret"] = BAILEYS_WEBHOOK_SECRET
        await axios.post(
          BAILEYS_MESSAGE_UPDATE_WEBHOOK_URL,
          {
            company_id: tenantId,
            tenant_id: tenantId,
            phone,
            remoteJid: canonicalRemoteJid,
            key: { id: keyId, remoteJid: canonicalRemoteJid, fromMe: !!key?.fromMe },
            status,
            update: upd?.update || {},
          },
          { headers, timeout: 8000 }
        )
      } catch (e) {
        console.warn("messages.update webhook failed:", e?.response?.data || e?.message || e)
      }
    }
  })

sock.ev.on("message-receipt.update", async (updates) => {
  if (!Array.isArray(updates)) return

  try {
    for (const upd of updates) {
      const key = upd?.key || {}
      const msgId = String(key?.id || "").trim()
      const remoteJid = String(key?.remoteJid || "").trim()
      if (!msgId || !remoteJid) continue

      const canonicalRemoteJid = resolveCanonicalChatJid(remoteJid, tenantId)
      const phone = normalizeMxDigits(String(canonicalRemoteJid).replace(/\D/g, ""))
      if (!phone) continue

      const status = upd?.receipt?.type || "unknown"

      const burst = trackReceiptBurst(tenantId, remoteJid, msgId)
      const retryish = burst || String(status || "").toLowerCase() === "retry"
      if (retryish && sessions[tenantId]) {
        console.warn("[WATCHDOG] retry receipt loop → reset peer + creds", {
          tenantId,
          remoteJid: String(remoteJid).slice(0, 56),
          msgId,
          status,
        })
        recoverPeerSignalSession(sessions[tenantId], tenantId, remoteJid, "message-receipt.update")
        clearReceiptBurst(tenantId, remoteJid, msgId)
      }

      const headers = {}
      if (BAILEYS_WEBHOOK_SECRET) headers["X-Baileys-Secret"] = BAILEYS_WEBHOOK_SECRET

      await axios.post(
        BAILEYS_MESSAGE_UPDATE_WEBHOOK_URL,
        {
          company_id: tenantId,
          tenant_id: tenantId,
          phone,
          remoteJid: canonicalRemoteJid,
          key: { id: msgId, remoteJid: canonicalRemoteJid },
          receipt_status: status
        },
        { headers, timeout: 8000 }
      )
    }
  } catch (e) {
    console.warn("message-receipt.update webhook failed:", e?.response?.data || e?.message || e)
  }
})



  function normalizeMx(phone) {
    if (!phone) return phone
    if (phone.startsWith("521") && phone.length === 13) {
      return "52" + phone.slice(3)
    }
    return phone
  }

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    try {
      const __rawMsgs = Array.isArray({ messages, type }?.messages) ? { messages, type }.messages : []
      console.log("[B7 RAW messages.upsert]", {
        type: { messages, type }?.type,
        count: __rawMsgs.length,
        keys: __rawMsgs.slice(0, 3).map(x => ({
          remoteJid: x?.key?.remoteJid,
          participant: x?.key?.participant,
          fromMe: x?.key?.fromMe,
          id: x?.key?.id,
          hasMessage: !!x?.message,
          messageKeys: x?.message ? Object.keys(x.message) : [],
          remoteJidAlt: x?.key?.remoteJidAlt,
          participantAlt: x?.key?.participantAlt,
          senderPn: x?.message?.senderPn,
          participantPn: x?.message?.participantPn,
        }))
      })
    } catch (e) {
      console.warn("[B7 RAW messages.upsert logger failed]", e?.message || e)
    }
    if (type !== 'notify') return

    const msgData = messages?.[0]
    if (!msgData) return
    if (socketGeneration[tenantId] !== gen) return

    const stubType = msgData.messageStubType
    if (stubType === STUB_CIPHERTEXT) {
      const rjStub = String(msgData?.key?.remoteJid || "").trim()

      if (
        rjStub &&
        !rjStub.endsWith("@g.us") &&
        rjStub !== "status@broadcast" &&
        !rjStub.includes("newsletter")
      ) {
        tlog(tenantId, "[WATCHDOG] ciphertext stub detected → repairing peer session", {
          tenantId,
          stubType,
          rj: rjStub.slice(0, 56),
        })

        recoverPeerSignalSession(sock, tenantId, rjStub, "ciphertext_stub")
      }
    } else if (stubType === STUB_PAYMENT_CIPHERTEXT) {
      const rjStub = String(msgData?.key?.remoteJid || "").trim()
      if (rjStub && !rjStub.endsWith("@g.us") && rjStub !== "status@broadcast" && !rjStub.includes("newsletter")) {
        console.warn("[WATCHDOG] messageStubType ciphertext → reset peer", { tenantId, stubType, rj: rjStub.slice(0, 56) })
        recoverPeerSignalSession(sock, tenantId, rjStub, "messageStubType")
      }
    }

    try {
      const runtimeRemoteJid = String(msgData?.key?.remoteJid || "").trim()
      const runtimeParticipant = String(msgData?.key?.participant || "").trim()
      if (
        runtimeRemoteJid &&
        runtimeParticipant &&
        runtimeRemoteJid !== runtimeParticipant &&
        runtimeParticipant.endsWith("@s.whatsapp.net")
      ) {
        lidMapping.set(runtimeRemoteJid, runtimeParticipant)
      }
    } catch (_) {
      // ignore runtime mapping errors
    }

    const m = msgData.message || {}

    // DETECTAR RESPUESTA CITADA (swipe / reply) — stanzaId para inventario automotive + Menz.
    const quotedId =
      m?.extendedTextMessage?.contextInfo?.stanzaId ||
      m?.imageMessage?.contextInfo?.stanzaId ||
      m?.videoMessage?.contextInfo?.stanzaId ||
      m?.documentMessage?.contextInfo?.stanzaId ||
      m?.buttonsResponseMessage?.contextInfo?.stanzaId ||
      m?.listResponseMessage?.contextInfo?.stanzaId ||
      m?.templateButtonReplyMessage?.contextInfo?.stanzaId ||
      null

    if (quotedId) {
      tlog(tenantId, `[tenant:${tenantId}] QUOTED PRODUCT DETECTED`, quotedId)
    }

    const remoteJid = msgData.key.remoteJid || ""
    const msgKeyId = String(msgData?.key?.id || "").trim()
    const fromMe = !!msgData.key.fromMe
    // Solo deduplicar salientes / reintentos fromMe; inbound real nunca se descarta por id repetido.
    if (fromMe && markInboundSeen(tenantId, msgKeyId, remoteJid)) {
      tlog(tenantId, "↷ duplicate upsert ignored (fromMe)", { tenantId, remoteJid, msgKeyId })
      return
    }
    const participant = msgData.key.participant || ""
    // permitir que mensajes enviados desde operador también se envíen al backend
    // (evitamos loops en Flask, no aquí)


    // Tenant/company must always come from the active WA session (never from JID/LID).
    const sessionCompanyId = Number(tenantId) || 1

    // Salientes (fromMe): remoteJid suele ser @lid del chat; el peer real viene en participant.
    const operatorNumber = (process.env.WHATSAPP_OPERATOR_NUMBER || "").replace(/\D/g, "")
    const operatorJid = operatorNumber ? `${operatorNumber}@s.whatsapp.net` : ""

    const senderJid =
      fromMe
        ? (operatorJid || participant || remoteJid)
        : (remoteJid === "status@broadcast" ? participant : remoteJid)

    const chatSourceJid = senderJid
    const webhookChatJid = resolveCanonicalChatJid(chatSourceJid, sessionCompanyId)

    const canonicalRemoteJid = String(
      msgData?.key?.participant ||
      msgData?.key?.remoteJid ||
      ""
    ).trim()
    const rjLidCheck = String(remoteJid || "").trim()
    // Prioridad 1) senderPn 2) participantPn 3) remoteJidAlt 4) participantAlt — antes que MSISDN desde JID (participant/remoteJid en key).
    let phone = null
    if (msgData?.key) {
      const k = msgData.key
      const at =
        (msgData.attrs && typeof msgData.attrs === "object" && msgData.attrs) || null
      const senderPnRaw =
        k.senderPn || msgData.senderPn || (at && (at.sender_pn || at.senderPn)) || ""
      const participantPnRaw = k.participantPn || msgData.participantPn || ""
      const remoteJidAltRaw = k.remoteJidAlt || msgData.remoteJidAlt || ""
      const participantAltRaw = k.participantAlt || msgData.participantAlt || ""
      const tryOrder = [
        ["senderPn", senderPnRaw],
        ["participantPn", participantPnRaw],
        ["remoteJidAlt", remoteJidAltRaw],
        ["participantAlt", participantAltRaw],
      ]
      let altDigits = ""
      let pnSource = ""
      for (const [name, raw] of tryOrder) {
        const d = tryExtractLatamMsisdnFromPnField(raw)
        if (d) {
          altDigits = d
          pnSource = name
          break
        }
      }
      if (altDigits) {
        phone = altDigits
        const lidJid0 = lidJidForLidToMsisdnMap(rjLidCheck, canonicalRemoteJid)
        if (lidJid0) {
          lidMapping.set(lidJid0, `${altDigits}@s.whatsapp.net`)
          void persistJidMapLidToPhone(tenantId, lidJid0, altDigits)
        }
        tlog(tenantId, "[PHONE RESOLVER Pn/Alt]", {
          remoteJid: rjLidCheck,
          resolvedPhone: altDigits,
          source: pnSource,
          hadSenderPn: !!String(senderPnRaw || "").trim(),
          hadParticipantPn: !!String(participantPnRaw || "").trim(),
        })
      }
    }
    if (
      !phone &&
      canonicalRemoteJid.endsWith("@s.whatsapp.net") &&
      !canonicalRemoteJid.includes(":")
    ) {
      const digits = normalizeMxDigits(
        canonicalRemoteJid.replace("@s.whatsapp.net", "")
      )
      if (
        /^\d{10,15}$/.test(digits) &&
        !/^(1\d{14}|[2678]\d{10,14})$/.test(digits) &&
        digits.startsWith("5")
      ) {
        phone = digits
      }
    }
    tlog(tenantId, "[PHONE RESOLVER]", {
      participant: msgData?.key?.participant,
      remoteJid: msgData?.key?.remoteJid,
      resolvedPhone: phone
    })

    let resolvedPhone = phone

    if (!resolvedPhone && remoteJid) {
      const rj = String(remoteJid || "").trim()

      if (rj.endsWith("@s.whatsapp.net")) {
        const digits = rj.replace("@s.whatsapp.net", "").replace(/\D/g, "")

        if (digits.length >= 15) {
          tlog(tenantId, "[LID DETECTED — IGNORING AS PHONE]", canonicalRemoteJid)
          resolvedPhone = null
          if (!resolvedPhone && canonicalRemoteJid?.endsWith("@s.whatsapp.net")) {
            resolvedPhone = canonicalRemoteJid.replace("@s.whatsapp.net", "").replace(/\D/g, "")
            tlog(tenantId, "[PHONE RESOLVER LID FALLBACK ACTIVATED]", resolvedPhone)
          }
        }
      }
    }

    if (!resolvedPhone && participant) {
      const p = String(participant || "").trim()

      if (p.endsWith("@s.whatsapp.net")) {
        resolvedPhone = p.replace("@s.whatsapp.net", "").replace(/\D/g, "")

        tlog(tenantId, "[PHONE RESOLVER FALLBACK PARTICIPANT]", {
          participant: p,
          resolvedPhone
        })
      }
    }

    if (!resolvedPhone) {
      const rj = String(remoteJid || "").trim()

      if (rj.endsWith("@s.whatsapp.net") && !rj.includes(":")) {
        const digits = rj.replace("@s.whatsapp.net", "").replace(/\D/g, "")

        if (digits.length < 15) {
          resolvedPhone = digits

          tlog(tenantId, "[PHONE RESOLVER FALLBACK MSISDN]", {
            remoteJid: rj,
            resolvedPhone
          })
        }
      }
    }

    if (!resolvedPhone) {
      // Last resort: consult jid_map in DB
      try {
        const lidKey = String(remoteJid || canonicalRemoteJid || "").trim()
        if (lidKey) {
          const db = await mysql.createConnection({
            host: process.env.DB_HOST || "127.0.0.1",
            user: process.env.DB_USER || "root",
            password: process.env.MYSQL_PASSWORD || "PasswordSeguro123",
            database: process.env.DB_NAME || "tuexpo_voice"
          })
          const [rows] = await db.query(
            "SELECT phone FROM jid_map WHERE remote_jid = ? LIMIT 1",
            [lidKey]
          )
          await db.end()
          if (rows && rows[0] && rows[0].phone) {
            resolvedPhone = rows[0].phone
            tlog(tenantId, "[PHONE RESOLVED FROM jid_map]", { lidKey, resolvedPhone })
          }
        }
      } catch (e) {
        console.warn("[jid_map lookup failed]", e?.message)
      }
    }

    if (!resolvedPhone) {
      const forLid = String(remoteJid || canonicalRemoteJid || "").trim()
      const localDigits = waLocalDigitsFromLidJid(forLid)
      if (/^\d{10,20}$/.test(localDigits)) {
        resolvedPhone = localDigits
        tlog(tenantId, "[PHONE RESOLVER LID CANONICAL]", { remoteJid: forLid, resolvedPhone: localDigits })
      } else {
        tlog(tenantId, "[PHONE DROPPED INVALID]", canonicalRemoteJid)
      }
    }

    const _resolvedDigits = String(resolvedPhone || "").replace(/\D/g, "")
    if (resolvedPhone && _resolvedDigits.length > 25) {
      tlog(tenantId, "[IGNORED INVALID LONG ID]", remoteJid)
      resolvedPhone = null
    }

    phone = resolvedPhone
    const sender = resolvedPhone

    const hasMedia = !!(
      m.imageMessage ||
      m.videoMessage ||
      m.audioMessage ||
      m.pttMessage ||
      m.stickerMessage ||
      m.documentMessage ||
      m.ptvMessage
    )

    const rawText =
      msgData.message?.conversation ||
      msgData.message?.extendedTextMessage?.text ||
      msgData.message?.ephemeralMessage?.message?.conversation ||
      msgData.message?.ephemeralMessage?.message?.extendedTextMessage?.text ||
      msgData.message?.viewOnceMessage?.message?.conversation ||
      msgData.message?.viewOnceMessage?.message?.extendedTextMessage?.text ||
      msgData.message?.imageMessage?.caption ||
      msgData.message?.videoMessage?.caption ||
      ""

    const message = String(rawText || "").trim() || (hasMedia ? "[media]" : "")

    // Filtrar eventos de infraestructura Signal (no son mensajes reales)
    if (!message && !hasMedia) {
      const isInfraEvent = !!(
        msgData.message?.protocolMessage ||
        msgData.message?.senderKeyDistributionMessage ||
        msgData.message?.historySyncNotification ||
        msgData.messageStubType
      )
      if (isInfraEvent) {
        return
      }
      tlog(tenantId, "[MESSAGE DROPPED EMPTY PAYLOAD]")
      return
    }


    const textNorm = String(message).trim().toLowerCase()
    const isControlCommand = ["activar", "ativar", "desactivar", "desativar"].includes(textNorm)
    // Ignore outbound messages written from the linked phone to avoid bot auto-replies
    // (e.g. accidental "Greetings" loop after a human sends from device).
    // Keep explicit control commands allowed.
    if (fromMe && !isControlCommand) {
      let db
      try {
        db = await mysql.createConnection({
          host: "localhost",
          user: "root",
          password: process.env.MYSQL_PASSWORD || "PasswordSeguro123",
          database: "tuexpo_voice",
        })
        try {
          await db.execute(
            `INSERT INTO messages (phone, tenant_id, company_id, direction, message, source)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [phone, sessionCompanyId, sessionCompanyId, "out", message || "", "mobile"]
          )
        } catch (eInsert) {
          if (String(eInsert?.message || "").toLowerCase().includes("company_id")) {
            await db.execute(
              `INSERT INTO messages (phone, tenant_id, direction, message, source)
               VALUES (?, ?, ?, ?, ?)`,
              [phone, sessionCompanyId, "out", message || "", "mobile"]
            )
          } else {
            throw eInsert
          }
        }
      } catch (err) {
        console.error("outbound message DB save failed", err.message)
      } finally {
        if (db) await db.end()
      }

      return
    }

    if (
      remoteJid === "status@broadcast" ||
      remoteJid.endsWith("@g.us") ||
      remoteJid.includes("newsletter")
    ) {
      return
    }

    const textForBrain = String(message || "").trim() || (hasMedia ? "[media]" : "")
    if (!textForBrain) {
      return
    }

    tlog(tenantId, "CHAT DETECTADO:", remoteJid)
    tlog(tenantId, "CHAT PARTICIPANT:", participant)
    if (webhookChatJid !== chatSourceJid) {
      tlog(tenantId, "CHAT NORMALIZED:", webhookChatJid, "src:", chatSourceJid)
    }

    // 🔥 BLOQUEO DEL GRUPO
    if (remoteJid?.includes("120363406188468587")) {
      tlog(tenantId, "🚫 BLOQUEADO:", remoteJid)
      return
    }

    tlog(tenantId, "MENSAJE ENTRANTE:", textForBrain)

    const pushName = String(msgData.pushName || "").trim()
    if (!fromMe && phone && pushName) {
      await saveContactPushName(phone, pushName, sessionCompanyId)
    }

    if (!fromMe && phone && webhookChatJid && !webhookChatJid.endsWith("@g.us")) {
      void saveContactProfilePicUrl(sock, phone, webhookChatJid, sessionCompanyId).catch((e) =>
        console.warn("saveContactProfilePicUrl", e.message || e)
      )
    }

    // Persist inbound WhatsApp messages to DB (non-fatal on errors).
    let persist = { mediaType: null, mediaUrl: null, mimeType: null, ptt: null, mediaBase64: null }
    if (!fromMe && phone && textForBrain) {
      persist = await persistIncomingMedia(sock, msgData, sessionCompanyId)
      false && await saveMessageToDb(phone, "in", textForBrain, "client", sessionCompanyId, persist.mediaType, persist.mediaUrl)
    }

    try {
      if (sender && String(sender).endsWith("@lid")) {
        tlog(tenantId, "[SKIP RELAY ENVELOPE DUPLICATE EVENT]", sender)
        return
      }

      const webhookPhone = normalizeMxDigits(String(phone || sender || "").replace(/\D/g, ""))
      tlog(tenantId, "→ Enviando a Flask...", TUEXPO_WHATSAPP_INCOMING_URL)
      const logPayload = {
        company_id: sessionCompanyId,
        tenant_id: sessionCompanyId,
        sender: resolvedPhone || remoteJid,
        remoteJid: webhookChatJid,
        rawRemoteJid: remoteJid,
      }
      if (persist.mediaType) {
        logPayload.media_type = persist.mediaType
        logPayload.media_url = persist.mediaUrl || null
        logPayload.mime_type = persist.mimeType || null
        logPayload.ptt = persist.ptt
        if (persist.mediaType === "audio" && persist.mediaBase64) {
          logPayload.media_base64 = `<${persist.mediaBase64.length} chars>`
        }
      }
      tlog(tenantId, "WEBHOOK PAYLOAD:", logPayload)
      const participantMsisdn = (() => {
        const p = String(participant || "").trim()
        if (!p.endsWith("@s.whatsapp.net") || p.includes(":")) return ""
        return normalizeMxDigits(p.replace("@s.whatsapp.net", "").replace(/\D/g, ""))
      })()
      const phoneForWebhook = String(resolvedPhone || participantMsisdn || webhookPhone || "").trim()
      const k = msgData.key || {}
      const flaskBody = {
        phone: phoneForWebhook,
        sender: phoneForWebhook || resolvedPhone || remoteJid,
        remoteJid: webhookChatJid,
        rawRemoteJid: String(k.remoteJid || remoteJid || "").trim(),
        remoteJidAlt: k.remoteJidAlt,
        participantAlt: k.participantAlt,
        senderPn: k.senderPn,
        participantPn: k.participantPn,
        key: {
          id: k.id,
          remoteJid: k.remoteJid,
          remoteJidAlt: k.remoteJidAlt,
          participant: k.participant,
          participantAlt: k.participantAlt,
          senderPn: k.senderPn,
          participantPn: k.participantPn,
          fromMe: !!k.fromMe,
        },
        tenant_id: sessionCompanyId,
        company_id: sessionCompanyId,
        participant,
        fromMe,
        text: textForBrain,
        message: textForBrain,
        // Swipe / reply: panel + automotive_brain (resolve_product_from_stanza + catalog_stanza_ids).
        quoted_id: quotedId,
        catalog_stanza_ids: catalogMessageIdsByTenant[sessionCompanyId] || [],
      }
      if (persist.mediaType) {
        flaskBody.media_type = persist.mediaType
        flaskBody.media_url = persist.mediaUrl || null
        flaskBody.mime_type = persist.mimeType || null
        flaskBody.ptt = persist.ptt === true
        if (persist.mediaType === "audio" && persist.mediaBase64) {
          flaskBody.media_base64 = persist.mediaBase64
          flaskBody.mime_type = persist.mimeType || "audio/ogg; codecs=opus"
        }
      }
      const inboundDebounceKey =
        (!fromMe && !flaskBody.media_type && !flaskBody.quoted_id && phoneForWebhook)
          ? `${sessionCompanyId}:${phoneForWebhook}`
          : ""

      const res = await debounceIncomingToFlask(
        inboundDebounceKey,
        flaskBody,
        (body) => axios.post(TUEXPO_WHATSAPP_INCOMING_URL, body)
      )


      tlog(tenantId, "← Respuesta de Flask:", res.data)
      const reply = res.data?.response
      const canonicalPhoneFromFlask = normalizeMxDigits(
        String(res.data?.canonical_phone || "").replace(/\D/g, "")
      )

      if (!reply) return
      if (!isAutoReplyEnabledForTenant(sessionCompanyId)) {
        console.warn("[SAFE MODE] auto-reply suppressed", {
          tenant: sessionCompanyId,
          phone,
          remoteJid: msgData.key.remoteJid,
        })
        return
      }

      // Operator control commands are internal. Do not show these acks to the client chat.
      const controlAcks = new Set([
        "Automatización desactivada.",
        "Automatización activada.",
        "Automação ativada.",
      ])
      if (fromMe && isControlCommand && controlAcks.has(String(reply).trim())) {
        tlog(tenantId, "↷ Control ACK suppressed from client chat")
        return
      }

      const replySock = sessions[sessionCompanyId]
      if (!replySock) {
        console.error("No socket for tenant when sending reply", sessionCompanyId)
        return
      }

      const canonical_phone = canonicalPhoneFromFlask
      if (!canonical_phone) {
        tlog(tenantId, "[BLOCKED OUTBOUND: missing canonical_phone]", {
          remoteJid,
          participant
        })
        return
      }

      const inboundChatJid = String(remoteJid || "").trim()
      let targetJid = null
      // Mismo hilo que el inbound: si el chat llegó como @lid, transportar la respuesta al @lid (MSISDN sigue en DB).
      if (inboundChatJid.endsWith("@lid")) {
        targetJid = inboundChatJid
        tlog(tenantId, "[OUTBOUND transport @lid same-thread]", inboundChatJid.slice(0, 56))
      } else {
        targetJid = `${canonical_phone}@s.whatsapp.net`
      }

      const msisdnTransportJid = `${canonical_phone}@s.whatsapp.net`

      tlog(tenantId, "Outbound targetJid resolved:", {
        tenant: sessionCompanyId,
        targetJid,
        sender,
        remoteJid,
        participant,
      })

      async function sendReplyTransport(content, sendOne = null) {
        if (inboundChatJid.endsWith("@lid")) {
          try {
            return await sendMessageWithMxFallback(
              replySock,
              targetJid,
              content,
              sendOne,
              sessionCompanyId
            )
          } catch (e) {
            tlog(tenantId, "[OUTBOUND @lid send failed → MSISDN fallback]", e?.message || e)
            return await sendMessageWithMxFallback(
              replySock,
              msisdnTransportJid,
              content,
              sendOne,
              sessionCompanyId
            )
          }
        }
        return await sendMessageWithMxFallback(
          replySock,
          targetJid,
          content,
          sendOne,
          sessionCompanyId
        )
      }

      function catalogImageCaption(name, priceRaw) {
        const nameStr = String(name || "Produto")
        const p = priceRaw != null && priceRaw !== "" ? String(priceRaw).trim() : ""
        let caption
        if (!p || p === "-") {
          caption = nameStr
        } else if (/^r\$/i.test(p)) {
          caption = `${nameStr}\n${p}`
        } else {
          caption = `${nameStr}\nR$ ${p}`
        }
        return caption
      }

      async function sendWhatsappImage(imageUrl, caption, opts = {}) {
        const msg = await sendReplyTransport({
          image: { url: imageUrl },
          caption: String(caption || ""),
        })

        const stanzaId =
          msg?.key?.id ||
          msg?.messages?.[0]?.key?.id ||
          null

        if (stanzaId) {
          if (!catalogMessageIdsByTenant[sessionCompanyId]) {
            catalogMessageIdsByTenant[sessionCompanyId] = []
          }
          catalogMessageIdsByTenant[sessionCompanyId].push(stanzaId)
        }

        const isAffiliate = Boolean(opts && opts.affiliate)
        const isCatalog = Boolean(opts && opts.catalog)
        const mediaType = isAffiliate ? "affiliate_product" : "image"
        /** @type {Record<string, unknown>} */
        const metaObj = {}
        if (stanzaId) {
          metaObj.stanza_id = stanzaId
        }
        if (isAffiliate) {
          const brand =
            typeof opts.brand === "string" && opts.brand
              ? opts.brand
              : inferAffiliateBrandFromCaption(caption)
          const { model, price, link } = parseAffiliateProductCaption(caption)
          Object.assign(metaObj, {
            affiliate: true,
            source: opts.source || "vtex",
            brand,
            model,
            price_brl: price,
            link,
            image_url: imageUrl,
            persisted_at: new Date().toISOString(),
          })
        } else if (isCatalog && opts.catalogItem) {
          const it = opts.catalogItem
          const nm = String(it?.name || "").trim() || undefined
          const pr = it?.price != null && it.price !== "" ? String(it.price) : undefined
          Object.assign(metaObj, {
            model: nm,
            name: nm,
            price_brl: pr,
            image_url: imageUrl,
            link: it?.link ? String(it.link) : undefined,
          })
        } else if (opts && opts.automotive_vehicle && typeof opts.automotive_vehicle === "object") {
          const av = opts.automotive_vehicle
          Object.assign(metaObj, {
            automotive_inventory: true,
            source: "bndv_inventory",
            title: av.title ? String(av.title) : undefined,
            brand: av.brand ? String(av.brand) : undefined,
            model: av.model ? String(av.model) : undefined,
            year_model: av.year_model ? String(av.year_model) : undefined,
            price_brl: av.price_brl != null && av.price_brl !== "" ? String(av.price_brl) : undefined,
            image_url: imageUrl,
            persisted_at: new Date().toISOString(),
          })
        }
        const metaJson =
          Object.keys(metaObj).length > 0 ? JSON.stringify(metaObj) : null
        await saveMessageToDb(
          phone,
          "out",
          caption || "(imagem)",
          "ai",
          sessionCompanyId,
          mediaType,
          imageUrl,
          metaJson
        )
      }

      if (reply && typeof reply === "object" && reply.type === "catalog" && Array.isArray(reply.items)) {
        if (reply.preface) {
          const pre = String(reply.preface).trim()
          if (pre) {
            await sendReplyTransport({ text: pre })
            false && await saveMessageToDb(phone, "out", pre, "ai", sessionCompanyId, null, null)
          }
        }
        for (const item of reply.items) {
          const name = String(item?.name || "Produto")
          const price = item?.price != null ? item.price : "-"
          const imageUrl = item?.image ? String(item.image) : ""
          const caption = catalogImageCaption(name, price)

          if (imageUrl) {
            await sendWhatsappImage(imageUrl, caption, {
              catalog: true,
              catalogItem: {
                name,
                price,
                image: imageUrl,
                link: item?.link,
              },
            })
          } else {
            await sendReplyTransport({ text: caption })
            false && await saveMessageToDb(phone, "out", caption, "ai", sessionCompanyId, null, null)
          }
        }
        return
      }

      // Afiliado VTEX (MENZ): texto introdutório + uma imagem por produto (caption = nome/preço/link).
      const affiliateImages = Array.isArray(res.data?.images) ? res.data.images : []
      const affiliateCaptions = Array.isArray(res.data?.captions) ? res.data.captions : []
      const affiliatePreface =
        typeof res.data?.preface === "string" ? res.data.preface.trim() : ""
      if (affiliateImages.length > 0) {
        if (affiliatePreface) {
          await sendReplyTransport({
            text: affiliatePreface,
          })
          false && await saveMessageToDb(phone, "out", affiliatePreface, "ai", sessionCompanyId, null, null)
        } else if (reply) {
          const fallbackIntro = String(reply).trim()
          if (fallbackIntro) {
            await sendReplyTransport({ text: fallbackIntro })
            false && await saveMessageToDb(phone, "out", fallbackIntro, "ai", sessionCompanyId, null, null)
          }
        }
        for (let i = 0; i < affiliateImages.length; i++) {
          const imageUrl = String(affiliateImages[i] || "").trim()
          if (!imageUrl) continue
          const caption = String(affiliateCaptions[i] || "").trim()
          await sendWhatsappImage(imageUrl, caption, {
            affiliate: true,
            source: "vtex",
            brand: /mizuno/i.test(caption) ? "mizuno" : "olympikus",
          })
        }
        return
      }



      //
      // 🔊 AUDIO RESPONSE SUPPORT (Tuexpo Voice TTS)
      //

      if (res?.data?.response_type === "audio" && res?.data?.response_audio_path) {

        try {

          const buffer = fs.readFileSync(res.data.response_audio_path)

          const sent = await sendReplyTransport({
            audio: buffer,
            mimetype: "audio/ogg; codecs=opus",
            ptt: true,
          })

          const mediaUrl = await persistOutboundMediaBuffer(
            buffer,
            "audio/ogg; codecs=opus",
            sessionCompanyId,
            "tts"
          )
          await saveMessageToDb(
            phone,
            "out",
            "[audio]",
            "ai",
            sessionCompanyId,
            "audio",
            mediaUrl,
            JSON.stringify({
              wa_key_id: sent?.key?.id || null,
              whatsapp_status: "sent",
              whatsapp_ack: 1,
              mime_type: "audio/ogg; codecs=opus",
            })
          )

          tlog(tenantId, "🎙️ Audio reply sent successfully")

          const imgsAfterAudio = Array.isArray(res?.data?.response_images) ? res.data.response_images : []
          for (const item of imgsAfterAudio) {
            const imageUrl = String(item?.image_url ?? "").trim()
            if (!imageUrl) continue
            try {
              const cap = String(item?.caption ?? "").trim()
              await sendWhatsappImage(imageUrl, cap, {
                automotive_vehicle: {
                  title: String(item?.title || "").trim() || cap.split("—")[0]?.trim() || "",
                  brand: String(item?.brand || "").trim(),
                  model: String(item?.model || "").trim(),
                  year_model: String(item?.year_model || "").trim(),
                  price_brl: item?.price != null ? String(item.price) : "",
                },
              })
            } catch (imgErr) {
              tlog(tenantId, "❌ Inventory image after audio failed:", imgErr)
            }
          }
          if (imgsAfterAudio.length) {
            tlog(tenantId, "🚗 Inventory image(s) sent after audio")
          }

          return

        } catch (err) {

          tlog(tenantId, "❌ Audio send failed:", err)

        }

      }

      // 🚗 Inventario: hasta 3 fotos (por response_images, no por response_type: con TTS sigue siendo "audio" + imágenes)
      if (Array.isArray(res?.data?.response_images) && res.data.response_images.length) {
        let sentSomething = false
        const intro = String(reply || "").trim()
        if (intro) {
          try {
            await sendReplyTransport({ text: intro })
            false && await saveMessageToDb(phone, "out", intro, "ai", sessionCompanyId, null, null)
            sentSomething = true
          } catch (txtErr) {
            tlog(tenantId, "❌ Inventory intro text failed:", txtErr)
          }
        }
        for (const item of res.data.response_images) {
          const imageUrl = String(item?.image_url ?? "").trim()
          if (!imageUrl) continue
          try {
            const cap = String(item?.caption ?? "").trim()
            const mediaType = String(item?.media_type ?? "image").trim()
            if (mediaType === "video") {
              await sendReplyTransport({ video: { url: imageUrl }, caption: cap })
              tlog(tenantId, "🎬 Education video sent:", imageUrl.slice(-40))
            } else {
              await sendWhatsappImage(imageUrl, cap, {
                automotive_vehicle: {
                  title: String(item?.title || "").trim() || cap.split("—")[0]?.trim() || "",
                  brand: String(item?.brand || "").trim(),
                  model: String(item?.model || "").trim(),
                  year_model: String(item?.year_model || "").trim(),
                  price_brl: item?.price != null ? String(item.price) : "",
                },
              })
            }
            sentSomething = true
          } catch (imgErr) {
            tlog(tenantId, "❌ Inventory multi_image item failed:", imgErr)
          }
        }
        if (sentSomething) {
          tlog(tenantId, "🚗 Inventory multi_image reply sent")
          return
        }
      }

      tlog(tenantId, "→ Enviando respuesta a WhatsApp:", reply)

      await sendToPreferredOrMsisdnVariants(
        replySock,
        targetJid,
        { text: String(reply || "") },
        null,
        canonical_phone,
        sessionCompanyId
      )

      await saveMessageToDb(
        phone,
        "out",
        String(reply || ""),
        "ai",
        sessionCompanyId,
        null,
        null
      )

    } catch (e) {

      console.error("❌ Error calling Flask:", e.response?.data || e.message)

    }
  })
}

app.get("/health", (req, res) => {
  res.json({ status: "ok", tenants: statusStore })
})

app.get("/qr", (req, res) => {
  const companyId = tenantFromRequest(req)
  if (!isTenantEnabled(companyId)) {
    return res.status(403).json({ company_id: companyId, status: "disabled", error: "tenant_disabled" })
  }
  const updatedAt = qrUpdatedAtStore[companyId] || Date.now()
  const ageSec = Math.floor((Date.now() - updatedAt) / 1000)
  const expiresIn = Math.max(0, 120 - ageSec)
  const sock = sessions[companyId]
  // Prefer explicit connection state; sock.user may persist briefly after disconnect.
  const computedStatus = statusStore[companyId] || (sock && sock.user ? "connected" : "unknown")
  const qr = qrStore[companyId] || null
  if (!qr) {
    return res.json({
      company_id: companyId,
      status: computedStatus,
      qr: null,
      qr_base64: null,
      qr_updated_at: new Date(updatedAt).toISOString(),
      qr_expires_in_seconds: expiresIn,
    })
  }
  qrcodePng.toDataURL(qr, { margin: 1, scale: 6 }, (err, dataUrl) => {
    const b64 = !err && typeof dataUrl === "string" && dataUrl.includes(",")
      ? dataUrl.split(",")[1]
      : null
    res.json({
      company_id: companyId,
      status: computedStatus,
      qr,
      qr_base64: b64,
      qr_updated_at: new Date(updatedAt).toISOString(),
      qr_expires_in_seconds: expiresIn,
    })
  })
})

app.post("/connect", async (req, res) => {
  const companyId = tenantFromRequest(req)
  if (!isTenantEnabled(companyId)) {
    return res.status(403).json({ company_id: companyId, status: "disabled", error: "tenant_disabled" })
  }
  try {
    if (connectInProgress[companyId]) {
      return res.status(429).json({ error: "connect_in_progress" })
    }
    connectInProgress[companyId] = true

    qrStore[companyId] = null
    statusStore[companyId] = "waiting_qr"
    qrUpdatedAtStore[companyId] = Date.now()

    // Ventana de QR abierta únicamente por acción humana desde panel/settings.
    qrAllowedUntilStore[companyId] = Date.now() + 2 * 60 * 1000
    delete reconnectBlockedUntilStore[companyId]

    await startWhatsApp(companyId, true)
    res.json({ company_id: companyId, status: "waiting_qr" })
  } catch (e) {
    console.error("connect failed:", e.message || e)
    res.status(500).json({ error: "connect_failed", detail: e.message || e })
  } finally {
    connectInProgress[companyId] = false
  }
})

app.post("/send", async (req, res) => {

  const number = req.body?.number || req.body?.phone || req.body?.to || req.body?.msisdn
  const message = req.body?.message || req.body?.text || req.body?.body || ""
  const plainSendText = plainTextForConnectorSend(message)
  const mediaBase64Legacy = String(req.body?.media_base64 || req.body?.base64 || "").trim()
  const companyId = tenantFromRequest(req)
  if (!isTenantEnabled(companyId)) {
    return res.status(403).json({ error: "tenant_disabled", company_id: companyId })
  }
  let sock = sessions[companyId]

  // Lazy bootstrap: after a restart we only start tenant 1 by default.
  // If a tenant tries to send while its socket isn't started, start it automatically
  // (without forcing a new QR).
  if (!sock) {
    try {
      await startWhatsApp(companyId, false)
      sock = sessions[companyId]
    } catch (e) {
      return res.status(503).json({
        error: "whatsapp_not_connected",
        company_id: companyId,
        status: statusStore[companyId] || "unknown",
        detail: String(e?.message || e || "start_failed"),
      })
    }
  }

  if (!sock || !sock.user) {
    return res.status(503).json({
      error: "whatsapp_not_connected",
      company_id: companyId,
      status: statusStore[companyId] || "unknown",
    })
  }

  // Backward compatibility: old panel versions post media to /send.
  if (mediaBase64Legacy) {
    const mimetype = String(req.body?.mime_type || req.body?.mimetype || "application/octet-stream").trim()
    const caption = String(req.body?.caption || plainSendText || "").trim()
    const filename = String(req.body?.filename || "attachment").trim() || "attachment"
    let buffer = null
    try {
      buffer = Buffer.from(mediaBase64Legacy, "base64")
    } catch (_) {
      buffer = null
    }
    if (!buffer || buffer.length === 0) return res.status(400).json({ error: "invalid_base64" })

    const mm = mimetype.toLowerCase()
    const mediaType = mediaTypeFromMime(mm)
    let content = {}
    if (mediaType === "image") {
      content = { image: buffer, mimetype, caption: caption || undefined }
    } else if (mediaType === "video") {
      content = { video: buffer, mimetype, caption: caption || undefined }
    } else if (mediaType === "audio") {
      content = { audio: buffer, mimetype, ptt: mm.includes("ogg") || mm.includes("opus") }
    } else {
      content = { document: buffer, mimetype, fileName: filename, caption: caption || undefined }
    }

    const phone = String(number || "").replace(/\D/g, "")
    if (!phone) {
      return res.status(400).json({ error: "missing_number" })
    }
    const candidatesMedia = buildWhatsAppPhoneCandidates(phone)
    const preferredMediaJid = outboundJidFromRequestBody(req.body)
    if (false && preferredMediaJid) {
      try {
        const fallbackPhone = phone
        if (preferredMediaJid?.endsWith("@lid") && fallbackPhone) {
          preferredMediaJid = `${fallbackPhone}@s.whatsapp.net`
        }
        const sent = await sendToPreferredOrMsisdnVariants(
          sock,
          preferredMediaJid,
          content,
          undefined,
          phone,
          companyId
        )
        tlog(companyId, "[OUTBOUND USING remoteJid]", preferredMediaJid)
        const canonicalPhone = normalizeMxDigits(phone)
        const mediaUrl = await persistOutboundMediaBuffer(buffer, mimetype, companyId, "send")
        await saveMessageToDb(
          canonicalPhone,
          "out",
          caption || "[media]",
          "web",
          companyId,
          mediaType,
          mediaUrl,
          JSON.stringify({
            wa_key_id: sent?.key?.id || null,
            whatsapp_status: "sent",
            whatsapp_ack: 1,
            mime_type: mimetype,
          })
        )
        return res.json({
          sent: true,
          jid: preferredMediaJid,
          normalized_number: canonicalPhone,
          media_type: mediaType,
          key_id: sent?.key?.id || null,
          key: sent?.key || null,
        })
      } catch (ePref) {
        console.warn("[OUTBOUND remoteJid failed, falling back to MSISDN]", ePref?.message || ePref)
      }
    }
    let lastErrMedia = null
    for (const cand of candidatesMedia) {
      const jid = cand + "@s.whatsapp.net"
      try {
        const sent = await sendMessageReliable(sock, jid, content, {}, phone)
        rememberSentProtoMessage(companyId, sent)
        const canonicalPhone = normalizeMxDigits(cand)
        const mediaUrl = await persistOutboundMediaBuffer(buffer, mimetype, companyId, "send")
        await saveMessageToDb(
          canonicalPhone,
          "out",
          caption || "[media]",
          "web",
          companyId,
          mediaType,
          mediaUrl,
          JSON.stringify({
            wa_key_id: sent?.key?.id || null,
            whatsapp_status: "sent",
            whatsapp_ack: 1,
            mime_type: mimetype,
          })
        )
        return res.json({
          sent: true,
          jid,
          normalized_number: cand,
          media_type: mediaType,
          key_id: sent?.key?.id || null,
          key: sent?.key || null,
        })
      } catch (e) {
        lastErrMedia = e
      }
    }
    return res.status(502).json({
      error: "send_media_failed",
      detail: String(lastErrMedia?.message || lastErrMedia || "unknown"),
      tried: candidatesMedia,
    })
  }

  const phone = String(number || "").replace(/\D/g, "")
  if (!phone) {
    return res.status(400).json({ error: "missing_number" })
  }
  const digits = phone

  // Mexico nuance: some accounts may still be reachable only via 521XXXXXXXXXX.
  // Try candidates and send to the first one that works.
  const candidates = buildWhatsAppPhoneCandidates(digits)

  // NEW: remote image_url support (affiliate catalogs)
  const imageUrl = req.body?.image_url || null
  const caption = String(req.body?.caption || plainSendText || "").trim()

  if (imageUrl) {
    const imageContent = {
      image: { url: imageUrl },
      caption: caption || undefined,
    }

    const preferredImageJid = outboundJidFromRequestBody(req.body)
    if (preferredImageJid) {
      try {
        const sent = await sendToPreferredOrMsisdnVariants(
          sock,
          preferredImageJid,
          imageContent,
          null,
          digits,
          companyId
        )
        rememberSentProtoMessage(companyId, sent)

        const canonicalPhone = normalizeMxDigits(digits)

        await saveMessageToDb(
          canonicalPhone,
          "out",
          caption || "[image]",
          "web",
          companyId,
          "image",
          imageUrl,
          JSON.stringify({
            wa_key_id: sent?.key?.id || null,
            whatsapp_status: "sent",
            whatsapp_ack: 1,
          })
        )

        return res.json({
          sent: true,
          jid: preferredImageJid,
          normalized_number: canonicalPhone,
          key_id: sent?.key?.id || null,
        })
      } catch (ePrefImage) {
        console.warn("[OUTBOUND image_url preferredJid failed, falling back to MSISDN]", ePrefImage?.message || ePrefImage)
      }
    }

    let lastImageErr = null
    for (const cand of candidates) {
      const jid = cand + "@s.whatsapp.net"
      try {
        const sent = await sendMessageReliable(sock, jid, imageContent, {}, digits)
        rememberSentProtoMessage(companyId, sent)

        const canonicalPhone = normalizeMxDigits(cand)

        await saveMessageToDb(
          canonicalPhone,
          "out",
          caption || "[image]",
          "web",
          companyId,
          "image",
          imageUrl,
          JSON.stringify({
            wa_key_id: sent?.key?.id || null,
            whatsapp_status: "sent",
            whatsapp_ack: 1,
          })
        )

        return res.json({
          sent: true,
          jid,
          normalized_number: canonicalPhone,
          key_id: sent?.key?.id || null,
        })
      } catch (e) {
        lastImageErr = e
      }
    }

    return res.status(502).json({
      error: "send_remote_image_failed",
      detail: String(lastImageErr?.message || lastImageErr || "unknown"),
      tried: candidates,
    })
  }

  const persistPhone = normalizeMxDigits(digits)
  let preferredJid = outboundJidFromRequestBody(req.body)
  if (preferredJid) {
    try {
      const sent = await sendToPreferredOrMsisdnVariants(
        sock,
        preferredJid,
        { text: plainSendText },
        null,
        digits,
        companyId
      )
      tlog(companyId, "[OUTBOUND USING preferredJid]", preferredJid)
      if (req.body.source !== "ai") {
        await saveMessageToDb(persistPhone, "out", plainSendText, "web", companyId, null, null)
        req._msgSaved = true;
      }
      return res.json({
        sent: true,
        jid: preferredJid,
        normalized_number: persistPhone,
        key_id: sent?.key?.id || null,
        key: sent?.key || null,
      })
    } catch (ePref) {
      console.warn("[OUTBOUND preferredJid failed, falling back to MSISDN]", preferredJid, ePref?.message || ePref)
    }
  }

  let lastErr = null
  for (const cand of candidates) {
    const jid = cand + "@s.whatsapp.net"
    try {
      const sent = await sendMessageWithMxFallback(sock, jid, { text: plainSendText }, null, companyId)
      // Persist as canonical digits (52 + 10) so Inbox doesn't fork threads.
      const canonicalPhone = normalizeMxDigits(cand)
      if (req.body.source !== "ai" && !req._msgSaved) {
        await saveMessageToDb(canonicalPhone, "out", plainSendText, "web", companyId, null, null)
      }
      return res.json({
        sent: true,
        jid,
        normalized_number: cand,
        key_id: sent?.key?.id || null,
        key: sent?.key || null,
      })
    } catch (e) {
      lastErr = e
    }
  }
  return res.status(502).json({
    error: "send_failed",
    detail: String(lastErr?.message || lastErr || "unknown"),
    tried: candidates,
  })
})

app.post("/sendMedia", async (req, res) => {
  const companyId = tenantFromRequest(req)
  if (!isTenantEnabled(companyId)) {
    return res.status(403).json({ error: "tenant_disabled", company_id: companyId })
  }
  let sock = sessions[companyId]
  if (!sock) {
    try {
      await startWhatsApp(companyId, false)
      sock = sessions[companyId]
    } catch (e) {
      return res.status(503).json({
        error: "whatsapp_not_connected",
        company_id: companyId,
        status: statusStore[companyId] || "unknown",
        detail: String(e?.message || e || "start_failed"),
      })
    }
  }
  if (!sock || !sock.user) {
    return res.status(503).json({
      error: "whatsapp_not_connected",
      company_id: companyId,
      status: statusStore[companyId] || "unknown",
    })
  }

  const phone = String(req.body?.number || "").replace(/\D/g, "")
  if (!phone) {
    return res.status(400).json({ error: "missing_number" })
  }
  const digits = phone

  const mediaBase64 = String(req.body?.base64 || req.body?.media_base64 || "").trim()
  if (!mediaBase64) return res.status(400).json({ error: "base64_required" })

  const mimetype = String(req.body?.mime_type || req.body?.mimetype || "application/octet-stream").trim()
  const caption = String(req.body?.caption || req.body?.message || "").trim()
  const filename = String(req.body?.filename || "attachment").trim() || "attachment"

  let buffer = null
  try {
    buffer = Buffer.from(mediaBase64, "base64")
  } catch (_) {
    buffer = null
  }
  if (!buffer || buffer.length === 0) return res.status(400).json({ error: "invalid_base64" })

  const candidates = buildWhatsAppPhoneCandidates(digits)
  let preferredJidMedia = outboundJidFromRequestBody(req.body)

  const mm = mimetype.toLowerCase()
  const mediaType = mediaTypeFromMime(mm)
  let content = {}
  if (mediaType === "image") {
    content = { image: buffer, mimetype: mimetype, caption: caption || undefined }
  } else if (mediaType === "video") {
    content = { video: buffer, mimetype: mimetype, caption: caption || undefined }
  } else if (mediaType === "audio") {
    // Inbox recorder can emit audio/webm; keep exact MIME so WhatsApp receives proper container.
    content = {
      audio: buffer,
      mimetype: mimetype,
      ptt: mm.includes("ogg") || mm.includes("opus"),
    }
  } else {
    content = {
      document: buffer,
      mimetype: mimetype,
      fileName: filename,
      caption: caption || undefined,
    }
  }

  const trySendMediaToJid = async (jid) => {
    let sent = null
    try {
      sent = await sendMessageReliable(sock, jid, content, {}, digits)
    } catch (eSend) {
      if (mediaType === "audio" && mm.includes("webm")) {
        const fallbackDoc = {
          document: buffer,
          mimetype: mimetype || "audio/webm",
          fileName: filename || "voice.webm",
          caption: caption || undefined,
        }
        sent = await sendMessageReliable(sock, jid, fallbackDoc, {}, digits)
      } else {
        throw eSend
      }
    }
    if (sent) rememberSentProtoMessage(companyId, sent)
    return sent
  }

  let lastErr = null
  if (preferredJidMedia) {
    try {
      // IMPORTANT: do not convert @lid to MSISDN here.
      // For Baileys v7 LID chats, @lid is the real WhatsApp thread.
      // Text outbound already uses this path successfully; media must follow it too.
      const sent = await sendToPreferredOrMsisdnVariants(
        sock,
        preferredJidMedia,
        content,
        (jid, _c) => trySendMediaToJid(jid),
        digits,
        companyId
      )
      tlog(companyId, "[OUTBOUND USING remoteJid]", preferredJidMedia)
      const canonicalPhone = normalizeMxDigits(digits)
      const mediaUrl = await persistOutboundMediaBuffer(buffer, mimetype, companyId, "sendmedia")
      await saveMessageToDb(
        canonicalPhone,
        "out",
        caption || "[media]",
        "web",
        companyId,
        mediaType,
        mediaUrl,
        JSON.stringify({
          wa_key_id: sent?.key?.id || null,
          whatsapp_status: "sent",
          whatsapp_ack: 1,
          mime_type: mimetype,
        })
      )
      return res.json({
        sent: true,
        jid: preferredJidMedia,
        normalized_number: canonicalPhone,
        media_type: mediaType,
        key_id: sent?.key?.id || null,
        key: sent?.key || null,
      })
    } catch (ePref) {
      console.warn("[OUTBOUND remoteJid failed, falling back to MSISDN]", ePref?.message || ePref)
    }
  }

  for (const cand of candidates) {
    const jid = cand + "@s.whatsapp.net"
    try {
      const sent = await trySendMediaToJid(jid)
      const canonicalPhone = normalizeMxDigits(cand)
      const mediaUrl = await persistOutboundMediaBuffer(buffer, mimetype, companyId, "sendmedia")
      await saveMessageToDb(
        canonicalPhone,
        "out",
        caption || "[media]",
        "web",
        companyId,
        mediaType,
        mediaUrl,
        JSON.stringify({
          wa_key_id: sent?.key?.id || null,
          whatsapp_status: "sent",
          whatsapp_ack: 1,
          mime_type: mimetype,
        })
      )
      return res.json({
        sent: true,
        jid,
        normalized_number: cand,
        media_type: mediaType,
        key_id: sent?.key?.id || null,
        key: sent?.key || null,
      })
    } catch (e) {
      lastErr = e
    }
  }

  return res.status(502).json({
    error: "send_media_failed",
    detail: String(lastErr?.message || lastErr || "unknown"),
    tried: candidates,
  })
})

app.listen(3015, async () => {

  console.log("Tuexpo WhatsApp connector iniciado en puerto 3015")
  await loadBaileys()
  const panelSecret = panelBaileysSecretFromEnvFile()
  if (panelSecret !== BAILEYS_WEBHOOK_SECRET) {
    console.warn(
      "[ACK SECRET] mismatch connector vs panel .env:",
      JSON.stringify({
        connector_has_secret: !!BAILEYS_WEBHOOK_SECRET,
        panel_env_has_secret: !!panelSecret,
      })
    )
  } else {
    console.log("[ACK SECRET] connector/panel secret aligned")
  }

  if (CONNECTOR_AUTO_START) {
    for (const tenantId of CONNECTOR_BOOTSTRAP_TENANTS) {
      await startWhatsApp(tenantId, false)
    }
  } else {
    console.log(
      "[CONNECTOR] CONNECTOR_AUTO_START=0: sin arranque al boot; usar POST /connect, GET /qr o primer /send"
    )
  }
  console.log(
    CONNECTOR_ALLOW_ALL
      ? "[CONNECTOR] todos los company_id permitidos (CONNECTOR_TENANTS=* o sin definir)"
      : `[CONNECTOR] solo tenants: ${CONNECTOR_TENANTS.join(",")}`
  )

})
