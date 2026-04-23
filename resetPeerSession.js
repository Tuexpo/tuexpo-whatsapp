"use strict"

const fs = require("fs")
const path = require("path")

/**
 * Borra archivos de sesión signal de Baileys cuyo nombre contiene el user part del JID (p. ej. MSISDN).
 * @param {string} basePath carpeta `sessions/<tenantId>`
 * @param {string} jid ej. `5517...@s.whatsapp.net` o `...@lid`
 */
function resetPeerSession(basePath, jid) {
  const dir = String(basePath || "").trim()
  if (!dir || !jid) return
  const prefix = String(jid).split("@")[0]
  if (!prefix) return

  let files
  try {
    files = fs.readdirSync(dir)
  } catch (e) {
    console.warn("[resetPeerSession] readdir failed", dir, e?.message || e)
    return
  }

  for (const f of files) {
    if (!f || !f.includes(prefix)) continue
    const abs = path.join(dir, f)
    try {
      fs.unlinkSync(abs)
    } catch (e) {
      console.warn("[resetPeerSession] unlink failed", abs, e?.message || e)
    }
  }
}

module.exports = { resetPeerSession }
