const express = require("express");
const axios = require("axios");
const cors = require("cors");
const morgan = require("morgan");
const dotenv = require("dotenv");
const FormData = require("form-data");

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: "30mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(morgan("dev"));

const PORT = process.env.PORT || 10000;

const {
  EVOLUTION_WEBHOOK_SECRET,
  EVOLUTION_BASE_URL,
  EVOLUTION_API_KEY,
  EVOLUTION_INSTANCE,
  BOT_INTERNAL_URL,
  BOT_INTERNAL_TOKEN,
  WHATSAPP_TOKEN,
  WHATSAPP_PHONE_NUMBER_ID,
  WHATSAPP_GRAPH_VERSION = "v23.0",
  GROUP_COMMAND = "/csf",
  SEND_PDF_TO_GROUP = "true",
  SEND_PDF_TO_REQUESTER = "true",
  SEND_PRIVATE_VIA = "cloud",
  ALLOWED_GROUPS = ""
} = process.env;

const allowedGroupsSet = new Set(
  (ALLOWED_GROUPS || "")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

const recentKeys = new Map();

function nowTs() {
  return Date.now();
}

function cleanupRecentKeys() {
  const limit = nowTs() - (10 * 60 * 1000);
  for (const [k, v] of recentKeys.entries()) {
    if (v < limit) recentKeys.delete(k);
  }
}

setInterval(cleanupRecentKeys, 60 * 1000);

function markSeen(key) {
  recentKeys.set(key, nowTs());
}

function wasSeen(key) {
  return recentKeys.has(key);
}

function safeStr(v) {
  return (v || "").toString().trim();
}

function boolEnv(v, fallback = false) {
  if (v == null) return fallback;
  return ["1", "true", "yes", "on"].includes(String(v).toLowerCase());
}

function parseCommand(text) {
  const t = safeStr(text);
  if (!t) return null;

  const lowered = t.toLowerCase();
  const cmd = GROUP_COMMAND.toLowerCase();

  if (!lowered.startsWith(cmd)) return null;

  const rest = t.slice(GROUP_COMMAND.length).trim();
  if (!rest) return null;

  return {
    command: GROUP_COMMAND,
    query: rest
  };
}

function normalizePhone(raw) {
  return String(raw || "").replace(/\D+/g, "");
}

function isAllowedGroup(groupJid) {
  if (!allowedGroupsSet.size) return true;
  return allowedGroupsSet.has(groupJid);
}

function getTextFromEvolutionPayload(body) {
  // Compatible con payloads comunes de Evolution
  const data = body?.data || body;
  const msg = data?.message || data?.messages?.[0] || body?.message || {};
  const messageType = msg?.messageType || msg?.type || "";

  let text = "";

  if (typeof msg?.text === "string") text = msg.text;
  if (!text && typeof msg?.conversation === "string") text = msg.conversation;
  if (!text && typeof msg?.body === "string") text = msg.body;
  if (!text && typeof msg?.message?.conversation === "string") text = msg.message.conversation;
  if (!text && typeof msg?.message?.extendedTextMessage?.text === "string") {
    text = msg.message.extendedTextMessage.text;
  }

  return {
    text: safeStr(text),
    messageType
  };
}

function getMessageMeta(body) {
  const data = body?.data || body;
  const key = data?.key || data?.message?.key || body?.key || {};
  const msg = data?.message || body?.message || {};
  const remoteJid =
    safeStr(key?.remoteJid) ||
    safeStr(data?.remoteJid) ||
    safeStr(msg?.key?.remoteJid);

  const participant =
    safeStr(key?.participant) ||
    safeStr(data?.participant) ||
    safeStr(msg?.key?.participant);

  const fromMe =
    Boolean(key?.fromMe) ||
    Boolean(data?.fromMe) ||
    Boolean(msg?.key?.fromMe);

  const pushName =
    safeStr(data?.pushName) ||
    safeStr(body?.pushName) ||
    safeStr(msg?.pushName);

  const id =
    safeStr(key?.id) ||
    safeStr(data?.id) ||
    safeStr(msg?.key?.id) ||
    `noid_${Date.now()}`;

  return {
    remoteJid,
    participant,
    fromMe,
    pushName,
    id
  };
}

async function evolutionSendText({ number, groupJid, text }) {
  const url = `${EVOLUTION_BASE_URL}/message/sendText/${EVOLUTION_INSTANCE}`;
  const payload = {};

  if (groupJid) payload.groupJid = groupJid;
  if (number) payload.number = number;

  payload.text = text;

  return axios.post(url, payload, {
    headers: {
      "Content-Type": "application/json",
      "apikey": EVOLUTION_API_KEY
    },
    timeout: 60000
  });
}

async function evolutionSendMediaByUrl({ number, groupJid, mediaUrl, fileName, caption }) {
  const url = `${EVOLUTION_BASE_URL}/message/sendMedia/${EVOLUTION_INSTANCE}`;
  const payload = {};

  if (groupJid) payload.groupJid = groupJid;
  if (number) payload.number = number;

  payload.mediatype = "document";
  payload.media = mediaUrl;
  payload.fileName = fileName || "documento.pdf";
  payload.caption = caption || "";

  return axios.post(url, payload, {
    headers: {
      "Content-Type": "application/json",
      "apikey": EVOLUTION_API_KEY
    },
    timeout: 120000
  });
}

async function uploadPdfToCloudApiFromUrl(pdfUrl) {
  const fileResp = await axios.get(pdfUrl, {
    responseType: "arraybuffer",
    timeout: 120000
  });

  const form = new FormData();
  form.append("messaging_product", "whatsapp");
  form.append("type", "application/pdf");
  form.append("file", Buffer.from(fileResp.data), {
    filename: "documento.pdf",
    contentType: "application/pdf"
  });

  const uploadUrl = `https://graph.facebook.com/${WHATSAPP_GRAPH_VERSION}/${WHATSAPP_PHONE_NUMBER_ID}/media`;

  const resp = await axios.post(uploadUrl, form, {
    headers: {
      ...form.getHeaders(),
      Authorization: `Bearer ${WHATSAPP_TOKEN}`
    },
    maxBodyLength: Infinity,
    maxContentLength: Infinity,
    timeout: 120000
  });

  return resp.data?.id;
}

async function sendCloudDocument({ to, mediaId, link, filename, caption }) {
  const url = `https://graph.facebook.com/${WHATSAPP_GRAPH_VERSION}/${WHATSAPP_PHONE_NUMBER_ID}/messages`;

  const documentObj = {
    filename: filename || "documento.pdf",
    caption: caption || ""
  };

  if (mediaId) {
    documentObj.id = mediaId;
  } else if (link) {
    documentObj.link = link;
  } else {
    throw new Error("sendCloudDocument requiere mediaId o link");
  }

  return axios.post(url, {
    messaging_product: "whatsapp",
    to,
    type: "document",
    document: documentObj
  }, {
    headers: {
      Authorization: `Bearer ${WHATSAPP_TOKEN}`,
      "Content-Type": "application/json"
    },
    timeout: 120000
  });
}

async function callBotGeneratePdf({ requesterNumber, requesterName, groupJid, text, query }) {
  const resp = await axios.post(BOT_INTERNAL_URL, {
    requester_number: requesterNumber,
    requester_name: requesterName,
    group_jid: groupJid,
    original_text: text,
    query
  }, {
    headers: {
      Authorization: `Bearer ${BOT_INTERNAL_TOKEN}`,
      "Content-Type": "application/json"
    },
    timeout: 240000
  });

  return resp.data;
}

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "wa-group-bridge",
    status: "up"
  });
});

app.post("/evolution/webhook", async (req, res) => {
  try {
    const secret = req.headers["x-bridge-secret"];
    if (EVOLUTION_WEBHOOK_SECRET && secret !== EVOLUTION_WEBHOOK_SECRET) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    const meta = getMessageMeta(req.body);
    const { text } = getTextFromEvolutionPayload(req.body);

    const remoteJid = meta.remoteJid;
    const participant = meta.participant;
    const fromMe = meta.fromMe;
    const messageId = meta.id;
    const pushName = meta.pushName || "Usuario";

    if (!remoteJid || !remoteJid.endsWith("@g.us")) {
      return res.json({ ok: true, ignored: "not_group" });
    }

    if (!isAllowedGroup(remoteJid)) {
      return res.json({ ok: true, ignored: "group_not_allowed" });
    }

    if (fromMe) {
      return res.json({ ok: true, ignored: "from_me" });
    }

    if (!participant) {
      return res.json({ ok: true, ignored: "no_participant" });
    }

    const dedupeKey = `${remoteJid}:${participant}:${messageId}`;
    if (wasSeen(dedupeKey)) {
      return res.json({ ok: true, ignored: "duplicate" });
    }
    markSeen(dedupeKey);

    const parsed = parseCommand(text);
    if (!parsed) {
      return res.json({ ok: true, ignored: "no_command" });
    }

    const requesterNumber = normalizePhone(participant.replace("@s.whatsapp.net", ""));
    const requesterTag = `@${requesterNumber}`;

    await evolutionSendText({
      groupJid: remoteJid,
      text: `⌛ Procesando solicitud de ${requesterTag}...`
    });

    const botData = await callBotGeneratePdf({
      requesterNumber,
      requesterName: pushName,
      groupJid: remoteJid,
      text,
      query: parsed.query
    });

    /**
     * Se espera que tu bot devuelva algo así:
     * {
     *   "ok": true,
     *   "pdf_url": "https://....pdf",
     *   "filename": "constancia.pdf",
     *   "caption": "Aquí está tu documento"
     * }
     *
     * o:
     * {
     *   "ok": true,
     *   "pdf_base64": "JVBERi0xLjcKJ....",
     *   "filename": "constancia.pdf",
     *   "caption": "Aquí está tu documento"
     * }
     */
    if (!botData?.ok) {
      const msg = botData?.error || "No fue posible generar el PDF.";
      await evolutionSendText({
        groupJid: remoteJid,
        text: `❌ ${requesterTag} ${msg}`
      });
      return res.json({ ok: true, delivered: false, reason: "bot_failed" });
    }

    let pdfUrl = botData?.pdf_url || "";
    const filename = botData?.filename || "documento.pdf";
    const caption = botData?.caption || "Aquí está tu documento.";

    if (!pdfUrl && botData?.pdf_base64) {
      // Si quieres soportar base64 real, súbelo primero a S3/R2/sitio temporal.
      // Aquí forzamos a usar URL por simplicidad.
      throw new Error("El bot devolvió pdf_base64; para este flujo usa pdf_url");
    }

    if (!pdfUrl) {
      throw new Error("Tu bot no devolvió pdf_url");
    }

    if (boolEnv(SEND_PDF_TO_GROUP, true)) {
      await evolutionSendMediaByUrl({
        groupJid: remoteJid,
        mediaUrl: pdfUrl,
        fileName: filename,
        caption: `📄 Documento solicitado por ${requesterTag}`
      });
    }

    if (boolEnv(SEND_PDF_TO_REQUESTER, true)) {
      if ((SEND_PRIVATE_VIA || "cloud").toLowerCase() === "evolution") {
        await evolutionSendMediaByUrl({
          number: requesterNumber,
          mediaUrl: pdfUrl,
          fileName: filename,
          caption
        });
      } else {
        const mediaId = await uploadPdfToCloudApiFromUrl(pdfUrl);
        await sendCloudDocument({
          to: requesterNumber,
          mediaId,
          filename,
          caption
        });
      }
    }

    return res.json({
      ok: true,
      delivered: true,
      requesterNumber,
      groupJid: remoteJid
    });
  } catch (err) {
    console.error("Webhook error:", err?.response?.data || err.message || err);
    return res.status(500).json({
      ok: false,
      error: err?.response?.data || err.message || "unknown_error"
    });
  }
});

app.post("/test/send-group", async (req, res) => {
  try {
    const { groupJid, text } = req.body;
    const r = await evolutionSendText({ groupJid, text });
    res.json({ ok: true, data: r.data });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.response?.data || err.message });
  }
});

app.post("/test/send-private", async (req, res) => {
  try {
    const { to, link, filename, caption } = req.body;
    const mediaId = await uploadPdfToCloudApiFromUrl(link);
    const r = await sendCloudDocument({
      to,
      mediaId,
      filename,
      caption
    });
    res.json({ ok: true, data: r.data });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.response?.data || err.message });
  }
});

app.listen(PORT, () => {
  console.log(`wa-group-bridge listening on :${PORT}`);
});
