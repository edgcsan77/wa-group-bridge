import os
import re
import time
import hashlib
import traceback
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

EVOLUTION_BASE_URL = os.getenv("EVOLUTION_BASE_URL", "").rstrip("/")
EVOLUTION_API_KEY = os.getenv("EVOLUTION_API_KEY", "").strip()
EVOLUTION_INSTANCE = os.getenv("EVOLUTION_INSTANCE", "").strip()
EVOLUTION_WEBHOOK_SECRET = os.getenv("EVOLUTION_WEBHOOK_SECRET", "").strip()

BOT_INTERNAL_URL = os.getenv("BOT_INTERNAL_URL", "").strip()
BOT_INTERNAL_TOKEN = os.getenv("BOT_INTERNAL_TOKEN", "").strip()

GROUP_COMMAND = os.getenv("GROUP_COMMAND", "/csf").strip()
SEND_PDF_TO_GROUP = os.getenv("SEND_PDF_TO_GROUP", "1") == "1"
SEND_PDF_TO_REQUESTER = os.getenv("SEND_PDF_TO_REQUESTER", "1") == "1"

ALLOWED_GROUPS = {
    x.strip() for x in (os.getenv("ALLOWED_GROUPS", "") or "").split(",") if x.strip()
}

SEEN = {}
SEEN_TTL = 600  # 10 min


def _cleanup_seen():
    now = time.time()
    dead = [k for k, v in SEEN.items() if now - v > SEEN_TTL]
    for k in dead:
        SEEN.pop(k, None)


def _seen(key: str) -> bool:
    _cleanup_seen()
    return key in SEEN


def _mark_seen(key: str):
    SEEN[key] = time.time()


def _safe(v):
    return (v or "").strip() if isinstance(v, str) else (str(v).strip() if v is not None else "")


def _normalize_phone(v: str) -> str:
    return re.sub(r"\D+", "", v or "")


def _parse_command(text: str):
    t = _safe(text)
    if not t:
        return None
    if not t.lower().startswith(GROUP_COMMAND.lower()):
        return None
    query = t[len(GROUP_COMMAND):].strip()
    if not query:
        return None
    return query


def _extract_evolution_message(payload: dict):
    """
    Flexible para payloads comunes de Evolution.
    """
    data = payload.get("data") or payload
    key = data.get("key") or {}
    message = data.get("message") or {}

    remote_jid = _safe(key.get("remoteJid") or data.get("remoteJid"))
    participant = _safe(key.get("participant") or data.get("participant"))
    msg_id = _safe(key.get("id") or data.get("id"))
    from_me = bool(key.get("fromMe") or data.get("fromMe"))
    push_name = _safe(data.get("pushName"))

    text = ""
    if isinstance(message.get("conversation"), str):
        text = message.get("conversation", "").strip()
    elif isinstance(message.get("extendedTextMessage"), dict):
        text = _safe(message["extendedTextMessage"].get("text"))
    elif isinstance(data.get("text"), str):
        text = _safe(data.get("text"))
    elif isinstance(payload.get("text"), str):
        text = _safe(payload.get("text"))

    return {
        "remote_jid": remote_jid,
        "participant": participant,
        "msg_id": msg_id,
        "from_me": from_me,
        "push_name": push_name,
        "text": text,
    }


def evolution_headers():
    return {
        "apikey": EVOLUTION_API_KEY,
        "Content-Type": "application/json",
    }


def evolution_send_text(group_jid=None, number=None, text=""):
    url = f"{EVOLUTION_BASE_URL}/message/sendText/{EVOLUTION_INSTANCE}"
    payload = {"text": text}
    if group_jid:
        payload["groupJid"] = group_jid
    if number:
        payload["number"] = number

    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=60)
    r.raise_for_status()
    return r.json()


def evolution_send_media(group_jid=None, number=None, media_url="", file_name="documento.pdf", caption=""):
    url = f"{EVOLUTION_BASE_URL}/message/sendMedia/{EVOLUTION_INSTANCE}"
    payload = {
        "mediatype": "document",
        "media": media_url,
        "fileName": file_name,
        "caption": caption,
    }
    if group_jid:
        payload["groupJid"] = group_jid
    if number:
        payload["number"] = number

    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=120)
    r.raise_for_status()
    return r.json()


def call_bot_internal(requester_number: str, requester_name: str, group_jid: str, original_text: str, query: str):
    headers = {
        "Authorization": f"Bearer {BOT_INTERNAL_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "requester_number": requester_number,
        "requester_name": requester_name,
        "group_jid": group_jid,
        "original_text": original_text,
        "query": query,
    }
    r = requests.post(BOT_INTERNAL_URL, json=payload, headers=headers, timeout=300)
    r.raise_for_status()
    return r.json()


@app.get("/")
def health():
    return jsonify({"ok": True, "service": "wa-group-bridge"}), 200


@app.post("/evolution/webhook")
def evolution_webhook():
    try:
        secret = request.headers.get("x-bridge-secret", "").strip()
        if EVOLUTION_WEBHOOK_SECRET and secret != EVOLUTION_WEBHOOK_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        payload = request.get_json(silent=True) or {}
        print("EVOLUTION WEBHOOK:", payload, flush=True)

        msg = _extract_evolution_message(payload)

        remote_jid = msg["remote_jid"]
        participant = msg["participant"]
        msg_id = msg["msg_id"]
        from_me = msg["from_me"]
        text = msg["text"]
        push_name = msg["push_name"] or "Usuario"

        if not remote_jid.endswith("@g.us"):
            return jsonify({"ok": True, "ignored": "not_group"}), 200

        if ALLOWED_GROUPS and remote_jid not in ALLOWED_GROUPS:
            return jsonify({"ok": True, "ignored": "group_not_allowed"}), 200

        if from_me:
            return jsonify({"ok": True, "ignored": "from_me"}), 200

        if not participant:
            return jsonify({"ok": True, "ignored": "no_participant"}), 200

        dedupe_key = hashlib.sha1(f"{remote_jid}|{participant}|{msg_id}|{text}".encode("utf-8")).hexdigest()
        if _seen(dedupe_key):
            return jsonify({"ok": True, "ignored": "duplicate"}), 200
        _mark_seen(dedupe_key)

        query = _parse_command(text)
        if not query:
            return jsonify({"ok": True, "ignored": "no_command"}), 200

        requester_number = _normalize_phone(participant.replace("@s.whatsapp.net", ""))
        requester_tag = f"@{requester_number}"

        try:
            evolution_send_text(
                group_jid=remote_jid,
                text=f"⌛ Procesando solicitud de {requester_tag}..."
            )
        except Exception as e:
            print("group ack error:", repr(e), flush=True)

        bot_resp = call_bot_internal(
            requester_number=requester_number,
            requester_name=push_name,
            group_jid=remote_jid,
            original_text=text,
            query=query,
        )

        if not bot_resp.get("ok"):
            err = bot_resp.get("error") or "No fue posible generar el documento."
            evolution_send_text(
                group_jid=remote_jid,
                text=f"❌ {requester_tag} {err}"
            )
            return jsonify({"ok": True, "delivered": False, "reason": "bot_failed"}), 200

        pdf_url = _safe(bot_resp.get("pdf_url"))
        file_name = _safe(bot_resp.get("filename")) or "documento.pdf"
        caption = _safe(bot_resp.get("caption")) or "Aquí está tu documento."

        if not pdf_url:
            evolution_send_text(
                group_jid=remote_jid,
                text=f"❌ {requester_tag} no se obtuvo enlace del PDF."
            )
            return jsonify({"ok": True, "delivered": False, "reason": "no_pdf_url"}), 200

        if SEND_PDF_TO_GROUP:
            evolution_send_media(
                group_jid=remote_jid,
                media_url=pdf_url,
                file_name=file_name,
                caption=f"📄 Documento solicitado por {requester_tag}"
            )

        if SEND_PDF_TO_REQUESTER:
            evolution_send_media(
                number=requester_number,
                media_url=pdf_url,
                file_name=file_name,
                caption=caption
            )

        return jsonify({
            "ok": True,
            "delivered": True,
            "group_jid": remote_jid,
            "requester_number": requester_number
        }), 200

    except Exception as e:
        print("evolution_webhook error:", repr(e), flush=True)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/test/send-group")
def test_send_group():
    data = request.get_json(silent=True) or {}
    group_jid = _safe(data.get("groupJid"))
    text = _safe(data.get("text")) or "Hola desde bridge"
    out = evolution_send_text(group_jid=group_jid, text=text)
    return jsonify({"ok": True, "data": out}), 200


@app.post("/test/send-private")
def test_send_private():
    data = request.get_json(silent=True) or {}
    number = _normalize_phone(data.get("number"))
    text = _safe(data.get("text")) or "Hola privado desde bridge"
    out = evolution_send_text(number=number, text=text)
    return jsonify({"ok": True, "data": out}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
