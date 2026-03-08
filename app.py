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

INFLIGHT = {}
INFLIGHT_TTL = 300  # 5 min


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


def _cleanup_inflight():
    now = time.time()
    dead = [k for k, v in INFLIGHT.items() if now - v > INFLIGHT_TTL]
    for k in dead:
        INFLIGHT.pop(k, None)


def _inflight_start(key: str) -> bool:
    _cleanup_inflight()
    if key in INFLIGHT:
        return False
    INFLIGHT[key] = time.time()
    return True


def _inflight_end(key: str):
    INFLIGHT.pop(key, None)


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
    Prioriza participantAlt porque en grupos modernos participant puede venir como @lid.
    """
    data = payload.get("data") or payload
    key = data.get("key") or {}
    message = data.get("message") or {}

    remote_jid = _safe(key.get("remoteJid") or data.get("remoteJid"))
    participant = _safe(
        key.get("participantAlt")
        or data.get("participantAlt")
        or key.get("participant")
        or data.get("participant")
    )
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
    """
    Workaround: para grupos usar number=<groupJid>
    porque groupJid ha estado dando 400 en algunos endpoints/versiones.
    """
    url = f"{EVOLUTION_BASE_URL}/message/sendText/{EVOLUTION_INSTANCE}"
    payload = {"text": text}

    if group_jid:
        payload["number"] = group_jid
    elif number:
        payload["number"] = number

    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=60)
    print("sendText payload:", payload, flush=True)
    print("sendText resp:", r.status_code, r.text, flush=True)
    r.raise_for_status()
    return r.json()


def evolution_send_media(group_jid=None, number=None, media_url="", file_name="documento.pdf", caption=""):
    """
    Workaround: para grupos usar number=<groupJid>
    """
    url = f"{EVOLUTION_BASE_URL}/message/sendMedia/{EVOLUTION_INSTANCE}"
    payload = {
        "mediatype": "document",
        "media": media_url,
        "fileName": file_name,
        "caption": caption,
    }

    if group_jid:
        payload["number"] = group_jid
    elif number:
        payload["number"] = number

    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=120)
    print("sendMedia payload:", payload, flush=True)
    print("sendMedia resp:", r.status_code, r.text, flush=True)
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
    print("call_bot_internal status:", r.status_code, flush=True)
    print("call_bot_internal resp:", r.text, flush=True)
    r.raise_for_status()
    return r.json()


@app.get("/")
def health():
    return jsonify({"ok": True, "service": "wa-group-bridge"}), 200


@app.post("/evolution/webhook")
def evolution_webhook():
    job_key = None

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

        requester_number = _normalize_phone(
            participant.replace("@s.whatsapp.net", "").replace("@lid", "")
        )
        requester_label = (push_name or "Usuario").strip()

        # Evita volver a procesar la misma solicitud mientras sigue en curso
        job_key = hashlib.sha1(f"{remote_jid}|{requester_number}|{query}".encode("utf-8")).hexdigest()
        if not _inflight_start(job_key):
            return jsonify({"ok": True, "ignored": "already_processing"}), 200

        # Ack una sola vez por mensaje
        ack_key = f"ack:{msg_id}"
        if not _seen(ack_key):
            _mark_seen(ack_key)
            try:
                evolution_send_text(
                    group_jid=remote_jid,
                    text=f"⌛ Procesando solicitud de {requester_label}. Esto puede tardar unos minutos..."
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
            try:
                evolution_send_text(
                    group_jid=remote_jid,
                    text=f"❌ {requester_label} {err}"
                )
            except Exception as e:
                print("group error text send fail:", repr(e), flush=True)

            return jsonify({"ok": True, "delivered": False, "reason": "bot_failed"}), 200

        pdf_url = _safe(bot_resp.get("pdf_url"))
        file_name = _safe(bot_resp.get("filename")) or "documento.pdf"

        if not pdf_url:
            try:
                evolution_send_text(
                    group_jid=remote_jid,
                    text=f"❌ {requester_label} no se obtuvo enlace del PDF."
                )
            except Exception as e:
                print("group no-pdf text send fail:", repr(e), flush=True)

            return jsonify({"ok": True, "delivered": False, "reason": "no_pdf_url"}), 200

        if SEND_PDF_TO_GROUP:
            try:
                evolution_send_media(
                    group_jid=remote_jid,
                    media_url=pdf_url,
                    file_name=file_name,
                )
            except Exception as e:
                print("group media send fail:", repr(e), flush=True)
                try:
                    evolution_send_text(
                        group_jid=remote_jid,
                        text=f"⚠️ {requester_label} el documento se generó, pero no pude enviarlo al grupo."
                    )
                except Exception as e2:
                    print("group fallback text fail:", repr(e2), flush=True)

                return jsonify({"ok": True, "delivered": False, "reason": "group_media_failed"}), 200

        if SEND_PDF_TO_REQUESTER:
            try:
                caption = _safe(bot_resp.get("caption")) or "Aquí está tu documento."
                evolution_send_media(
                    number=requester_number,
                    media_url=pdf_url,
                    file_name=file_name,
                    caption=caption
                )
            except Exception as e:
                print("private media send fail:", repr(e), flush=True)

        return jsonify({
            "ok": True,
            "delivered": True,
            "group_jid": remote_jid,
            "requester_number": requester_number
        }), 200

    except Exception as e:
        print("evolution_webhook error:", repr(e), flush=True)
        traceback.print_exc()
        return jsonify({"ok": True, "handled": False, "error": str(e)}), 200

    finally:
        try:
            if job_key:
                _inflight_end(job_key)
        except Exception:
            pass


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
