import os
import re
import hashlib
import traceback
import requests
import base64
from flask import Flask, request, jsonify, Response
from redis import Redis
from rq import Queue

import json
from datetime import datetime
from zoneinfo import ZoneInfo

app = Flask(__name__)

EVOLUTION_BASE_URL = os.getenv("EVOLUTION_BASE_URL", "").rstrip("/")
EVOLUTION_API_KEY = os.getenv("EVOLUTION_API_KEY", "").strip()
EVOLUTION_INSTANCE = os.getenv("EVOLUTION_INSTANCE", "").strip()
EVOLUTION_WEBHOOK_SECRET = os.getenv("EVOLUTION_WEBHOOK_SECRET", "").strip()

GROUP_COMMAND = os.getenv("GROUP_COMMAND", "/csf").strip()

ALLOWED_GROUPS = {
    x.strip() for x in (os.getenv("ALLOWED_GROUPS", "") or "").split(",") if x.strip()
}

REDIS_URL = os.getenv("REDIS_URL", "").strip()
BOT_INTERNAL_URL = os.getenv("BOT_INTERNAL_URL", "").strip()
BOT_INTERNAL_TOKEN = os.getenv("BOT_INTERNAL_TOKEN", "").strip()

redis_conn = Redis.from_url(REDIS_URL)
task_queue = Queue("constancia_jobs", connection=redis_conn)

def _safe(v):
    return (v or "").strip() if isinstance(v, str) else (str(v).strip() if v is not None else "")

def _normalize_phone(v: str) -> str:
    return re.sub(r"\D+", "", v or "")

def _parse_command(text: str):
    t = _safe(text)
    if not t:
        return None

    raw = t.strip()

    # Si viene con /csf, lo quitamos pero no lo obligamos
    if GROUP_COMMAND:
        cmd = GROUP_COMMAND.strip()
        if raw.lower() == cmd.lower():
            return None
        if raw.lower().startswith((cmd + " ").lower()) or raw.lower().startswith((cmd + "\n").lower()):
            raw = raw[len(cmd):].strip()

    if not raw:
        return None

    upper_raw = raw.upper()
    flat = re.sub(r"[ \t]+", " ", upper_raw).strip()

    # patrones
    curp_pattern = r"[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d"
    rfc_pattern = r"[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}"
    idcif_pattern = r"\d{11}"

    # lugar de emisión: MUNICIPIO, ENTIDAD
    lugar_pattern = r"[A-ZÁÉÍÓÚÜÑ\s]+,\s*[A-ZÁÉÍÓÚÜÑ\s]+"

    # -------------------------------------------------
    # 1) CURP exacto
    # -------------------------------------------------
    if re.fullmatch(curp_pattern, flat):
        return flat

    # -------------------------------------------------
    # 2) RFC exacto
    # -------------------------------------------------
    if re.fullmatch(rfc_pattern, flat):
        return flat

    # -------------------------------------------------
    # 3) CURP + lugar (multilínea o corrido)
    # -------------------------------------------------
    if re.search(rf"\b{curp_pattern}\b", upper_raw) and re.search(lugar_pattern, upper_raw):
        return upper_raw

    # -------------------------------------------------
    # 4) RFC ONLY + lugar (multilínea o corrido)
    # -------------------------------------------------
    if re.search(rf"\b{rfc_pattern}\b", upper_raw) and not re.search(rf"\b{idcif_pattern}\b", upper_raw) and re.search(lugar_pattern, upper_raw):
        return upper_raw

    # -------------------------------------------------
    # 5) RFC + IDCIF (si además trae lugar, conservar TODO)
    # -------------------------------------------------
    rfc_match = re.search(rf"\b({rfc_pattern})\b", upper_raw)
    idcif_match = re.search(rf"\b({idcif_pattern})\b", upper_raw)

    if rfc_match and idcif_match:
        if re.search(lugar_pattern, upper_raw):
            return upper_raw
        rfc = rfc_match.group(1)
        idcif = idcif_match.group(1)
        return f"RFC: {rfc}\nIDCIF: {idcif}"

    # -------------------------------------------------
    # 6) RFC + IDCIF separados en líneas
    # -------------------------------------------------
    lines = [re.sub(r"\s+", " ", line).strip().upper() for line in raw.splitlines()]
    lines = [line for line in lines if line]

    if len(lines) >= 2:
        found_rfc = None
        found_idcif = None

        for line in lines:
            if not found_rfc and re.fullmatch(rfc_pattern, line):
                found_rfc = line
            if not found_idcif and re.fullmatch(idcif_pattern, line):
                found_idcif = line

        if found_rfc and found_idcif:
            if any("," in line for line in lines):
                return "\n".join(lines)
            return f"RFC: {found_rfc}\nIDCIF: {found_idcif}"

    return None

PANEL_TZ = os.getenv("PANEL_TZ", "America/Monterrey").strip()

def _panel_now():
    return datetime.now(ZoneInfo(PANEL_TZ))

def _panel_day_str():
    return _panel_now().strftime("%Y-%m-%d")

def _safe_int(v, default=0):
    try:
        return int(v or 0)
    except Exception:
        return default

def _extract_group_name(payload: dict) -> str:
    data = payload.get("data") or payload or {}
    msg = data.get("message") or {}

    candidates = {
        "data.groupName": _safe(data.get("groupName")),
        "data.subject": _safe(data.get("subject")),
        "data.groupSubject": _safe(data.get("groupSubject")),
        "payload.groupName": _safe(payload.get("groupName")),
        "payload.subject": _safe(payload.get("subject")),
        "msg.groupName": _safe(msg.get("groupName")) if isinstance(msg, dict) else "",
    }

    print("[GROUP NAME CANDIDATES]", candidates, flush=True)

    for _, val in candidates.items():
        if val:
            return val

    return ""

def _to_str(v):
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="ignore")
    return v or ""

def _panel_load_today_rows():
    day = _panel_day_str()
    prefix = f"panel_stats:{day}:group:"
    rows = []

    for key in redis_conn.scan_iter(match=prefix + "*"):
        key_s = _to_str(key)

        raw = redis_conn.hgetall(key) or {}
        raw = {_to_str(k): _to_str(v) for k, v in raw.items()}

        row = {
            "group_jid": raw.get("group_jid") or key_s.split(":group:", 1)[-1],
            "group_name": raw.get("group_name") or raw.get("group_jid") or key_s.split(":group:", 1)[-1],
            "total": _safe_int(raw.get("total")),
            "ok_rfc_idcif_qr": _safe_int(raw.get("ok_rfc_idcif_qr")),
            "ok_rfc_clon": _safe_int(raw.get("ok_rfc_clon")),
            "ok_rfc_idcif": _safe_int(raw.get("ok_rfc_idcif")),
            "ok_qr": _safe_int(raw.get("ok_qr")),
            "ok_curp": _safe_int(raw.get("ok_curp")),
            "ok_rfc_only": _safe_int(raw.get("ok_rfc_only")),
            "updated_at": raw.get("updated_at") or "",
            "day": raw.get("day") or day,
        }
        rows.append(row)

    rows.sort(key=lambda x: (-x["total"], x["group_name"], x["group_jid"]))
    return rows

def _panel_summary(rows):
    return {
        "day": _panel_day_str(),
        "groups": len(rows),
        "total": sum(r["total"] for r in rows),
        "ok_rfc_idcif_qr": sum(r["ok_rfc_idcif_qr"] for r in rows),
        "ok_rfc_clon": sum(r["ok_rfc_clon"] for r in rows),
        "ok_rfc_idcif": sum(r["ok_rfc_idcif"] for r in rows),
        "ok_qr": sum(r["ok_qr"] for r in rows),
        "ok_curp": sum(r["ok_curp"] for r in rows),
        "ok_rfc_only": sum(r["ok_rfc_only"] for r in rows),
    }

def _extract_evolution_message(payload: dict):
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
    group_name = _extract_group_name(payload)

    text = ""
    msg_type = "unknown"
    media_id = ""
    mime_type = ""
    
    if isinstance(message.get("conversation"), str):
        msg_type = "text"
        text = message.get("conversation", "").strip()

    elif isinstance(message.get("extendedTextMessage"), dict):
        msg_type = "text"
        text = _safe(message["extendedTextMessage"].get("text"))

    elif isinstance(message.get("imageMessage"), dict):
        img = message["imageMessage"]
        msg_type = "image"
        media_id = msg_id
        mime_type = _safe(img.get("mimetype"))
        text = _safe(img.get("caption"))

    elif isinstance(message.get("documentMessage"), dict):
        doc = message["documentMessage"]
        msg_type = "document"
        media_id = msg_id
        mime_type = _safe(doc.get("mimetype"))
        text = _safe(doc.get("caption") or doc.get("fileName"))

    elif isinstance(data.get("text"), str):
        msg_type = "text"
        text = _safe(data.get("text"))

    elif isinstance(payload.get("text"), str):
        msg_type = "text"
        text = _safe(payload.get("text"))

    return {
        "remote_jid": remote_jid,
        "participant": participant,
        "msg_id": msg_id,
        "from_me": from_me,
        "push_name": push_name,
        "group_name": group_name,
        "text": text,
        "msg_type": msg_type,
        "media_id": media_id,
        "mime_type": mime_type,
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
        payload["number"] = group_jid
    elif number:
        payload["number"] = number

    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=60)
    print("sendText payload:", payload, flush=True)
    print("sendText resp:", r.status_code, r.text, flush=True)
    r.raise_for_status()
    return r.json()

def _redis_setnx_ttl(key: str, ttl: int) -> bool:
    return bool(redis_conn.set(key, "1", ex=ttl, nx=True))

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

        print("[GROUP NAME RAW FROM MSG]", repr(msg.get("group_name")), flush=True)

        remote_jid = msg["remote_jid"]
        participant = msg["participant"]
        msg_id = msg["msg_id"]
        from_me = msg["from_me"]
        text = msg["text"]
        push_name = msg["push_name"] or "Usuario"
        group_name = msg.get("group_name") or remote_jid

        print("[GROUP NAME FINAL BEFORE JOB]", repr(group_name), flush=True)
        print("[REMOTE JID]", repr(remote_jid), flush=True)

        if not remote_jid.endswith("@g.us"):
            return jsonify({"ok": True, "ignored": "not_group"}), 200

        if ALLOWED_GROUPS and remote_jid not in ALLOWED_GROUPS:
            return jsonify({"ok": True, "ignored": "group_not_allowed"}), 200

        if from_me:
            return jsonify({"ok": True, "ignored": "from_me"}), 200

        if not participant:
            return jsonify({"ok": True, "ignored": "no_participant"}), 200

        dedupe_key = f"dedupe:{EVOLUTION_INSTANCE}:{msg_id}"
        if not _redis_setnx_ttl(dedupe_key, 600):
            return jsonify({"ok": True, "ignored": "duplicate"}), 200

        msg_type = msg["msg_type"]
        media_id = msg["media_id"]
        mime_type = msg["mime_type"]
        
        query = _parse_command(text)
        
        is_media_candidate = (
            msg_type in ("image", "document")
            and bool(media_id)
        )
        
        if not query and not is_media_candidate:
            return jsonify({"ok": True, "ignored": "no_command"}), 200

        requester_number = _normalize_phone(
            participant.replace("@s.whatsapp.net", "").replace("@lid", "")
        )
        requester_label = (push_name or "Usuario").strip()

        if query:
            normalized_query = re.sub(r"\s+", " ", query.strip().upper())
        else:
            normalized_query = f"MEDIA:{msg_type}:{media_id}"
        
        command_key = hashlib.sha1(
            f"{remote_jid}|{requester_number}|{normalized_query}".encode("utf-8")
        ).hexdigest()

        inflight_key = f"inflight:{EVOLUTION_INSTANCE}:{command_key}"
        if not _redis_setnx_ttl(inflight_key, 300):
            return jsonify({"ok": True, "ignored": "already_processing"}), 200

        ack_key = f"ack:{EVOLUTION_INSTANCE}:{msg_id}"
        if _redis_setnx_ttl(ack_key, 300):
            try:
                evolution_send_text(
                    group_jid=remote_jid,
                    text=f"⌛ Solicitud recibida de {requester_label}. Esto puede tardar unos minutos..."
                )
            except Exception as e:
                print("group ack error:", repr(e), flush=True)

        job_data = {
            "requester_number": requester_number,
            "requester_name": push_name,
            "requester_label": requester_label,
            "group_jid": remote_jid,
            "group_name": group_name,
            "original_text": text,
            "query": query,
            "msg_type": msg_type,
            "media_id": media_id,
            "mime_type": mime_type,
            "bot_internal_url": BOT_INTERNAL_URL,
            "bot_internal_token": BOT_INTERNAL_TOKEN,
        }

        task_queue.enqueue(
            "worker_jobs.process_group_request_job",
            job_data,
            job_timeout=900,
            result_ttl=3600,
            failure_ttl=86400,
        )

        return jsonify({
            "ok": True,
            "queued": True,
            "group_jid": remote_jid,
            "requester_number": requester_number
        }), 200

    except Exception as e:
        print("evolution_webhook error:", repr(e), flush=True)
        traceback.print_exc()
        return jsonify({"ok": True, "handled": False, "error": str(e)}), 200

@app.get("/panel/api/stats")
def panel_api_stats():
    rows = _panel_load_today_rows()
    summary = _panel_summary(rows)
    return jsonify({
        "ok": True,
        "summary": summary,
        "rows": rows,
    }), 200

@app.get("/panel")
def panel_stats():
    rows = _panel_load_today_rows()
    summary = _panel_summary(rows)

    def esc(v):
        if v is None:
            return ""
        return str(v)

    html = f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Panel puente WA</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {{
      --bg: #f3f6fb;
      --panel: #ffffff;
      --panel-2: #f8fafc;
      --text: #0f172a;
      --muted: #64748b;
      --line: #e2e8f0;
      --primary: #2563eb;
      --primary-2: #1d4ed8;
      --success: #16a34a;
      --shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
      --radius: 18px;
      --radius-sm: 14px;
    }}

    * {{
      box-sizing: border-box;
    }}

    html, body {{
      margin: 0;
      padding: 0;
      font-family: Inter, Arial, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(37,99,235,.08), transparent 30%),
        linear-gradient(180deg, #f8fbff 0%, var(--bg) 100%);
      color: var(--text);
    }}

    body {{
      padding: 16px;
    }}

    .wrap {{
      width: 100%;
      max-width: 1400px;
      margin: 0 auto;
    }}

    .hero {{
      background: linear-gradient(135deg, #0f172a 0%, #1e293b 55%, #2563eb 100%);
      color: white;
      border-radius: 24px;
      padding: clamp(18px, 3vw, 32px);
      box-shadow: var(--shadow);
      margin-bottom: 18px;
      position: relative;
      overflow: hidden;
    }}

    .hero::after {{
      content: "";
      position: absolute;
      right: -60px;
      top: -60px;
      width: 220px;
      height: 220px;
      border-radius: 50%;
      background: rgba(255,255,255,.08);
      filter: blur(4px);
    }}

    .hero h1 {{
      margin: 0 0 8px;
      font-size: clamp(1.5rem, 2.5vw, 2.2rem);
      line-height: 1.1;
      letter-spacing: -.02em;
      position: relative;
      z-index: 1;
    }}

    .hero .sub {{
      margin: 0;
      color: rgba(255,255,255,.82);
      font-size: clamp(.92rem, 1.4vw, 1rem);
      line-height: 1.5;
      position: relative;
      z-index: 1;
      max-width: 900px;
    }}

    .cards {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }}

    .card {{
      background: rgba(255,255,255,.92);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(226,232,240,.9);
      border-radius: var(--radius);
      padding: 18px;
      box-shadow: var(--shadow);
      min-height: 108px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }}

    .card .label {{
      font-size: .85rem;
      color: var(--muted);
      margin-bottom: 10px;
      font-weight: 600;
      letter-spacing: .01em;
    }}

    .card .value {{
      font-size: clamp(1.4rem, 3vw, 2rem);
      font-weight: 800;
      line-height: 1;
      color: var(--text);
    }}

    .section {{
      background: rgba(255,255,255,.88);
      border: 1px solid rgba(226,232,240,.9);
      border-radius: 22px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }}

    .section-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 18px 20px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
    }}

    .section-title {{
      margin: 0;
      font-size: 1rem;
      font-weight: 800;
      color: var(--text);
    }}

    .section-note {{
      color: var(--muted);
      font-size: .9rem;
      white-space: nowrap;
    }}

    .table-wrap {{
      width: 100%;
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
    }}

    table {{
      width: 100%;
      min-width: 1050px;
      border-collapse: separate;
      border-spacing: 0;
      background: white;
    }}

    thead th {{
      background: #0f172a;
      color: #fff;
      font-size: .82rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .03em;
      padding: 14px 12px;
      text-align: left;
      position: sticky;
      top: 0;
      z-index: 2;
      border-bottom: 1px solid #0b1220;
    }}

    tbody td {{
      padding: 14px 12px;
      border-bottom: 1px solid var(--line);
      font-size: .95rem;
      vertical-align: top;
      background: white;
    }}

    tbody tr:hover td {{
      background: #f8fbff;
    }}

    tbody tr:last-child td {{
      border-bottom: none;
    }}

    .right {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}

    .group-name {{
      font-weight: 700;
      color: var(--text);
      margin-bottom: 4px;
      word-break: break-word;
    }}

    .group-id {{
      color: var(--muted);
      font-size: .82rem;
      word-break: break-all;
      line-height: 1.35;
    }}

    .muted {{
      color: var(--muted);
      font-size: .84rem;
      line-height: 1.35;
    }}

    .badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 36px;
      padding: 6px 10px;
      border-radius: 999px;
      background: #eff6ff;
      color: #1d4ed8;
      font-weight: 700;
      font-size: .86rem;
    }}

    .total-badge {{
      background: #ecfdf5;
      color: #15803d;
    }}

    .empty {{
      padding: 28px 18px;
      text-align: center;
      color: var(--muted);
      font-size: .95rem;
      background: white;
    }}

    @media (max-width: 1200px) {{
      .cards {{
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }}
    }}

    @media (max-width: 720px) {{
      body {{
        padding: 12px;
      }}

      .hero {{
        border-radius: 20px;
      }}

      .cards {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }}

      .card {{
        min-height: 96px;
        padding: 16px;
      }}

      .section {{
        border-radius: 18px;
      }}

      .section-head {{
        flex-direction: column;
        align-items: flex-start;
      }}

      .table-wrap {{
        overflow: visible;
      }}

      table,
      thead,
      tbody,
      th,
      td,
      tr {{
        display: block;
        width: 100%;
      }}

      table {{
        min-width: 0;
        background: transparent;
      }}

      thead {{
        display: none;
      }}

      tbody {{
        display: grid;
        gap: 12px;
        padding: 12px;
        background: #f8fafc;
      }}

      tbody tr {{
        background: white;
        border: 1px solid var(--line);
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06);
      }}

      tbody td {{
        border-bottom: 1px solid #eef2f7;
        padding: 12px 14px;
        text-align: left !important;
        position: relative;
      }}

      tbody td:last-child {{
        border-bottom: none;
      }}

      tbody td::before {{
        content: attr(data-label);
        display: block;
        font-size: .76rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: .03em;
        color: var(--muted);
        margin-bottom: 6px;
      }}

      .group-name {{
        font-size: 1rem;
      }}

      .badge,
      .total-badge {{
        min-width: auto;
      }}
    }}

    @media (max-width: 420px) {{
      .cards {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
  <script>
    setTimeout(() => location.reload(), 30000);
  </script>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Panel puente WA</h1>
      <p class="sub">
        Corte diario automático: {summary["day"]} (reinicio lógico a las 00:00:00, {PANEL_TZ})
      </p>
    </section>

    <section class="cards">
      <div class="card">
        <div class="label">Total exitosos hoy</div>
        <div class="value">{summary["total"]}</div>
      </div>
      <div class="card">
        <div class="label">RFC IDCIF</div>
        <div class="value">{summary["ok_rfc_idcif"]}</div>
      </div>
      <div class="card">
        <div class="label">QR</div>
        <div class="value">{summary["ok_qr"]}</div>
      </div>
      <div class="card">
        <div class="label">CURP</div>
        <div class="value">{summary["ok_curp"]}</div>
      </div>
      <div class="card">
        <div class="label">RFC solo</div>
        <div class="value">{summary["ok_rfc_only"]}</div>
      </div>
      <div class="card">
        <div class="label">Grupos con actividad</div>
        <div class="value">{summary["groups"]}</div>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <h2 class="section-title">Actividad por grupo</h2>
        <div class="section-note">Actualización automática cada 30 segundos</div>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Grupo</th>
              <th class="right">Total</th>
              <th class="right">RFC_IDCIF</th>
              <th class="right">QR</th>
              <th class="right">CURP</th>
              <th class="right">RFC_solo</th>
              <th>Actualizado</th>
            </tr>
          </thead>
          <tbody>
    """

    if rows:
        for r in rows:
            html += f"""
            <tr>
              <td data-label="Grupo">
                <div class="group-name">{esc(r["group_name"])}</div>
                <div class="group-id">{esc(r["group_jid"])}</div>
              </td>
              <td data-label="Total" class="right"><span class="badge total-badge">{esc(r["total"])}</span></td>
              <td data-label="RFC_IDCIF" class="right"><span class="badge">{esc(r["ok_rfc_idcif"])}</span></td>
              <td data-label="QR" class="right"><span class="badge">{esc(r["ok_qr"])}</span></td>
              <td data-label="CURP" class="right"><span class="badge">{esc(r["ok_curp"])}</span></td>
              <td data-label="RFC_solo" class="right"><span class="badge">{esc(r["ok_rfc_only"])}</span></td>
              <td data-label="Actualizado"><span class="muted">{esc(r["updated_at"])}</span></td>
            </tr>
            """
    else:
        html += """
            <tr>
              <td colspan="9" class="empty">Sin actividad hoy.</td>
            </tr>
        """

    html += """
          </tbody>
        </table>
      </div>
    </section>
  </div>
</body>
</html>
    """
    return Response(html, mimetype="text/html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
