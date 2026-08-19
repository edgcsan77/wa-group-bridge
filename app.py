import os
import re
import hashlib
import traceback
import requests
import base64
from flask import Flask, request, jsonify, Response, redirect
from redis import Redis
from rq import Queue

import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

app = Flask(__name__)

EVOLUTION_BASE_URL = os.getenv("EVOLUTION_BASE_URL", "").rstrip("/")
EVOLUTION_API_KEY = os.getenv("EVOLUTION_API_KEY", "").strip()
EVOLUTION_INSTANCE = os.getenv("EVOLUTION_INSTANCE", "").strip()
EVOLUTION_WEBHOOK_SECRET = os.getenv("EVOLUTION_WEBHOOK_SECRET", "").strip()

# =========================
# BOT PUENTE IDCIF
# =========================
# Instancia nueva que SOLO recibe solicitudes IDCIF y habla con la proveedora.
IDCIF_RECEIVE_INSTANCE = os.getenv("IDCIF_RECEIVE_INSTANCE", "").strip()

# Instancia actual que debe responder al cliente con el PDF o avisos.
# En tu caso normalmente será grupo02.
RFC_RESPONSE_INSTANCE = os.getenv("RFC_RESPONSE_INSTANCE", EVOLUTION_INSTANCE).strip()

# Grupo donde está la proveedora + bot IDCIF nuevo.
IDCIF_PROVIDER_GROUP = os.getenv("IDCIF_PROVIDER_GROUP", "").strip()

# Tiempo que se guarda una solicitud pendiente esperando IDCIF.
IDCIF_PENDING_TTL_SEC = int(os.getenv("IDCIF_PENDING_TTL_SEC", "1800") or "1800")

IDCIF_PENDING_PREFIX = "idcif_pending"

GROUP_COMMAND = os.getenv("GROUP_COMMAND", "/csf").strip()

ALLOWED_GROUPS = {
    x.strip() for x in (os.getenv("ALLOWED_GROUPS", "") or "").split(",") if x.strip()
}

LOAD_DEFAULT_GROUPS = (os.getenv("LOAD_DEFAULT_GROUPS", "1").strip() == "1")

GROUP_NAME_MAP = {
    "120363407784035104@g.us": "VALLARTA",
    "120363424161924716@g.us": "DANIEL",
    "120363406744080404@g.us": "MELANI",
    "120363424434192043@g.us": "PEREYRA",
    "120363408404644680@g.us": "LILI CLON Y IDCIF",
    "120363422922827356@g.us": "DAY",
    "120363407149613052@g.us": "GESTORIA AC",
    "120363407782555379@g.us": "CARIDAD",
    "120363425761743324@g.us": "TRAMITES ANA",
    "120363425261888443@g.us": "SVS. DIGITALES",
    "120363423412816922@g.us": "LUIS ARMANDO",
    "120363424415085327@g.us": "PADRON RFC 2026",
    "120363404433497990@g.us": "ZENITRAN",
    "120363424298172274@g.us": "KEDETALLE",
    "120363405833386653@g.us": "KAREN",
    "120363405310833172@g.us": "EDUARDO",
    "120363424106109563@g.us": "TREVIÑO",
    "120363425336493958@g.us": "MORELOS",
    "120363427426623452@g.us": "NORKI LEAL",
    "120363407274319744@g.us": "NEGOCIO IMPERIO",
    "120363424987815870@g.us": "NEGOCIO AIRENET",
    "120363424921651306@g.us": "NEGOCIO CIBERT SAN LUIS",
    "120363424526047978@g.us": "NEGOCIO PAPELERIA MC",
    "120363421612572808@g.us": "NEGOCIO SERVITODO",
    "120363426454500572@g.us": "GESTORIA MARTINEZ",
    "120363406986631838@g.us": "NEGOCIO PAPELERIA BELLADIRA",
    "120363422728933907@g.us": "NEGOCIO PAPELERIA BATALLON",
    "120363423715238694@g.us": "ANA PINEDA",
    "120363406852977870@g.us": "NEGOCIO CIBERT ALMA",
    "120363421904462408@g.us": "VERACRUZ",
    "120363425323721713@g.us": "PRUEBA DOCIFY MX",
    "120363423742899713@g.us": "JOYA",
    "120363424117651122@g.us": "MAX ELIMINADO",
    "120363424256034923@g.us": "MAX 1",
    "120363408231220228@g.us": "MAX 2",
    "120363424846520578@g.us": "MAX 3",
    "120363404313875699@g.us": "MAX 4",
    "120363423948864434@g.us": "MAX 5",
    "120363425605880699@g.us": "MAX 6",
    "120363427046929010@g.us": "MAX 7",
    "120363426519871561@g.us": "MAX 8",
    "120363427164559374@g.us": "MAX 9",
    "120363407250162731@g.us": "MAX 10",
    "120363410011580719@g.us": "RODOLFO",
    "120363424196692440@g.us": "ISIDRO",
    "120363423807195838@g.us": "HIRO",
    "120363423486806524@g.us": "SERCOMEX",
    "120363409658465099@g.us": "DIEGO",
    "120363424548688064@g.us": "MARVIN",
    "120363426062563170@g.us": "SIN NOMBRE",
    "120363425116755394@g.us": "DOCIFY 1 - OZIEL",
    "120363425494922075@g.us": "DOCIFY 2 - EDUARDO",
    "120363425965991832@g.us": "DOCS",
    "120363424671454364@g.us": "DOCIFY 3 - DAYCI",
    "120363422954476813@g.us": "DOCIFY 4.1 - BRANDON",
    "120363422921737237@g.us": "DOCIFY 4.2 - BRANDON",
    "120363421638731486@g.us": "DOCIFY 4.3 - BRANDON",
    "120363401473888510@g.us": "DOCIFY 4.4 - BRANDON",
}

if not LOAD_DEFAULT_GROUPS:
    GROUP_NAME_MAP = {}

REDIS_URL = os.getenv("REDIS_URL", "").strip()
BOT_INTERNAL_URL = os.getenv("BOT_INTERNAL_URL", "").strip()
BOT_INTERNAL_TOKEN = os.getenv("BOT_INTERNAL_TOKEN", "").strip()

redis_conn = Redis.from_url(REDIS_URL)
task_queue = Queue("constancia_jobs", connection=redis_conn)

REQUEST_INFLIGHT_TTL_SEC = int(
    os.getenv("REQUEST_INFLIGHT_TTL_SEC", "1200") or "1200"
)
REQUEST_PROCESSED_TTL_SEC = int(
    os.getenv(
        "REQUEST_PROCESSED_TTL_SEC",
        "86400",
    ) or "86400"
)

IDCIF_REQUEST_LOCK_TTL_SEC = int(
    os.getenv("IDCIF_REQUEST_LOCK_TTL_SEC", "1800") or "1800"
)


def _idcif_request_lock_key(
    client_group: str,
    requester_number: str,
    term: str,
) -> str:
    raw = (
        f"{client_group}|"
        f"{requester_number}|"
        f"{_idcif_norm(term)}"
    )

    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"idcif_request_lock:{digest}"

# =========================
# CORTES / PRECIOS / CRON
# =========================
PANEL_CRON_SECRET = os.getenv("PANEL_CRON_SECRET", "").strip()

BENEFICIARIO_CORTE = "JUAN DE DIOS MESINO MANZANO"
CLABE_CORTE = "63818001018336874"
ENTIDAD_CORTE = "Nu México"

# Precio por defecto si el grupo no está en el mapa
DEFAULT_PRICES = {
    "clon": 0.00,
    "idcif": 0.00,
}

# PERSONALIZA AQUÍ LOS PRECIOS POR GRUPO
GROUP_PRICES = {
    "120363425323721713@g.us": {"clon": 0.00, "idcif": 0.00},  # PRUEBA
    "120363424298172274@g.us": {"clon": 6.00, "idcif": 6.00},  # KEDETALLE
    "120363405833386653@g.us": {"clon": 5.00, "idcif": 5.00},  # KAREN
    "120363423412816922@g.us": {"clon": 6.00, "idcif": 6.00},  # LUIS ARMANDO
    "120363425261888443@g.us": {"clon": 5.00, "idcif": 5.00},  # SVS. DIGITALES
    "120363424415085327@g.us": {"clon": 4.00, "idcif": 3.00},  # PADRON RFC 2026
    "120363425761743324@g.us": {"clon": 5.00, "idcif": 5.00},  # TRAMITES ANA
    "120363407782555379@g.us": {"clon": 5.00, "idcif": 5.00},  # CARIDAD
    "120363407149613052@g.us": {"clon": 5.00, "idcif": 5.00},  # GESTORIA AC
    "120363422922827356@g.us": {"clon": 5.00, "idcif": 5.00},  # DAY
    "120363408404644680@g.us": {"clon": 3.50, "idcif": 2.00},  # LILI
    "120363424434192043@g.us": {"clon": 5.00, "idcif": 4.00},  # PEREYRA
    "120363406744080404@g.us": {"clon": 3.50, "idcif": 1.50},  # MELANI
    "120363424161924716@g.us": {"clon": 5.00, "idcif": 5.00},  # DANIEL
    "120363407784035104@g.us": {"clon": 6.00, "idcif": 6.00},  # VALLARTA
    "120363404433497990@g.us": {"clon": 40.00, "idcif": 40.00},  # ZENITRAN
    "120363405310833172@g.us": {"clon": 5.00, "idcif": 3.00},  # EDUARDO
    "120363424106109563@g.us": {"clon": 5.00, "idcif": 4.00},  # TREVIÑO
    "120363425336493958@g.us": {"clon": 5.00, "idcif": 5.00},  # MORELOS
    "120363427426623452@g.us": {"clon": 8.00, "idcif": 8.00},  # NORKI LEAL

    # MAX (incluye todos los MAX 1–10)
    "120363424256034923@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 1
    "120363408231220228@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 2
    "120363424846520578@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 3
    "120363404313875699@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 4
    "120363423948864434@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 5
    "120363425605880699@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 6
    "120363427046929010@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 7
    "120363426519871561@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 8
    "120363427164559374@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 9
    "120363407250162731@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX 10
    "120363424117651122@g.us": {"clon": 4.00, "idcif": 2.00},  # MAX ELIMINADO

    "120363407274319744@g.us": {"clon": 30.00, "idcif": 30.00},  # NEGOCIO IMPERIO
    "120363424987815870@g.us": {"clon": 40.00, "idcif": 40.00},  # NEGOCIO AIRENET
    "120363424921651306@g.us": {"clon": 25.00, "idcif": 25.00},  # CIBERT SAN LUIS
    "120363424526047978@g.us": {"clon": 30.00, "idcif": 30.00},  # PAPELERIA MC
    "120363421612572808@g.us": {"clon": 25.00, "idcif": 25.00},  # SERVITODO
    "120363426454500572@g.us": {"clon": 5.00, "idcif": 5.00},  # GESTORIA MARTINEZ
    "120363406986631838@g.us": {"clon": 35.00, "idcif": 35.00},  # BELLADIRA
    "120363422728933907@g.us": {"clon": 35.00, "idcif": 35.00},  # BATALLON
    "120363423715238694@g.us": {"clon": 5.00, "idcif": 5.00},  # ANA PINEDA
    "120363406852977870@g.us": {"clon": 35.00, "idcif": 35.00},  # ALMA
    "120363421904462408@g.us": {"clon": 6.00, "idcif": 6.00},  # VERACRUZ
    "120363423742899713@g.us": {"clon": 4.00, "idcif": 2.00},  # JOYA
    "120363424196692440@g.us": {"clon": 7.00, "idcif": 5.00},  # ISIDRO
    "120363423807195838@g.us": {"clon": 6.00, "idcif": 6.00},  # HIRO
    "120363423486806524@g.us": {"clon": 40.00, "idcif": 40.00},  # SERCOMEX
    "120363409658465099@g.us": {"clon": 4.00, "idcif": 3.00},  # DIEGO
    "120363424548688064@g.us": {"clon": 8.00, "idcif": 8.00},  # MARVIN
    "120363410011580719@g.us": {"clon": 4.00, "idcif": 2.00},  # RODOLFO
    "120363425116755394@g.us": {"clon": 7.00, "idcif": 0.00},  # DOCIFY 1
    "120363425494922075@g.us": {"clon": 7.00, "idcif": 7.00},  # DOCIFY 2
    "120363424671454364@g.us": {"clon": 7.00, "idcif": 0.00},  # DOCIFY 3
    "120363422954476813@g.us": {"clon": 7.00, "idcif": 0.00},  # DOCIFY 4.1
    "120363422921737237@g.us": {"clon": 7.00, "idcif": 0.00},  # DOCIFY 4.2
    "120363421638731486@g.us": {"clon": 7.00, "idcif": 0.00},  # DOCIFY 4.3
    "120363401473888510@g.us": {"clon": 7.00, "idcif": 0.00},  # DOCIFY 4.4
}

if not LOAD_DEFAULT_GROUPS:
    GROUP_PRICES = {}

NO_CORTE_GROUPS = {
    "120363425323721713@g.us",  # PRUEBA DOCIFY MX
    "120363424415085327@g.us",  # PADRON RFC 2026
    "120363408404644680@g.us",  # LILI CLON Y IDCIF
    "120363424256034923@g.us",  # MAX 1
    "120363408231220228@g.us",  # MAX 2
    "120363424846520578@g.us",  # MAX 3
    "120363404313875699@g.us",  # MAX 4
    "120363423948864434@g.us",  # MAX 5
    "120363425605880699@g.us",  # MAX 6
    "120363427046929010@g.us",  # MAX 7
    "120363426519871561@g.us",  # MAX 8
    "120363427164559374@g.us",  # MAX 9
    "120363407250162731@g.us",  # MAX 10
    "120363424117651122@g.us",  # MAX ELIMINADO
    "120363409658465099@g.us",  # DIEGO
}

if not LOAD_DEFAULT_GROUPS:
    NO_CORTE_GROUPS = set()

GROUP_ALIASES_KEY = "group_aliases"
DYNAMIC_ALLOWED_GROUPS_KEY = "dynamic_allowed_groups"

ADMIN_NUMBERS = {
    "5218994588342",
    "5218991131427",
    "5217555592077",
    "5219618280050",
}

def get_dynamic_allowed_groups() -> set:
    try:
        vals = redis_conn.smembers(DYNAMIC_ALLOWED_GROUPS_KEY) or set()
        out = set()
        for v in vals:
            if isinstance(v, bytes):
                out.add(v.decode("utf-8", errors="ignore"))
            else:
                out.add(str(v))
        return out
    except Exception as e:
        print("get_dynamic_allowed_groups error:", repr(e), flush=True)
        return set()

def add_allowed_group(group_jid: str):
    try:
        if group_jid:
            redis_conn.sadd(DYNAMIC_ALLOWED_GROUPS_KEY, group_jid)
    except Exception as e:
        print("add_allowed_group error:", repr(e), flush=True)

def remove_allowed_group(group_jid: str):
    try:
        if group_jid:
            redis_conn.srem(DYNAMIC_ALLOWED_GROUPS_KEY, group_jid)
    except Exception as e:
        print("remove_allowed_group error:", repr(e), flush=True)

def is_group_allowed(group_jid: str) -> bool:
    try:
        if not group_jid:
            return False

        dynamic_groups = get_dynamic_allowed_groups()

        # Si no hay restricciones configuradas, permitir todo
        if not ALLOWED_GROUPS and not dynamic_groups:
            return True

        # Permitidos explícitos
        if group_jid in ALLOWED_GROUPS or group_jid in dynamic_groups:
            return True

        # Compatibilidad con grupos viejos ya conocidos por el sistema
        if is_legacy_known_group(group_jid):
            return True

        return False
    except Exception as e:
        print("is_group_allowed error:", repr(e), flush=True)
        return False

def is_legacy_known_group(group_jid: str) -> bool:
    try:
        if not group_jid:
            return False

        # 1) Si está en alias guardados, ya lo conocemos
        alias = get_group_alias(group_jid)
        if alias:
            return True

        # 2) Si está en bloqueados / no_corte, ya lo conocemos
        if is_group_blocked(group_jid):
            return True

        if group_jid in get_no_corte_groups():
            return True

        # 3) Si ya tiene precios configurados, ya lo conocemos
        raw_prices = redis_conn.hgetall(_group_prices_key(group_jid)) or {}
        if raw_prices:
            return True

        # No hacer SCAN de Redis dentro del webhook.
        # Los grupos deben reconocerse por:
        # alias, bloqueados, no-corte, precios o lista dinámica.
        return False
        
    except Exception as e:
        print("is_legacy_known_group error:", repr(e), flush=True)
        return False

def _parse_group_admin_command(text: str):
    raw = _safe(text)
    if not raw:
        return {"ok": False, "command": "", "args": ""}

    line = raw.strip()
    lower = line.lower()

    if lower == "/groupid":
        return {"ok": True, "command": "groupid", "args": ""}

    if lower.startswith("/addgroup"):
        args = line[len("/addgroup"):].strip()
        return {"ok": True, "command": "addgroup", "args": args}

    return {"ok": False, "command": "", "args": ""}

def get_group_alias(group_jid: str) -> str:
    try:
        if not group_jid:
            return ""
        val = redis_conn.hget(GROUP_ALIASES_KEY, group_jid)
        if isinstance(val, bytes):
            return val.decode("utf-8", errors="ignore").strip()
        return (val or "").strip()
    except Exception as e:
        print("get_group_alias error:", repr(e), flush=True)
        return ""

def set_group_alias(group_jid: str, alias: str):
    try:
        if not group_jid:
            return
        alias = _safe(alias)
        if alias:
            redis_conn.hset(GROUP_ALIASES_KEY, group_jid, alias)
        else:
            redis_conn.hdel(GROUP_ALIASES_KEY, group_jid)
    except Exception as e:
        print("set_group_alias error:", repr(e), flush=True)

def resolve_group_name(group_jid: str, fallback_name: str = "") -> str:
    alias = get_group_alias(group_jid)
    if alias:
        return alias

    static_name = GROUP_NAME_MAP.get(group_jid)
    if static_name:
        return static_name

    fallback_name = _safe(fallback_name)
    if fallback_name:
        return fallback_name

    return group_jid or ""

# =========================
# BLOQUEO DE GRUPOS
# =========================
BLOCKED_GROUPS_KEY = "blocked_groups"

NO_CORTE_GROUPS_KEY = "no_corte_groups"
GROUP_PRICES_KEY_PREFIX = "group_prices"

def get_no_corte_groups() -> set:
    try:
        vals = redis_conn.smembers(NO_CORTE_GROUPS_KEY) or set()
        out = set(NO_CORTE_GROUPS)  # conserva los hardcodeados actuales
        for v in vals:
            if isinstance(v, bytes):
                out.add(v.decode("utf-8", errors="ignore"))
            else:
                out.add(str(v))
        return out
    except Exception as e:
        print("get_no_corte_groups error:", repr(e), flush=True)
        return set(NO_CORTE_GROUPS)

def is_group_no_corte(group_jid: str) -> bool:
    try:
        if not group_jid:
            return False
        return group_jid in get_no_corte_groups()
    except Exception as e:
        print("is_group_no_corte error:", repr(e), flush=True)
        return group_jid in NO_CORTE_GROUPS

def disable_cut_group(group_jid: str):
    try:
        if group_jid:
            redis_conn.sadd(NO_CORTE_GROUPS_KEY, group_jid)
    except Exception as e:
        print("disable_cut_group error:", repr(e), flush=True)

def enable_cut_group(group_jid: str):
    try:
        if group_jid:
            redis_conn.srem(NO_CORTE_GROUPS_KEY, group_jid)
    except Exception as e:
        print("enable_cut_group error:", repr(e), flush=True)

def is_group_blocked(group_jid: str) -> bool:
    try:
        if not group_jid:
            return False
        return bool(redis_conn.sismember(BLOCKED_GROUPS_KEY, group_jid))
    except Exception as e:
        print("is_group_blocked error:", repr(e), flush=True)
        return False

def block_group(group_jid: str):
    try:
        if group_jid:
            redis_conn.sadd(BLOCKED_GROUPS_KEY, group_jid)
    except Exception as e:
        print("block_group error:", repr(e), flush=True)

def unblock_group(group_jid: str):
    try:
        if group_jid:
            redis_conn.srem(BLOCKED_GROUPS_KEY, group_jid)
    except Exception as e:
        print("unblock_group error:", repr(e), flush=True)

def get_blocked_groups() -> set:
    try:
        vals = redis_conn.smembers(BLOCKED_GROUPS_KEY) or set()
        out = set()
        for v in vals:
            if isinstance(v, bytes):
                out.add(v.decode("utf-8", errors="ignore"))
            else:
                out.add(str(v))
        return out
    except Exception as e:
        print("get_blocked_groups error:", repr(e), flush=True)
        return set()

def _safe(v):
    return (v or "").strip() if isinstance(v, str) else (str(v).strip() if v is not None else "")

def _payload_instance(payload: dict) -> str:
    inst = _safe(payload.get("instance"))
    if inst:
        return inst

    data = payload.get("data") or {}
    inst = _safe(data.get("instance"))
    if inst:
        return inst

    return EVOLUTION_INSTANCE

def _normalize_phone(v: str) -> str:
    return re.sub(r"\D+", "", v or "")

def _clean_spaces(text: str) -> str:
    text = (text or "").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()

def _normalize_upper(text: str) -> str:
    return _clean_spaces((text or "").upper())

def _dato_solicitado_text(value: str) -> str:
    value = _safe(value).strip().upper()

    if not value:
        return "NO DISPONIBLE"

    return value

def _format_input_error():
    return ""

def _looks_like_curp(value: str) -> bool:
    v = re.sub(r"\s+", "", _normalize_upper(value))
    if not v:
        return False

    if len(v) < 16 or len(v) > 20:
        return False

    if not re.match(r"^[A-Z0-9]{4}", v):
        return False

    if len(v) >= 10:
        fecha = v[4:10]
        digits_like = sum(ch.isdigit() or ch in "OISBZ" for ch in fecha)
        if digits_like < 5:
            return False

    return bool(re.fullmatch(r"[A-Z0-9]{16,20}", v))

def _looks_like_rfc(value: str) -> bool:
    v = re.sub(r"\s+", "", _normalize_upper(value))
    if not v:
        return False

    if len(v) < 11 or len(v) > 14:
        return False

    # inicio tipo letras
    if not re.match(r"^[A-ZÑ&0-9]{3,4}", v):
        return False

    # bloque fecha "parecido" a 6 caracteres casi numéricos
    body = v[3:] if len(v) == 12 else v[4:]
    if len(body) < 6:
        return False

    fecha = body[:6]
    digits_like = sum(ch.isdigit() or ch in "OISBZ" for ch in fecha)
    if digits_like < 5:
        return False

    return True

def _looks_like_idcif(value: str) -> bool:
    v = re.sub(r"\s+", "", _normalize_upper(value))
    if not v:
        return False

    if len(v) < 8 or len(v) > 14:
        return False

    digits = sum(ch.isdigit() for ch in v)

    # si casi todo son dígitos, tratarlo como IDCIF sospechoso
    if digits >= max(6, len(v) - 2):
        return True

    return False

def _is_text_candidate(text: str) -> bool:
    raw = _safe(text)
    if not raw:
        return False

    normalized = _normalize_upper(raw)
    normalized_clean = re.sub(r"\b(RFC|IDCIF|CURP)\s*:?\s+", "", normalized, flags=re.IGNORECASE)

    # Primero separar líneas reales
    base_lines = [re.sub(r"\s+", " ", line).strip().upper() for line in raw.splitlines()]
    base_lines = [line for line in base_lines if line]

    # Luego expandir etiquetas internas en una misma línea
    expanded_lines = []
    for line in base_lines or [normalized]:
        expanded = _expand_labeled_segments(line)
        if expanded:
            expanded_lines.extend(expanded)
        else:
            expanded_lines.append(line)

    raw_lines = [line for line in expanded_lines if line]
    lines = [_strip_known_prefix(line) for line in raw_lines]
    lines = [line for line in lines if line]

    curp_pattern = r"^[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d$"
    rfc_pattern = r"^[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}$"
    idcif_pattern = r"^\d{11}$"

    # Exactos válidos
    if re.fullmatch(curp_pattern, normalized_clean):
        return True
    if re.fullmatch(rfc_pattern, normalized_clean):
        return True
    if re.fullmatch(idcif_pattern, normalized_clean):
        return True

    # Caso fuerte: dos tokens en una misma línea, tipo RFC + algo parecido a IDCIF
    compact_tokens = re.findall(r"[A-Z0-9Ñ&]+", normalized_clean)
    if len(compact_tokens) >= 2:
        first = compact_tokens[0]
        second = compact_tokens[1]
    
        first_is_rfc = bool(
            _looks_like_rfc(first)
            or re.fullmatch(
                rfc_pattern,
                first
            )
        )
    
        second_is_rfc = bool(
            _looks_like_rfc(second)
            or re.fullmatch(
                rfc_pattern,
                second
            )
        )
    
        first_is_idcif = bool(
            _looks_like_idcif(first)
            or re.fullmatch(
                idcif_pattern,
                first
            )
        )
    
        second_is_idcif = bool(
            _looks_like_idcif(second)
            or re.fullmatch(
                idcif_pattern,
                second
            )
        )
    
        if (
            first_is_rfc
            and second_is_idcif
        ):
            return True
    
        if (
            first_is_idcif
            and second_is_rfc
        ):
            return True
    
        if (
            _looks_like_curp(first)
            or re.fullmatch(
                curp_pattern,
                first
            )
        ):
            return True

    # Por línea / pseudo-líneas
    strong_hits = 0
    for line in lines:
        if re.fullmatch(curp_pattern, line):
            strong_hits += 1
            continue
        if re.fullmatch(rfc_pattern, line):
            strong_hits += 1
            continue
        if re.fullmatch(idcif_pattern, line):
            strong_hits += 1
            continue

        if _looks_like_rfc(line):
            strong_hits += 1
            continue
        if _looks_like_curp(line):
            strong_hits += 1
            continue
        if _looks_like_idcif(line):
            strong_hits += 1
            continue

    if strong_hits >= 1:
        return True

    # Texto corrido
    if re.search(r"[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}", normalized_clean):
        return True

    if re.search(r"[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d", normalized_clean):
        return True

    if re.search(r"\d{8,14}", normalized_clean):
        return True

    return False

def _looks_like_lugar(value: str) -> bool:
    v = _normalize_upper(value)
    if not v:
        return False

    # Solo considerar lugar si trae coma
    if "," not in v:
        return False

    # Debe verse como dos partes de solo letras/espacios
    parts = [p.strip() for p in v.split(",", 1)]
    if len(parts) != 2:
        return False

    left, right = parts
    if not left or not right:
        return False

    if not re.fullmatch(r"[A-ZÁÉÍÓÚÜÑ\s]+", left):
        return False
    if not re.fullmatch(r"[A-ZÁÉÍÓÚÜÑ\s]+", right):
        return False

    return True

def _strip_known_prefix(line: str) -> str:
    s = _normalize_upper(line)

    # Quita prefijos al inicio con o sin :
    s = re.sub(r"^(RFC|IDCIF|CURP)\s*:?\s+", "", s, flags=re.IGNORECASE).strip()

    return s

def _expand_labeled_segments(text: str):
    s = _normalize_upper(text)
    if not s:
        return []

    # Inserta salto antes de cada etiqueta conocida, excepto si ya está al inicio
    s = re.sub(r"\s+(RFC|IDCIF|CURP)\s*:?\s*", r"\n\1: ", s, flags=re.IGNORECASE)

    parts = [p.strip() for p in s.splitlines() if p.strip()]
    return parts

def _extract_embedded_tokens(text: str):
    s = _normalize_upper(text)
    s = re.sub(r"\b(RFC|IDCIF|CURP)\s*:?\s+", "", s, flags=re.IGNORECASE)

    curp_pattern = r"\b[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d\b"
    rfc_pattern = r"\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b"
    idcif_pattern = r"\b\d{11}\b"
    lugar_pattern = r"\b[A-ZÁÉÍÓÚÜÑ\s]+,\s*[A-ZÁÉÍÓÚÜÑ\s]+\b"

    return {
        "curp": re.findall(curp_pattern, s),
        "rfc": re.findall(rfc_pattern, s),
        "idcif": re.findall(idcif_pattern, s),
        "lugar": re.findall(lugar_pattern, s),
    }

def _parse_command(text: str):
    t = _safe(text)
    if not t:
        return {
            "ok": False,
            "type": "empty",
            "query": None,
            "error": _format_input_error()
        }

    raw = t.strip()

    # Si viene con /csf, lo quitamos pero no lo obligamos
    if GROUP_COMMAND:
        cmd = GROUP_COMMAND.strip()
        if raw.lower() == cmd.lower():
            return {
                "ok": False,
                "type": "empty",
                "query": None,
                "error": _format_input_error()
            }

        if raw.lower().startswith((cmd + " ").lower()) or raw.lower().startswith((cmd + "\n").lower()):
            raw = raw[len(cmd):].strip()

    if not raw:
        return {
            "ok": False,
            "type": "empty",
            "query": None,
            "error": _format_input_error()
        }

    upper_raw = _normalize_upper(raw)
    upper_raw_clean = re.sub(r"\b(RFC|IDCIF|CURP)\s*:?\s+", "", upper_raw, flags=re.IGNORECASE)
    flat = re.sub(r"[ \t]+", " ", upper_raw_clean).strip()

    # patrones
    curp_pattern = r"[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d"
    rfc_pattern = r"[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}"
    idcif_pattern = r"\d{11}"
    lugar_pattern = r"[A-ZÁÉÍÓÚÜÑ\s]+,\s*[A-ZÁÉÍÓÚÜÑ\s]+"

    m_curp_plus_any = re.match(
        rf"^\s*({curp_pattern})(?:\s+|$)(.*)$",
        upper_raw_clean,
        flags=re.I | re.S
    )

    if m_curp_plus_any:
        curp = m_curp_plus_any.group(1).strip().upper()
        rest = (m_curp_plus_any.group(2) or "").strip()

        if rest:
            query_final = f"{curp} {rest}".strip()
        else:
            query_final = curp

        return {
            "ok": True,
            "type": "curp",
            "query": query_final,
            "error": None
        }

    raw_lines = [re.sub(r"\s+", " ", line).strip().upper() for line in raw.splitlines()]
    raw_lines = [line for line in raw_lines if line]
    
    # Expandir etiquetas internas en una misma línea
    expanded_lines = []
    for line in raw_lines or [upper_raw]:
        expanded = _expand_labeled_segments(line)
        if expanded:
            expanded_lines.extend(expanded)
        else:
            expanded_lines.append(line)
    
    raw_lines = [line for line in expanded_lines if line]
    
    lines = [_strip_known_prefix(line) for line in raw_lines]
    lines = [line for line in lines if line]

   # -------------------------------------------------
    # DETECTAR LOTE RFC + IDCIF ANTES DEL PAR INDIVIDUAL
    #
    # Admite:
    #   RFC IDCIF
    #   IDCIF RFC
    #   RFC\nIDCIF
    #   IDCIF\nRFC
    #   y combinaciones mezcladas.
    # -------------------------------------------------
    batch_token_pattern = re.compile(
        rf"(?P<RFC>\b{rfc_pattern}\b)"
        rf"|(?P<IDCIF>\b{idcif_pattern}\b)",
        flags=re.IGNORECASE,
    )
    
    batch_tokens = []
    
    for match in batch_token_pattern.finditer(
        upper_raw_clean
    ):
        if match.group("RFC"):
            batch_tokens.append(
                (
                    "RFC",
                    match.group("RFC").strip().upper(),
                )
            )
            continue
    
        if match.group("IDCIF"):
            batch_tokens.append(
                (
                    "IDCIF",
                    match.group("IDCIF").strip(),
                )
            )
    
    batch_pairs = []
    batch_seen = set()

    # =========================================================
    # RFC + IDCIF BATCH SEGURO
    #
    # Se aceptan AMBOS órdenes por pareja:
    #
    #   RFC IDCIF
    #   IDCIF RFC
    #
    # También pueden venir mezclados:
    #
    #   RFC IDCIF
    #   IDCIF RFC
    #   RFC IDCIF
    #
    # SEGURIDAD:
    # Cada pareja se toma SIEMPRE de 2 en 2.
    # Nunca se desliza un token buscando pareja más adelante.
    #
    # Si hay 3 o más tokens y la cantidad es impar,
    # se rechaza TODO el lote para impedir corrimientos.
    # =========================================================
    batch_structure_valid = True
    batch_structure_error = ""

    # 1 token = RFC o IDCIF individual.
    # 2 tokens = pareja individual.
    # 3+ tokens = intento de lote múltiple.
    batch_is_multi_candidate = len(batch_tokens) >= 3

    if batch_is_multi_candidate:
        if len(batch_tokens) % 2 != 0:
            batch_structure_valid = False
            batch_structure_error = "ODD_TOKEN_COUNT"

        else:
            for idx in range(0, len(batch_tokens), 2):
                type_1, value_1 = batch_tokens[idx]
                type_2, value_2 = batch_tokens[idx + 1]

                # RFC + IDCIF
                if type_1 == "RFC" and type_2 == "IDCIF":
                    pair = (
                        value_1,
                        value_2,
                    )

                # IDCIF + RFC
                elif type_1 == "IDCIF" and type_2 == "RFC":
                    pair = (
                        value_2,
                        value_1,
                    )

                # RFC + RFC o IDCIF + IDCIF:
                # pareja inválida; NO intentar correr tokens.
                else:
                    batch_structure_valid = False
                    batch_structure_error = (
                        f"INVALID_PAIR_AT_{idx}_{idx + 1}:"
                        f"{type_1}_{type_2}"
                    )
                    break

                if pair not in batch_seen:
                    batch_seen.add(pair)
                    batch_pairs.append(pair)

    if batch_is_multi_candidate and not batch_structure_valid:
        print(
            "[RFC_IDCIF_BATCH_REJECTED]",
            {
                "reason": batch_structure_error,
                "tokens": batch_tokens,
                "raw": repr(raw),
            },
            flush=True,
        )

        return {
            "ok": False,
            "type": "invalid_rfc_idcif_batch",
            "query": None,
            "error": (
                "⚠️ La lista RFC + IDCIF tiene un formato incompleto "
                "o desalineado.\n\n"
                f"*Dato solicitado:*\n{raw}"
            ),
        }
    
    print(
        "[RFC_IDCIF_BATCH_PARSE]",
        {
            "raw": repr(raw),
            "upper_raw_clean": repr(upper_raw_clean),
            "tokens": batch_tokens,
            "pairs": batch_pairs,
            "pair_count": len(batch_pairs),
        },
        flush=True,
    )
    
    if len(batch_pairs) >= 2:
        batch_query_lines = []
    
        for batch_rfc, batch_idcif in batch_pairs:
            batch_query_lines.append(batch_rfc)
            batch_query_lines.append(batch_idcif)
    
        return {
            "ok": True,
            "type": "rfc_idcif_batch",
            "query": "\n".join(batch_query_lines),
            "error": None,
        }

    # -------------------------------------------------
    # 1) VÁLIDOS EXACTOS
    # -------------------------------------------------
    if re.fullmatch(curp_pattern, flat):
        return {
            "ok": True,
            "type": "curp",
            "query": flat,
            "error": None
        }

    if re.fullmatch(rfc_pattern, flat):
        return {
            "ok": True,
            "type": "rfc",
            "query": flat,
            "error": None
        }

    if re.fullmatch(idcif_pattern, flat):
        return {
            "ok": True,
            "type": "idcif",
            "query": flat,
            "error": None
        }

    # CURP + lugar
    if re.search(rf"\b{curp_pattern}\b", upper_raw_clean) and re.search(lugar_pattern, upper_raw_clean):
        return {
            "ok": True,
            "type": "curp_lugar",
            "query": upper_raw,
            "error": None
        }

    # RFC + lugar
    if re.search(rf"\b{rfc_pattern}\b", upper_raw_clean) and not re.search(rf"\b{idcif_pattern}\b", upper_raw_clean) and re.search(lugar_pattern, upper_raw_clean):
        return {
            "ok": True,
            "type": "rfc_lugar",
            "query": upper_raw,
            "error": None
        }

    # RFC + IDCIF
    rfc_match = re.search(rf"\b({rfc_pattern})\b", upper_raw_clean)
    idcif_match = re.search(rf"\b({idcif_pattern})\b", upper_raw_clean)

    if rfc_match and idcif_match:
        if re.search(lugar_pattern, upper_raw_clean):
            return {
                "ok": True,
                "type": "rfc_idcif_lugar",
                "query": upper_raw,
                "error": None
            }

        rfc = rfc_match.group(1)
        idcif = idcif_match.group(1)
        return {
            "ok": True,
            "type": "rfc_idcif",
            "query": f"RFC: {rfc}\nIDCIF: {idcif}",
            "error": None
        }

    # RFC + IDCIF separados en líneas
    if len(lines) >= 2:
        found_rfc = None
        found_idcif = None
        found_curp = None
        found_lugar = None

        for line in lines:
            if not found_rfc and re.fullmatch(rfc_pattern, line):
                found_rfc = line
            if not found_idcif and re.fullmatch(idcif_pattern, line):
                found_idcif = line
            if not found_curp and re.fullmatch(curp_pattern, line):
                found_curp = line
            if not found_lugar and re.fullmatch(lugar_pattern, line):
                found_lugar = line

        if found_curp and found_lugar and not found_rfc and not found_idcif:
            return {
                "ok": True,
                "type": "curp_lugar",
                "query": f"{found_curp}\n{found_lugar}",
                "error": None
            }

        if found_rfc and found_lugar and not found_curp and not found_idcif:
            return {
                "ok": True,
                "type": "rfc_lugar",
                "query": f"{found_rfc}\n{found_lugar}",
                "error": None
            }

        if found_rfc and found_idcif and not found_curp and not found_lugar:
            return {
                "ok": True,
                "type": "rfc_idcif",
                "query": f"RFC: {found_rfc}\nIDCIF: {found_idcif}",
                "error": None
            }

        if found_rfc and found_idcif and found_lugar and not found_curp:
            return {
                "ok": True,
                "type": "rfc_idcif_lugar",
                "query": f"RFC: {found_rfc}\nIDCIF: {found_idcif}\n{found_lugar}",
                "error": None
            }

    # -------------------------------------------------
    # 2-A) CASO CONTEXTUAL: RFC válido + segunda línea inválida
    # -------------------------------------------------
    if len(lines) == 2:
        first_line = lines[0]
        second_line = lines[1]

        if re.fullmatch(rfc_pattern, first_line):
            # Si la segunda no es lugar y además parece un IDCIF o algo cercano,
            # tratarla como error de IDCIF
            if not re.fullmatch(lugar_pattern, second_line):
                if _looks_like_idcif(second_line) and not re.fullmatch(idcif_pattern, second_line):
                    return {
                        "ok": False,
                        "type": "invalid_idcif",
                        "query": None,
                        "error": (
                            "⚠️ IDCIF inválido.\n\n"
                            f"*Dato solicitado:* {_dato_solicitado_text(second_line)}\n\n"
                            "Debe contener únicamente 11 dígitos."
                        )
                    }

    # -------------------------------------------------
    # 2-A PRE) TEXTO CORRIDO CON PREFIJOS EXPLÍCITOS
    # -------------------------------------------------
    m_curp = re.search(r"\bCURP\s*:?\s*([A-Z0-9]{10,25})\b", upper_raw, flags=re.IGNORECASE)
    if m_curp:
        curp_candidate = _normalize_upper(m_curp.group(1))
        if not re.fullmatch(curp_pattern, curp_candidate):
            return {
                "ok": False,
                "type": "invalid_curp",
                "query": None,
                "error": (
                    "⚠️ CURP inválida.\n\n"
                    f"*Dato solicitado:* {_dato_solicitado_text(curp_candidate)}\n\n"
                    "Debe tener 18 caracteres con formato correcto."
                )
            }

    m_rfc = re.search(r"\bRFC\s*:?\s*([A-ZÑ&0-9]{8,20})\b", upper_raw, flags=re.IGNORECASE)
    if m_rfc:
        rfc_candidate = _normalize_upper(m_rfc.group(1))
        if not re.fullmatch(rfc_pattern, rfc_candidate):
            return {
                "ok": False,
                "type": "invalid_rfc",
                "query": None,
                "error": (
                    "⚠️ RFC inválido.\n\n"
                    f"*Dato solicitado:* {_dato_solicitado_text(rfc_candidate)}\n\n"
                    "Persona física: 13 caracteres.\n"
                    "Persona moral: 12 caracteres."
                )
            }

    m_idcif = re.search(r"\bIDCIF\s*:?\s*([A-Z0-9]{8,20})\b", upper_raw, flags=re.IGNORECASE)
    if m_idcif:
        idcif_candidate = _normalize_upper(m_idcif.group(1))
        if not re.fullmatch(idcif_pattern, idcif_candidate):
            return {
                "ok": False,
                "type": "invalid_idcif",
                "query": None,
                "error": (
                    "⚠️ IDCIF inválido.\n\n"
                    f"*Dato solicitado:* {_dato_solicitado_text(idcif_candidate)}\n\n"
                    "Debe contener únicamente 11 dígitos."
                )
            }
    
    # -------------------------------------------------
    # 2-A EXTRA) TEXTO CORRIDO PRO: RFC/IDCIF aunque estén mal
    # -------------------------------------------------
    compact_tokens = re.findall(r"[A-Z0-9Ñ&]+", upper_raw_clean)

    maybe_rfc = None
    maybe_idcif = None
    maybe_curp = None

    for tok in compact_tokens:
        if _looks_like_rfc(tok):
            maybe_rfc = tok
            break

    for tok in compact_tokens:
        if _looks_like_idcif(tok):
            maybe_idcif = tok
            break

    for tok in compact_tokens:
        if _looks_like_curp(tok):
            maybe_curp = tok
            break

    # CURP sospechosa en texto corrido
    if maybe_curp and not re.fullmatch(curp_pattern, maybe_curp):
        return {
            "ok": False,
            "type": "invalid_curp",
            "query": None,
            "error": (
                "⚠️ CURP inválida.\n\n"
                f"*Dato solicitado:* {_dato_solicitado_text(maybe_curp)}\n\n"
                "Debe tener 18 caracteres con formato correcto."
            )
        }

    # RFC sospechoso + posible IDCIF
    if maybe_rfc and not re.fullmatch(rfc_pattern, maybe_rfc):
        return {
            "ok": False,
            "type": "invalid_rfc",
            "query": None,
            "error": (
                "⚠️ RFC inválido.\n\n"
                f"*Dato solicitado:* {_dato_solicitado_text(maybe_rfc)}\n\n"
                "Persona física: 13 caracteres.\n"
                "Persona moral: 12 caracteres."
            )
        }

    # RFC válido + IDCIF sospechoso inválido
    if maybe_rfc and re.fullmatch(rfc_pattern, maybe_rfc) and maybe_idcif and not re.fullmatch(idcif_pattern, maybe_idcif):
        return {
            "ok": False,
            "type": "invalid_idcif",
            "query": None,
            "error": (
                "⚠️ IDCIF inválido.\n\n"
                f"*Dato solicitado:* {_dato_solicitado_text(maybe_idcif)}\n\n"
                "Debe contener únicamente 11 dígitos."
            )
        }

    # -------------------------------------------------
    # 2) INVÁLIDOS ESPECÍFICOS POR LÍNEA
    # -------------------------------------------------
    non_id_lines = []

    for line in lines if lines else [flat]:
        # RFC primero para que no se lo robe CURP
        if _looks_like_rfc(line) and not re.fullmatch(rfc_pattern, line):
            return {
                "ok": False,
                "type": "invalid_rfc",
                "query": None,
                "error": (
                    "⚠️ RFC inválido.\n\n"
                    f"*Dato solicitado:* {_dato_solicitado_text(line)}\n\n"
                    "Persona física: 13 caracteres.\n"
                    "Persona moral: 12 caracteres."
                )
            }

        if _looks_like_curp(line) and not re.fullmatch(curp_pattern, line):
            return {
                "ok": False,
                "type": "invalid_curp",
                "query": None,
                "error": (
                    "⚠️ CURP inválida.\n\n"
                    f"*Dato solicitado:* {_dato_solicitado_text(line)}\n\n"
                    "Debe tener 18 caracteres con formato correcto."
                )
            }

        if _looks_like_idcif(line) and not re.fullmatch(idcif_pattern, line):
            return {
                "ok": False,
                "type": "invalid_idcif",
                "query": None,
                "error": (
                    "⚠️ IDCIF inválido.\n\n"
                    f"*Dato solicitado:* {_dato_solicitado_text(line)}\n\n"
                    "Debe contener únicamente 11 dígitos."
                )
            }

        if (
            not re.fullmatch(curp_pattern, line)
            and not re.fullmatch(rfc_pattern, line)
            and not re.fullmatch(idcif_pattern, line)
        ):
            non_id_lines.append(line)

    for line in non_id_lines:
        if _looks_like_lugar(line) and not re.fullmatch(lugar_pattern, line):
            return {
                "ok": False,
                "type": "invalid_lugar",
                "query": None,
                "error": (
                    "⚠️ Lugar inválido.\n"
                    "Debes escribir municipio y entidad separados por coma.\n"
                )
            }

    # -------------------------------------------------
    # 2-B) DETECCIÓN PRO DE TOKENS INCRUSTADOS
    # -------------------------------------------------
    embedded = _extract_embedded_tokens(raw)

    if embedded["rfc"] and embedded["idcif"]:
        rfc = embedded["rfc"][0]
        idcif = embedded["idcif"][0]
        if embedded["lugar"]:
            return {
                "ok": True,
                "type": "rfc_idcif_lugar",
                "query": f"RFC: {rfc}\nIDCIF: {idcif}\n{embedded['lugar'][0]}",
                "error": None
            }
        return {
            "ok": True,
            "type": "rfc_idcif",
            "query": f"RFC: {rfc}\nIDCIF: {idcif}",
            "error": None
        }

    if embedded["curp"] and embedded["lugar"]:
        return {
            "ok": True,
            "type": "curp_lugar",
            "query": f"{embedded['curp'][0]}\n{embedded['lugar'][0]}",
            "error": None
        }

    if embedded["rfc"] and embedded["lugar"]:
        return {
            "ok": True,
            "type": "rfc_lugar",
            "query": f"{embedded['rfc'][0]}\n{embedded['lugar'][0]}",
            "error": None
        }

    # -------------------------------------------------
    # 3) ERROR GENÉRICO
    # -------------------------------------------------
    return {
        "ok": False,
        "type": "invalid_format",
        "query": None,
        "error": _format_input_error()
    }

PANEL_TZ = os.getenv("PANEL_TZ", "America/Monterrey").strip()

def _panel_now():
    return datetime.now(ZoneInfo(PANEL_TZ))

def _panel_day_str():
    return _panel_now().strftime("%Y-%m-%d")

def _panel_week_start(dt=None):
    dt = dt or _panel_now()
    start = dt - timedelta(days=dt.weekday())  # lunes
    return start.replace(hour=0, minute=0, second=0, microsecond=0)

def _panel_week_end(dt=None):
    return _panel_week_start(dt) + timedelta(days=7)

def _panel_month_start(dt=None):
    dt = dt or _panel_now()
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def _panel_month_end(dt=None):
    dt = dt or _panel_now()
    if dt.month == 12:
        next_month = dt.replace(year=dt.year+1, month=1, day=1)
    else:
        next_month = dt.replace(month=dt.month+1, day=1)
    return next_month.replace(hour=0, minute=0, second=0, microsecond=0)

def _daterange_days(start_dt, end_dt):
    days = []
    cur = start_dt
    while cur < end_dt:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days

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

def _panel_load_rows_for_days(days):
    rows_map = {}
    blocked = get_blocked_groups()

    for day in days:
        prefix = f"panel_stats:{day}:group:"
        for key in redis_conn.scan_iter(match=prefix + "*"):
            key_s = _to_str(key)

            raw = redis_conn.hgetall(key) or {}
            raw = {_to_str(k): _to_str(v) for k, v in raw.items()}

            group_jid = raw.get("group_jid") or key_s.split(":group:", 1)[-1]
            group_name = resolve_group_name(group_jid, raw.get("group_name"))

            if group_jid not in rows_map:
                rows_map[group_jid] = {
                    "group_jid": group_jid,
                    "group_name": group_name,
                    "total": 0,
                    "ok_rfc_idcif_qr": 0,
                    "ok_rfc_clon": 0,
                    "ok_rfc_idcif": 0,
                    "ok_qr": 0,
                    "ok_curp": 0,
                    "ok_rfc_only": 0,
                    "updated_at": "",
                    "blocked": group_jid in blocked,
                }

            rows_map[group_jid]["total"] += _safe_int(raw.get("total"))
            rows_map[group_jid]["ok_rfc_idcif_qr"] += _safe_int(raw.get("ok_rfc_idcif_qr"))
            rows_map[group_jid]["ok_rfc_clon"] += _safe_int(raw.get("ok_rfc_clon"))
            rows_map[group_jid]["ok_rfc_idcif"] += _safe_int(raw.get("ok_rfc_idcif"))
            rows_map[group_jid]["ok_qr"] += _safe_int(raw.get("ok_qr"))
            rows_map[group_jid]["ok_curp"] += _safe_int(raw.get("ok_curp"))
            rows_map[group_jid]["ok_rfc_only"] += _safe_int(raw.get("ok_rfc_only"))

            updated_at = raw.get("updated_at") or ""
            if updated_at and (not rows_map[group_jid]["updated_at"] or updated_at > rows_map[group_jid]["updated_at"]):
                rows_map[group_jid]["updated_at"] = updated_at

    # incluir grupos del mapa aunque no tengan actividad
    for group_jid, group_name in GROUP_NAME_MAP.items():
        if group_jid not in rows_map:
            rows_map[group_jid] = {
                "group_jid": group_jid,
                "group_name": resolve_group_name(group_jid, group_name),
                "total": 0,
                "ok_rfc_idcif_qr": 0,
                "ok_rfc_clon": 0,
                "ok_rfc_idcif": 0,
                "ok_qr": 0,
                "ok_curp": 0,
                "ok_rfc_only": 0,
                "updated_at": "",
                "blocked": group_jid in blocked,
            }

    # incluir bloqueados aunque no estén en mapa ni stats
    for group_jid in blocked:
        if group_jid not in rows_map:
            rows_map[group_jid] = {
                "group_jid": group_jid,
                "group_name": resolve_group_name(group_jid),
                "total": 0,
                "ok_rfc_idcif_qr": 0,
                "ok_rfc_clon": 0,
                "ok_rfc_idcif": 0,
                "ok_qr": 0,
                "ok_curp": 0,
                "ok_rfc_only": 0,
                "updated_at": "",
                "blocked": True,
            }
    
    # incluir grupos permitidos dinámicamente aunque no tengan stats
    for group_jid in get_dynamic_allowed_groups():
        if group_jid not in rows_map:
            rows_map[group_jid] = {
                "group_jid": group_jid,
                "group_name": resolve_group_name(group_jid),
                "total": 0,
                "ok_rfc_idcif_qr": 0,
                "ok_rfc_clon": 0,
                "ok_rfc_idcif": 0,
                "ok_qr": 0,
                "ok_curp": 0,
                "ok_rfc_only": 0,
                "updated_at": "",
                "blocked": group_jid in blocked,
            }

    rows = list(rows_map.values())
    rows.sort(key=lambda x: (x["blocked"], -x["total"], x["group_name"], x["group_jid"]))
    return rows

def _panel_load_today_rows():
    return _panel_load_rows_for_days([_panel_day_str()])

def _panel_load_week_rows():
    start = _panel_week_start()
    end = _panel_week_end()
    days = _daterange_days(start, end)
    return _panel_load_rows_for_days(days)

def _panel_load_month_rows():
    start = _panel_month_start()
    end = _panel_month_end()
    days = _daterange_days(start, end)
    return _panel_load_rows_for_days(days)

def _panel_summary(rows):
    return {
        "day": _panel_day_str(),
        "groups": sum(1 for r in rows if _safe_int(r.get("total")) > 0),
        "total": sum(r["total"] for r in rows),
        "ok_rfc_idcif_qr": sum(r["ok_rfc_idcif_qr"] for r in rows),
        "ok_rfc_clon": sum(r["ok_rfc_clon"] for r in rows),
        "ok_rfc_idcif": sum(r["ok_rfc_idcif"] for r in rows),
        "ok_qr": sum(r["ok_qr"] for r in rows),
        "ok_curp": sum(r["ok_curp"] for r in rows),
        "ok_rfc_only": sum(r["ok_rfc_only"] for r in rows),
    }

# =========================
# HELPERS DE CORTES
# =========================

MONTHS_ES = {
    1: "ENERO",
    2: "FEBRERO",
    3: "MARZO",
    4: "ABRIL",
    5: "MAYO",
    6: "JUNIO",
    7: "JULIO",
    8: "AGOSTO",
    9: "SEPTIEMBRE",
    10: "OCTUBRE",
    11: "NOVIEMBRE",
    12: "DICIEMBRE",
}

DAYS_ES = {
    0: "LUNES",
    1: "MARTES",
    2: "MIÉRCOLES",
    3: "JUEVES",
    4: "VIERNES",
    5: "SÁBADO",
    6: "DOMINGO",
}

def _safe_float(v, default=0.0):
    try:
        return float(v or 0)
    except Exception:
        return default

def _fmt_money(v: float) -> str:
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return "0.00"

def _period_day_label_es(day_str: str) -> str:
    dt = datetime.strptime(day_str, "%Y-%m-%d")
    return f"{dt.day:02d} {MONTHS_ES[dt.month]} {dt.year}"

def _today_label_es() -> str:
    now = _panel_now()
    return f"{now.day:02d} {MONTHS_ES[now.month]} {now.year}"

def _day_name_es(day_str: str) -> str:
    dt = datetime.strptime(day_str, "%Y-%m-%d")
    return DAYS_ES[dt.weekday()]

def _group_prices_key(group_jid: str) -> str:
    return f"{GROUP_PRICES_KEY_PREFIX}:{group_jid}"

def set_group_prices(group_jid: str, clon: float, idcif: float):
    try:
        if not group_jid:
            return
        redis_conn.hset(
            _group_prices_key(group_jid),
            mapping={
                "clon": round(_safe_float(clon), 2),
                "idcif": round(_safe_float(idcif), 2),
            }
        )
    except Exception as e:
        print("set_group_prices error:", repr(e), flush=True)

def _get_group_prices(group_jid: str):
    try:
        raw = redis_conn.hgetall(_group_prices_key(group_jid)) or {}
        raw = {_to_str(k): _to_str(v) for k, v in raw.items()}

        if raw:
            return {
                "clon": _safe_float(raw.get("clon"), DEFAULT_PRICES["clon"]),
                "idcif": _safe_float(raw.get("idcif"), DEFAULT_PRICES["idcif"]),
            }
    except Exception as e:
        print("_get_group_prices redis error:", repr(e), flush=True)

    p = GROUP_PRICES.get(group_jid) or DEFAULT_PRICES
    return {
        "clon": _safe_float(p.get("clon"), DEFAULT_PRICES["clon"]),
        "idcif": _safe_float(p.get("idcif"), DEFAULT_PRICES["idcif"]),
    }

def _cut_stats_key_for_day(day: str, group_jid: str) -> str:
    return f"cut_stats:{day}:group:{group_jid}"

def _period_days(view: str):
    view = (view or "day").strip().lower()

    if view == "month":
        start = _panel_month_start()
        end = _panel_month_end()
        return _daterange_days(start, end)

    if view == "prev_month":
        now = _panel_now()

        if now.month == 1:
            prev = now.replace(year=now.year - 1, month=12, day=1)
        else:
            prev = now.replace(month=now.month - 1, day=1)

        start = prev.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if prev.month == 12:
            end = prev.replace(year=prev.year + 1, month=1, day=1)
        else:
            end = prev.replace(month=prev.month + 1, day=1)

        end = end.replace(hour=0, minute=0, second=0, microsecond=0)

        return _daterange_days(start, end)

    return [_panel_day_str()]

def _load_cut_rows_for_days(days):
    rows_map = {}

    for day in days:
        prefix = f"cut_stats:{day}:group:"
        for key in redis_conn.scan_iter(match=prefix + "*"):
            key_s = _to_str(key)
            raw = redis_conn.hgetall(key) or {}
            raw = {_to_str(k): _to_str(v) for k, v in raw.items()}

            group_jid = raw.get("group_jid") or key_s.split(":group:", 1)[-1]
            group_name = resolve_group_name(group_jid, raw.get("group_name"))

            count_clon = _safe_int(raw.get("count_clon"))
            count_idcif = _safe_int(raw.get("count_idcif"))

            prices = _get_group_prices(group_jid)
            price_clon = prices["clon"]
            price_idcif = prices["idcif"]

            subtotal_clon = round(count_clon * price_clon, 2)
            subtotal_idcif = round(count_idcif * price_idcif, 2)
            total = round(subtotal_clon + subtotal_idcif, 2)

            if group_jid not in rows_map:
                rows_map[group_jid] = {
                    "group_jid": group_jid,
                    "group_name": group_name,
                    "count_clon": 0,
                    "count_idcif": 0,
                    "subtotal_clon": 0.0,
                    "subtotal_idcif": 0.0,
                    "total": 0.0,
                    "price_clon": price_clon,
                    "price_idcif": price_idcif,
                    "updated_at": "",
                }

            rows_map[group_jid]["count_clon"] += count_clon
            rows_map[group_jid]["count_idcif"] += count_idcif
            rows_map[group_jid]["subtotal_clon"] += subtotal_clon
            rows_map[group_jid]["subtotal_idcif"] += subtotal_idcif
            rows_map[group_jid]["total"] += total

            updated_at = raw.get("updated_at") or ""
            if updated_at and (not rows_map[group_jid]["updated_at"] or updated_at > rows_map[group_jid]["updated_at"]):
                rows_map[group_jid]["updated_at"] = updated_at

    rows = list(rows_map.values())
    rows.sort(key=lambda x: (-x["total"], x["group_name"], x["group_jid"]))
    return rows

def _load_cut_detail_for_group(group_jid: str, days):
    prices = _get_group_prices(group_jid)
    group_name = resolve_group_name(group_jid)
    detail = []

    total_clon = 0
    total_idcif = 0
    total_sub_clon = 0.0
    total_sub_idcif = 0.0
    total_general = 0.0

    week_clon = 0
    week_idcif = 0
    week_sub_clon = 0.0
    week_sub_idcif = 0.0
    week_total = 0.0
    week_start = None

    for i, day in enumerate(days):
        raw = redis_conn.hgetall(_cut_stats_key_for_day(day, group_jid)) or {}
        raw = {_to_str(k): _to_str(v) for k, v in raw.items()}

        if raw.get("group_name"):
            group_name = raw.get("group_name")

        count_clon = _safe_int(raw.get("count_clon"))
        count_idcif = _safe_int(raw.get("count_idcif"))

        subtotal_clon = round(count_clon * prices["clon"], 2)
        subtotal_idcif = round(count_idcif * prices["idcif"], 2)
        total = round(subtotal_clon + subtotal_idcif, 2)

        day_name = _day_name_es(day)

        detail.append({
            "is_cut": False,
            "date": day,
            "day_name": day_name,
            "count_clon": count_clon,
            "count_idcif": count_idcif,
            "price_clon": prices["clon"],
            "price_idcif": prices["idcif"],
            "subtotal_clon": subtotal_clon,
            "subtotal_idcif": subtotal_idcif,
            "total": total,
        })

        total_clon += count_clon
        total_idcif += count_idcif
        total_sub_clon += subtotal_clon
        total_sub_idcif += subtotal_idcif
        total_general += total

        if week_start is None:
            week_start = day

        week_clon += count_clon
        week_idcif += count_idcif
        week_sub_clon += subtotal_clon
        week_sub_idcif += subtotal_idcif
        week_total += total

        is_last_day = i == len(days) - 1

        if day_name == "DOMINGO" or is_last_day:
            detail.append({
                "is_cut": True,
                "label": "CORTE SEMANAL",
                "from_date": week_start,
                "to_date": day,
                "week_clon": week_clon,
                "week_idcif": week_idcif,
                "price_clon": prices["clon"],
                "price_idcif": prices["idcif"],
                "week_sub_clon": round(week_sub_clon, 2),
                "week_sub_idcif": round(week_sub_idcif, 2),
                "week_total": round(week_total, 2),
            })

            week_clon = 0
            week_idcif = 0
            week_sub_clon = 0.0
            week_sub_idcif = 0.0
            week_total = 0.0
            week_start = None

    return {
        "group_jid": group_jid,
        "group_name": group_name,
        "price_clon": prices["clon"],
        "price_idcif": prices["idcif"],
        "rows": detail,
        "totals": {
            "count_clon": total_clon,
            "count_idcif": total_idcif,
            "subtotal_clon": round(total_sub_clon, 2),
            "subtotal_idcif": round(total_sub_idcif, 2),
            "total": round(total_general, 2),
        }
    }

def _cut_summary(rows):
    return {
        "groups": sum(1 for r in rows if _safe_float(r.get("total")) > 0),
        "count_clon": sum(_safe_int(r.get("count_clon")) for r in rows),
        "count_idcif": sum(_safe_int(r.get("count_idcif")) for r in rows),
        "subtotal_clon": round(sum(_safe_float(r.get("subtotal_clon")) for r in rows), 2),
        "subtotal_idcif": round(sum(_safe_float(r.get("subtotal_idcif")) for r in rows), 2),
        "total": round(sum(_safe_float(r.get("total")) for r in rows), 2),
    }

def _build_cut_message(group_name: str, date_label: str, count_clon: int, price_clon: float, subtotal_clon: float, count_idcif: int, price_idcif: float, subtotal_idcif: float, total: float) -> str:
    return (
        f"🚀 *DOCU EXPRES*\n\n"
        f"*{group_name}*\n"
        f"*CORTE DE FECHA* {date_label}\n\n"
        f"{count_clon} RFC clon x ${_fmt_money(price_clon)} = ${_fmt_money(subtotal_clon)}\n"
        f"{count_idcif} RFC idcif x ${_fmt_money(price_idcif)} = ${_fmt_money(subtotal_idcif)}\n"
        f"${_fmt_money(total)} pesos\n\n"
        f"*Beneficiario:* {BENEFICIARIO_CORTE}\n"
        f"*CLABE:* {CLABE_CORTE}\n"
        f"*Entidad financiera:* {ENTIDAD_CORTE}\n\n"
        f"*Favor de mandar comprobante*\n"
        f"Agradecemos su preferencia"
    )

def send_daily_cut_for_group(group_jid: str, day_str: str = None, instance_name=None):
    instance_name = _safe(instance_name) or EVOLUTION_INSTANCE

    if is_group_no_corte(group_jid):
        return {
            "ok": False,
            "error": "Grupo excluido de corte."
        }
    
    day_str = (day_str or _panel_day_str()).strip()
    rows = _load_cut_rows_for_days([day_str])

    target = None
    for r in rows:
        if r["group_jid"] == group_jid:
            target = r
            break

    if not target or _safe_float(target.get("total")) <= 0:
        return {
            "ok": False,
            "error": "Sin actividad para ese grupo en esa fecha."
        }

    msg = _build_cut_message(
        group_name=target["group_name"],
        date_label=_period_day_label_es(day_str),
        count_clon=_safe_int(target["count_clon"]),
        price_clon=_safe_float(target["price_clon"]),
        subtotal_clon=_safe_float(target["subtotal_clon"]),
        count_idcif=_safe_int(target["count_idcif"]),
        price_idcif=_safe_float(target["price_idcif"]),
        subtotal_idcif=_safe_float(target["subtotal_idcif"]),
        total=_safe_float(target["total"]),
    )

    evolution_send_text(
        group_jid=group_jid,
        text=msg,
        instance_name=instance_name
    )

    return {
        "ok": True,
        "group_jid": group_jid,
        "group_name": target["group_name"],
        "day": day_str,
        "total": _safe_float(target["total"]),
        "instance_name": instance_name,
    }

def send_daily_cuts(day_str: str = None, instance_name=None):
    instance_name = _safe(instance_name) or EVOLUTION_INSTANCE

    day_str = (day_str or _panel_day_str()).strip()
    rows = _load_cut_rows_for_days([day_str])

    sent = []
    skipped = []
    
    for r in rows:
        if is_group_no_corte(r["group_jid"]):
            skipped.append(r["group_jid"])
            continue
        
        if _safe_float(r.get("total")) <= 0:
            skipped.append(r["group_jid"])
            continue

        msg = _build_cut_message(
            group_name=r["group_name"],
            date_label=_period_day_label_es(day_str),
            count_clon=_safe_int(r["count_clon"]),
            price_clon=_safe_float(r["price_clon"]),
            subtotal_clon=_safe_float(r["subtotal_clon"]),
            count_idcif=_safe_int(r["count_idcif"]),
            price_idcif=_safe_float(r["price_idcif"]),
            subtotal_idcif=_safe_float(r["subtotal_idcif"]),
            total=_safe_float(r["total"]),
        )

        evolution_send_text(
            group_jid=r["group_jid"],
            text=msg,
            instance_name=instance_name
        )

        sent.append({
            "group_jid": r["group_jid"],
            "group_name": r["group_name"],
            "total": _safe_float(r["total"]),
        })

    return {
        "ok": True,
        "day": day_str,
        "sent": sent,
        "skipped": skipped,
        "instance_name": instance_name,
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

def evolution_send_text(
    group_jid=None,
    number=None,
    text="",
    instance_name=None,
    timeout=(3.05, 12),
):
    instance_name = _safe(instance_name) or EVOLUTION_INSTANCE

    url = (
        f"{EVOLUTION_BASE_URL}/message/sendText/"
        f"{instance_name}"
    )

    payload = {
        "text": text,
    }

    if group_jid:
        payload["number"] = group_jid
    elif number:
        payload["number"] = number
    else:
        raise ValueError(
            "evolution_send_text requiere group_jid o number"
        )

    started_at = time.monotonic()

    try:
        response = requests.post(
            url,
            json=payload,
            headers=evolution_headers(),
            timeout=timeout,
        )

        elapsed = round(
            time.monotonic() - started_at,
            3,
        )

        print(
            "[EVOLUTION SEND TEXT]",
            {
                "instance": instance_name,
                "number": payload.get("number"),
                "status": response.status_code,
                "seconds": elapsed,
            },
            flush=True,
        )

        response.raise_for_status()
        return response.json()

    except requests.Timeout:
        elapsed = round(
            time.monotonic() - started_at,
            3,
        )

        print(
            "[EVOLUTION SEND TEXT TIMEOUT]",
            {
                "instance": instance_name,
                "number": payload.get("number"),
                "seconds": elapsed,
            },
            flush=True,
        )

        raise

def evolution_send_ack_fast(
    group_jid: str,
    requester_label: str,
    instance_name: str,
):
    return evolution_send_text(
        group_jid=group_jid,
        text=(
            "👽 DOCIFY MX\n"
            f"Solicitud recibida de {requester_label}.\n"
            "Esto puede tardar unos minutos..."
        ),
        instance_name=instance_name,
        timeout=(2.5, 8),
    )

def _redis_setnx_ttl(key: str, ttl: int) -> bool:
    return bool(redis_conn.set(key, "1", ex=ttl, nx=True))

# =========================
# FLUJO PUENTE IDCIF
# =========================

IDCIF_CURP_RE = re.compile(r"\b[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d\b", re.I)
IDCIF_RFC_RE = re.compile(r"\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b", re.I)
IDCIF_IDCIF_RE = re.compile(r"\b\d{11}\b")

IDCIF_NO_PATTERNS = (
    "NO HAY IDCIF",
    "SIN IDCIF",
    "NO TIENE IDCIF",
    "NO SE ENCONTRO IDCIF",
    "NO SE ENCONTRÓ IDCIF",
    "NO LOCALIZADO",
    "NO LOCALIZADA",
    "NO ENCONTRADO",
    "NO ENCONTRADA",
    "SIN REGISTRO",
    "SIN RESULTADO",
)


def _idcif_norm(text: str) -> str:
    return re.sub(r"\s+", " ", _normalize_upper(text or "")).strip()


def _idcif_pending_key(term: str) -> str:
    term = _idcif_norm(term)
    return f"{IDCIF_PENDING_PREFIX}:{term}"


def _idcif_extract_curp(text: str) -> str:
    m = IDCIF_CURP_RE.search(_normalize_upper(text or ""))
    return m.group(0).upper() if m else ""


def _idcif_extract_rfc(text: str) -> str:
    m = IDCIF_RFC_RE.search(_normalize_upper(text or ""))
    return m.group(0).upper() if m else ""


def _idcif_extract_idcif(text: str) -> str:
    m = IDCIF_IDCIF_RE.search(_normalize_upper(text or ""))
    return m.group(0) if m else ""


def _idcif_extract_request_term(text: str) -> tuple[str, str]:
    """
    Extrae CURP o RFC.
    Prioridad CURP porque el usuario pidió que también acepte CURP.
    """
    curp = _idcif_extract_curp(text)
    if curp:
        return "CURP", curp

    rfc = _idcif_extract_rfc(text)
    if rfc:
        return "RFC", rfc

    return "", ""


def _idcif_is_no_idcif(text: str) -> bool:
    t = _idcif_norm(text)
    return any(p in t for p in IDCIF_NO_PATTERNS)


def _idcif_save_pending(term: str, data: dict):
    term = _idcif_norm(term)
    if not term:
        return

    key = _idcif_pending_key(term)
    data = dict(data or {})
    data["term"] = term
    data["created_at"] = int(time.time())

    redis_conn.set(
        key,
        json.dumps(data, ensure_ascii=False),
        ex=IDCIF_PENDING_TTL_SEC,
    )


def _idcif_load_pending(term: str) -> dict | None:
    term = _idcif_norm(term)
    if not term:
        return None

    raw = redis_conn.get(_idcif_pending_key(term))
    if not raw:
        return None

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")

    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _idcif_delete_pending(term: str):
    term = _idcif_norm(term)
    if term:
        redis_conn.delete(_idcif_pending_key(term))


def _idcif_pop_pending(term: str) -> dict | None:
    data = _idcif_load_pending(term)
    if data:
        _idcif_delete_pending(term)
    return data


def _idcif_release_request_lock(pending: dict):
    if not isinstance(pending, dict):
        return

    request_lock_key = (
        pending.get("request_lock_key")
        or ""
    ).strip()

    if request_lock_key:
        redis_conn.delete(request_lock_key)

        print(
            "[IDCIF REQUEST LOCK RELEASED]",
            request_lock_key,
            flush=True,
        )


def _idcif_scan_single_pending() -> tuple[str, dict] | tuple[str, None]:
    """
    Fallback: si la proveedora responde solo RFC+IDCIF, pero la solicitud original era CURP,
    no siempre podemos empatar por término. Si hay UNA sola pendiente, la usamos.
    Si hay varias, no adivinamos.
    """
    found = []

    try:
        for key in redis_conn.scan_iter(f"{IDCIF_PENDING_PREFIX}:*"):
            raw_key = key.decode("utf-8", errors="ignore") if isinstance(key, bytes) else str(key)
            raw = redis_conn.get(key)

            if not raw:
                continue

            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="ignore")

            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    found.append((raw_key, data))
            except Exception:
                continue

    except Exception as e:
        print("IDCIF_SCAN_PENDING_ERROR =", repr(e), flush=True)
        return "", None

    if len(found) == 1:
        key, data = found[0]
        redis_conn.delete(key)
        return key, data

    return "", None


def _idcif_pop_pending_from_provider_text(text: str) -> tuple[str, dict | None]:
    """
    Intenta empatar respuesta de proveedora contra pendiente:
    - por CURP si viene CURP
    - por RFC si viene RFC
    - si no empata y solo hay una pendiente, usa esa única pendiente
    """
    terms = []

    curp = _idcif_extract_curp(text)
    rfc = _idcif_extract_rfc(text)

    if curp:
        terms.append(curp)
    if rfc and rfc not in terms:
        terms.append(rfc)

    for term in terms:
        pending = _idcif_pop_pending(term)
        if pending:
            return term, pending

    single_key, single_pending = _idcif_scan_single_pending()
    if single_pending:
        return single_key, single_pending

    return "", None


def _idcif_send_to_client(client_group: str, text: str):
    """
    Todo lo que vea el cliente debe salir por el bot RFC actual.
    """
    return evolution_send_text(
        group_jid=client_group,
        text=text,
        instance_name=RFC_RESPONSE_INSTANCE,
    )


def _idcif_send_to_provider(text: str):
    """
    Todo lo que vaya a la proveedora debe salir por el bot IDCIF nuevo.
    """
    return evolution_send_text(
        group_jid=IDCIF_PROVIDER_GROUP,
        text=text,
        instance_name=IDCIF_RECEIVE_INSTANCE,
    )


def _idcif_enqueue_generation(pending: dict, query: str, provider_msg_id: str = ""):
    """
    Reusa la cola normal constancia_jobs.
    worker_jobs.py ya manda a constancia-backend-rfc y envía PDF por evolution_instance.
    """
    client_group = (pending.get("client_group") or "").strip()
    requester_number = (pending.get("requester_number") or "").strip()
    requester_name = (pending.get("requester_name") or "").strip()
    requester_label = (pending.get("requester_label") or "Usuario").strip()
    group_name = (pending.get("group_name") or client_group).strip()
    original_text = (pending.get("original_text") or query).strip()
    original_msg_id = (pending.get("msg_id") or "").strip()

    request_raw = (
        f"{RFC_RESPONSE_INSTANCE}|"
        f"{client_group}|"
        f"{requester_number}|"
        f"{_normalize_upper(query)}"
    )
    
    request_key = hashlib.sha256(
        request_raw.encode("utf-8")
    ).hexdigest()

    job_data = {
        "requester_number": requester_number,
        "requester_name": requester_name,
        "requester_label": requester_label,
        "group_jid": client_group,
        "group_name": group_name,
        "original_text": original_text,
        "query": query,
        "query_type": "rfc_idcif",
        "msg_type": "text",
        "media_id": "",
        "msg_id": f"idcif:{original_msg_id}:{provider_msg_id}",
        "mime_type": "",
        "bot_internal_url": BOT_INTERNAL_URL,
        "bot_internal_token": BOT_INTERNAL_TOKEN,
        "evolution_instance": RFC_RESPONSE_INSTANCE,
        "request_key": request_key,
    }

    rq_job_id = f"idcif-generation:{request_key}"

    try:
        task_queue.enqueue(
            "worker_jobs.process_group_request_job",
            job_data,
            job_id=rq_job_id,
            job_timeout=900,
            result_ttl=0,
            failure_ttl=1200,
        )
    
    except Exception as enqueue_err:
        err_text = str(enqueue_err).lower()
    
        if (
            "already exists" in err_text
            or "duplicate" in err_text
        ):
            print(
                "[IDCIF DUPLICATE GENERATION BLOCKED]",
                rq_job_id,
                repr(enqueue_err),
                flush=True,
            )
            return False
    
        raise
    
    return True


def _handle_idcif_client_message(
    remote_jid: str,
    requester_number: str,
    push_name: str,
    group_name: str,
    text: str,
    msg_id: str,
):
    kind, term = _idcif_extract_request_term(text)

    if not term:
        _idcif_send_to_client(
            remote_jid,
            (
                "⚠️ Formato no válido para IDCIF.\n\n"
                f"*Dato solicitado:* {_dato_solicitado_text(text)}\n\n"
                "Envía solamente RFC o CURP."
            ),
        )
        return {"ok": True, "handled": "idcif_client_invalid"}

    idcif_request_lock = _idcif_request_lock_key(
        remote_jid,
        requester_number,
        term,
    )
    
    if not _redis_setnx_ttl(
        idcif_request_lock,
        IDCIF_REQUEST_LOCK_TTL_SEC,
    ):
        _idcif_send_to_client(
            remote_jid,
            (
                "⏳ Esta solicitud IDCIF ya está en proceso.\n\n"
                f"*Dato solicitado:* {term}\n\n"
                "No es necesario volver a enviarla."
            ),
        )
    
        return {
            "ok": True,
            "handled": "idcif_duplicate_request_blocked",
            "term": term,
        }

    if not IDCIF_PROVIDER_GROUP:
        redis_conn.delete(idcif_request_lock)
    
        _idcif_send_to_client(
            remote_jid,
            (
                "⚠️ No está configurado el grupo de la proveedora IDCIF.\n\n"
                f"*Dato solicitado:* {term}"
            ),
        )
    
        return {
            "ok": False,
            "handled": "idcif_provider_group_missing",
        }

    _idcif_save_pending(term, {
        "term": term,
        "kind": kind,
        "client_group": remote_jid,
        "requester_number": requester_number,
        "requester_name": push_name,
        "requester_label": push_name or "Usuario",
        "group_name": group_name,
        "original_text": text,
        "msg_id": msg_id,
        "request_lock_key": idcif_request_lock,
    })

    # Aviso al cliente sale por el bot RFC actual.
    _idcif_send_to_client(
        remote_jid,
        (
            "⏳ *Solicitud IDCIF recibida*\n\n"
            f"Dato: {term}\n"
            "Estoy solicitando el IDCIF."
        ),
    )

    # Solicitud a proveedora sale por el bot IDCIF nuevo.
    try:
        _idcif_send_to_provider(
            (
                "🟡 *SOLICITUD IDCIF*\n\n"
                f"Dato: {term}\n"
                f"Tipo: {kind}\n"
                f"Grupo cliente: {group_name or remote_jid}\n"
                f"Solicitante: {push_name or requester_number}\n\n"
                "Responder con:\n"
                "RFC IDCIF\n\n"
                "Si el dato enviado fue CURP, favor de responder con RFC + IDCIF.\n\n"
                "Si no hay IDCIF:\n"
                f"NO HAY IDCIF {term}"
            )
        )
    
    except Exception as provider_send_err:
        print(
            "[IDCIF PROVIDER SEND ERROR]",
            repr(provider_send_err),
            flush=True,
        )
    
        _idcif_delete_pending(term)
        redis_conn.delete(idcif_request_lock)
    
        _idcif_send_to_client(
            remote_jid,
            (
                "⚠️ No fue posible enviar la solicitud IDCIF "
                "a la proveedora.\n\n"
                f"*Dato solicitado:* {term}\n\n"
                "Puedes intentarlo nuevamente."
            ),
        )
    
        return {
            "ok": False,
            "handled": "idcif_provider_send_failed",
            "term": term,
        }

    return {
        "ok": True,
        "handled": "idcif_client_forwarded_to_provider",
        "term": term,
        "kind": kind,
    }


def _handle_idcif_provider_message(text: str, msg_id: str):
    # Caso A: proveedora dice que no hay IDCIF.
    if _idcif_is_no_idcif(text):
        matched_term, pending = _idcif_pop_pending_from_provider_text(text)

        if not pending:
            print(
                "IDCIF_NO_IDCIF_WITHOUT_PENDING =",
                {
                    "text": text,
                    "matched_term": matched_term,
                },
                flush=True,
            )
            return {
                "ok": True,
                "handled": "idcif_no_idcif_without_pending",
            }
        
        _idcif_release_request_lock(pending)
        
        client_group = pending.get("client_group") or ""
        term = pending.get("term") or matched_term or _idcif_extract_rfc(text) or _idcif_extract_curp(text)

        _idcif_send_to_client(
            client_group,
            (
                "❌ *No se localizó IDCIF*\n\n"
                f"Dato: {term}\n\n"
                "No se generó constancia."
            ),
        )

        return {
            "ok": True,
            "handled": "idcif_no_idcif_notified_client",
            "term": term,
        }

    # Caso B: proveedora responde con IDCIF.
    idcif = _idcif_extract_idcif(text)
    if not idcif:
        return {"ok": True, "handled": "idcif_provider_text_without_idcif"}

    matched_term, pending = _idcif_pop_pending_from_provider_text(text)

    if not pending:
        print("IDCIF_PROVIDER_IDCIF_WITHOUT_PENDING =", {
            "text": text,
            "matched_term": matched_term,
            "idcif": idcif,
        }, flush=True)
        return {"ok": True, "handled": "idcif_provider_idcif_without_pending"}

    client_group = pending.get("client_group") or ""

    # Para generar por IDCIF, constancia-backend-rfc necesita RFC + IDCIF.
    # Si la proveedora mandó RFC, usamos ese RFC.
    rfc_from_provider = _idcif_extract_rfc(text)

    # Si la solicitud original fue RFC y la proveedora solo puso IDCIF, combinamos RFC pendiente + IDCIF.
    pending_kind = (pending.get("kind") or "").upper()
    pending_term = (pending.get("term") or "").strip().upper()

    if rfc_from_provider:
        query = f"RFC: {rfc_from_provider}\nIDCIF: {idcif}"
    elif pending_kind == "RFC" and pending_term:
        query = f"RFC: {pending_term}\nIDCIF: {idcif}"
    else:
        # Si era CURP y la proveedora solo responde CURP + IDCIF, no alcanza para RFC_IDCIF.
        # Se le pide que mande RFC + IDCIF.
        _idcif_send_to_client(
            client_group,
            (
                "⚠️ Se recibió un IDCIF, pero falta el RFC para generar la constancia.\n\n"
                f"Dato original: {pending_term}\n"
                "Se pidió corrección a la proveedora."
            ),
        )

        _idcif_send_to_provider(
            (
                "⚠️ Para generar la constancia necesito RFC + IDCIF.\n\n"
                f"Dato original: {pending_term}\n"
                f"IDCIF recibido: {idcif}\n\n"
                "Responder en este formato:\n"
                "RFC IDCIF"
            )
        )

        # Volvemos a guardar pendiente para que la proveedora pueda corregir.
        _idcif_save_pending(pending_term, pending)

        return {
            "ok": True,
            "handled": "idcif_received_without_rfc_waiting_correction",
            "term": pending_term,
            "idcif": idcif,
        }

    enqueued = _idcif_enqueue_generation(
        pending=pending,
        query=query,
        provider_msg_id=msg_id,
    )
    
    if enqueued:
        _idcif_release_request_lock(pending)
    
        _idcif_send_to_client(
            client_group,
            (
                "✅ *IDCIF localizado*\n\n"
                "Generando constancia..."
            ),
        )
    
        return {
            "ok": True,
            "handled": "idcif_enqueued_generation",
            "query": query,
        }
    
    print(
        "[IDCIF GENERATION NOT ENQUEUED]",
        {
            "client_group": client_group,
            "query": query,
        },
        flush=True,
    )
    
    # No liberamos el lock inmediatamente para impedir otra ráfaga.
    # Expirará por TTL si el job duplicado ya estaba activo.
    return {
        "ok": True,
        "handled": "idcif_generation_duplicate_or_not_enqueued",
        "query": query,
    }


def _handle_idcif_bridge_message(
    instance_name: str,
    remote_jid: str,
    requester_number: str,
    push_name: str,
    group_name: str,
    text: str,
    msg_type: str,
    msg_id: str,
):
    """
    Maneja SOLO el bot IDCIF nuevo.
    """
    if not IDCIF_RECEIVE_INSTANCE:
        return {"ok": True, "handled": False, "ignored": "idcif_instance_not_configured"}

    if instance_name != IDCIF_RECEIVE_INSTANCE:
        return {"ok": True, "handled": False, "ignored": "not_idcif_instance"}

    if msg_type != "text":
        return {"ok": True, "handled": True, "ignored": "idcif_only_text"}

    if remote_jid == IDCIF_PROVIDER_GROUP:
        result = _handle_idcif_provider_message(text=text, msg_id=msg_id)
        result["handled"] = result.get("handled") or "idcif_provider"
        return result

    return _handle_idcif_client_message(
        remote_jid=remote_jid,
        requester_number=requester_number,
        push_name=push_name,
        group_name=group_name,
        text=text,
        msg_id=msg_id,
    )

@app.get("/")
def health():
    return jsonify({"ok": True, "service": "wa-group-bridge"}), 200

@app.post("/evolution/webhook")
def evolution_webhook():
    try:
        webhook_started_at = time.monotonic()
        webhook_received_epoch = time.time()
        secret = request.headers.get("x-bridge-secret", "").strip()
        if EVOLUTION_WEBHOOK_SECRET and secret != EVOLUTION_WEBHOOK_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        payload = request.get_json(silent=True) or {}
        print("EVOLUTION WEBHOOK:", payload, flush=True)

        data_for_timestamp = payload.get("data") or {}

        raw_message_timestamp = (
            data_for_timestamp.get("messageTimestamp")
            or payload.get("messageTimestamp")
            or 0
        )
        
        try:
            message_timestamp = float(raw_message_timestamp)
        
            # Algunos payloads pueden venir en milisegundos.
            if message_timestamp > 10_000_000_000:
                message_timestamp /= 1000.0
        
            webhook_delivery_delay = round(
                webhook_received_epoch - message_timestamp,
                3,
            )
        
        except Exception:
            message_timestamp = 0
            webhook_delivery_delay = None
        
        print(
            "[WEBHOOK RECEIVED]",
            {
                "server_epoch": webhook_received_epoch,
                "message_timestamp": message_timestamp,
                "delivery_delay_seconds": webhook_delivery_delay,
            },
            flush=True,
        )

        instance_name = _payload_instance(payload)
        print("[EVOLUTION INSTANCE]", repr(instance_name), flush=True)

        msg = _extract_evolution_message(payload)

        print("[GROUP NAME RAW FROM MSG]", repr(msg.get("group_name")), flush=True)

        remote_jid = msg["remote_jid"]
        participant = msg["participant"]
        msg_id = msg["msg_id"]
        from_me = msg["from_me"]
        text = msg["text"]
        push_name = msg["push_name"] or "Usuario"
        group_name = resolve_group_name(remote_jid, msg.get("group_name"))

        print("[GROUP NAME FINAL BEFORE JOB]", repr(group_name), flush=True)
        print("[REMOTE JID]", repr(remote_jid), flush=True)

        if not remote_jid.endswith("@g.us"):
            return jsonify({"ok": True, "ignored": "not_group"}), 200

        if is_group_blocked(remote_jid):
            return jsonify({"ok": True, "ignored": "group_blocked"}), 200

        if not participant and not payload.get("sender"):
            return jsonify({"ok": True, "ignored": "no_participant"}), 200

        admin_cmd = _parse_group_admin_command(text)

        requester_number = _normalize_phone(
            (
                payload.get("sender")
                if from_me else participant
            ) or ""
        )
        
        print("ADMIN_DEBUG_FROM_ME =", from_me, flush=True)
        print("ADMIN_DEBUG_PARTICIPANT =", participant, flush=True)
        print("ADMIN_DEBUG_SENDER =", payload.get("sender"), flush=True)
        print("ADMIN_DEBUG_REQUESTER =", requester_number, flush=True)
        
        if from_me and not admin_cmd["ok"]:
            return jsonify({"ok": True, "ignored": "from_me"}), 200

        if admin_cmd["ok"]:
            if requester_number not in ADMIN_NUMBERS:
                return jsonify({"ok": True, "ignored": "not_admin"}), 200
                
            try:
                if admin_cmd["command"] == "groupid":
                    current_name = resolve_group_name(remote_jid, msg.get("group_name"))
                    evolution_send_text(
                        group_jid=remote_jid,
                        text=(
                            "📍 *DATOS DEL GRUPO*\n\n"
                            f"Nombre: {current_name or 'SIN NOMBRE'}\n"
                            f"JID: {remote_jid}"
                        ),
                        instance_name=instance_name
                    )
                    return jsonify({
                        "ok": True,
                        "handled": "groupid",
                        "group_jid": remote_jid
                    }), 200

                if admin_cmd["command"] == "addgroup":
                    alias = _safe(admin_cmd.get("args"))
                    detected_name = resolve_group_name(remote_jid, msg.get("group_name"))

                    add_allowed_group(remote_jid)

                    if alias:
                        set_group_alias(remote_jid, alias)
                        final_name = alias
                    else:
                        # si no mandan alias, guardar el nombre detectado solo si existe
                        if detected_name and detected_name != remote_jid:
                            set_group_alias(remote_jid, detected_name)
                        final_name = resolve_group_name(remote_jid, msg.get("group_name"))

                    evolution_send_text(
                        group_jid=remote_jid,
                        text=(
                            "✅ *GRUPO AGREGADO*\n\n"
                            f"Nombre: {final_name or 'SIN NOMBRE'}\n"
                            f"JID: {remote_jid}\n\n"
                            "Este grupo ya quedó autorizado."
                        ),
                        instance_name=instance_name
                    )
                    return jsonify({
                        "ok": True,
                        "handled": "addgroup",
                        "group_jid": remote_jid,
                        "group_name": final_name
                    }), 200

            except Exception as e:
                print("admin group command error:", repr(e), flush=True)
                traceback.print_exc()
                return jsonify({
                    "ok": True,
                    "handled": False,
                    "error": str(e)
                }), 200

        if not is_group_allowed(remote_jid):
            # El grupo de proveedora IDCIF puede no estar en ALLOWED_GROUPS,
            # pero debe poder responderle al bot IDCIF.
            is_idcif_provider_group = (
                IDCIF_RECEIVE_INSTANCE
                and instance_name == IDCIF_RECEIVE_INSTANCE
                and IDCIF_PROVIDER_GROUP
                and remote_jid == IDCIF_PROVIDER_GROUP
            )
        
            if not is_idcif_provider_group:
                return jsonify({"ok": True, "ignored": "group_not_allowed"}), 200

        dedupe_key = f"dedupe:{instance_name}:{msg_id}"
        if not _redis_setnx_ttl(dedupe_key, 600):
            return jsonify({"ok": True, "ignored": "duplicate"}), 200

        msg_type = msg["msg_type"]
        media_id = msg["media_id"]
        mime_type = msg["mime_type"]

        # =========================
        # FLUJO ESPECIAL BOT IDCIF
        # =========================
        if IDCIF_RECEIVE_INSTANCE and instance_name == IDCIF_RECEIVE_INSTANCE:
            result = _handle_idcif_bridge_message(
                instance_name=instance_name,
                remote_jid=remote_jid,
                requester_number=requester_number,
                push_name=push_name,
                group_name=group_name,
                text=text,
                msg_type=msg_type,
                msg_id=msg_id,
            )
        
            return jsonify(result), 200
        
        # En texto, solo procesar si realmente parece consulta del bot
        if msg_type == "text" and not _is_text_candidate(text):
            return jsonify({"ok": True, "ignored": "chat_text"}), 200

        parsed = _parse_command(text)

        is_media_candidate = (
            msg_type in ("image", "document")
            and bool(media_id)
        )
        
        # Si NO es media y además el texto es inválido, responder error al usuario
        if not is_media_candidate:
            if not parsed.get("ok"):
                try:
                    evolution_send_text(
                        group_jid=remote_jid,
                        text=parsed.get("error") or _format_input_error(),
                        instance_name=instance_name
                    )
                except Exception as e:
                    print("validation sendText error:", repr(e), flush=True)
        
                return jsonify({
                    "ok": True,
                    "ignored": "invalid_input",
                    "reason": parsed.get("type")
                }), 200
        
        query = parsed.get("query")

        requester_label = (push_name or "Usuario").strip()

        if query:
            normalized_query = re.sub(r"\s+", " ", query.strip().upper())
        else:
            normalized_query = f"MEDIA:{msg_type}:{media_id}"
        
        command_raw = (
            f"{instance_name}|"
            f"{remote_jid}|"
            f"{requester_number}|"
            f"{normalized_query}"
        )
        
        command_key = hashlib.sha256(
            command_raw.encode("utf-8")
        ).hexdigest()
        
        inflight_key = f"inflight:{command_key}"

        processed_key = f"processed:{command_key}"

        if redis_conn.exists(processed_key):
            print(
                "[PROCESSED REQUEST BLOCKED]",
                {
                    "instance": instance_name,
                    "group": remote_jid,
                    "requester": requester_number,
                    "query": normalized_query,
                    "processed_key": processed_key,
                },
                flush=True,
            )
        
            processed_notice_key = (
                f"processed_notice:"
                f"{instance_name}:"
                f"{command_key}"
            )
        
            # Evita mandar varios avisos si lo repite muchas veces seguidas.
            if _redis_setnx_ttl(
                processed_notice_key,
                60,
            ):
                try:
                    evolution_send_text(
                        group_jid=remote_jid,
                        text=(
                            f"⏳ {requester_label}, esta solicitud "
                            "ya fue procesada.\n\n"
                            f"*Dato solicitado:* {_dato_solicitado_text(query or text)}\n\n"
                            "No es necesario volver a enviarla."
                        ),
                        instance_name=instance_name,
                        timeout=(2.5, 8),
                    )
        
                except Exception as processed_notice_error:
                    print(
                        "[PROCESSED NOTICE ERROR]",
                        repr(processed_notice_error),
                        flush=True,
                    )
        
            return jsonify({
                "ok": True,
                "ignored": "already_processed",
                "message": (
                    "La misma solicitud ya fue procesada."
                ),
            }), 200
        
        if not _redis_setnx_ttl(
            inflight_key,
            REQUEST_INFLIGHT_TTL_SEC,
        ):
            print(
                "[DUPLICATE REQUEST BLOCKED]",
                {
                    "instance": instance_name,
                    "group": remote_jid,
                    "requester": requester_number,
                    "query": normalized_query,
                    "inflight_key": inflight_key,
                },
                flush=True,
            )
        
            duplicate_notice_key = (
                f"duplicate_notice:"
                f"{instance_name}:"
                f"{command_key}"
            )
        
            # Avisar como máximo una vez cada 60 segundos.
            if _redis_setnx_ttl(
                duplicate_notice_key,
                60,
            ):
                try:
                    evolution_send_text(
                        group_jid=remote_jid,
                        text=(
                            f"⏳ {requester_label}, esta solicitud "
                            "ya está siendo procesada.\n\n"
                            f"*Dato solicitado:* {_dato_solicitado_text(query or text)}\n\n"
                            "No es necesario volver a enviarla."
                        ),
                        instance_name=instance_name,
                        timeout=(2.5, 8),
                    )
        
                except Exception as duplicate_notice_error:
                    print(
                        "[DUPLICATE NOTICE ERROR]",
                        repr(duplicate_notice_error),
                        flush=True,
                    )
        
            return jsonify({
                "ok": True,
                "ignored": "already_processing",
                "message": (
                    "La misma solicitud ya está siendo procesada."
                ),
            }), 200

        ack_key = f"ack:{instance_name}:{msg_id}"

        if _redis_setnx_ttl(ack_key, 300):
            ack_started_at = time.monotonic()
        
            try:
                evolution_send_ack_fast(
                    group_jid=remote_jid,
                    requester_label=requester_label,
                    instance_name=instance_name,
                )
        
                print(
                    "[WEBHOOK ACK SENT]",
                    {
                        "instance": instance_name,
                        "group": remote_jid,
                        "msg_id": msg_id,
                        "seconds": round(
                            time.monotonic() - ack_started_at,
                            3,
                        ),
                    },
                    flush=True,
                )
        
            except Exception as ack_error:
                print(
                    "[WEBHOOK ACK ERROR]",
                    {
                        "instance": instance_name,
                        "group": remote_jid,
                        "msg_id": msg_id,
                        "error": repr(ack_error),
                        "seconds": round(
                            time.monotonic() - ack_started_at,
                            3,
                        ),
                    },
                    flush=True,
                )

        job_data = {
            "requester_number": requester_number,
            "requester_name": push_name,
            "requester_label": requester_label,
            "group_jid": remote_jid,
            "group_name": group_name,
            "original_text": text,
            "query": query,
            "query_type": parsed.get("type") if not is_media_candidate else msg_type,
            "msg_type": msg_type,
            "media_id": media_id,
            "msg_id": msg_id,
            "mime_type": mime_type,
            "bot_internal_url": BOT_INTERNAL_URL,
            "bot_internal_token": BOT_INTERNAL_TOKEN,
            "evolution_instance": instance_name,
            "request_key": command_key,
            "inflight_key": inflight_key,
            "processed_key": processed_key,
            "processed_ttl_sec": REQUEST_PROCESSED_TTL_SEC,
        }

        rq_job_id = f"group-request:{command_key}"

        try:
            print(
                "[WEBHOOK BEFORE ENQUEUE]",
                {
                    "instance": instance_name,
                    "group": remote_jid,
                    "msg_id": msg_id,
                    "elapsed_seconds": round(
                        time.monotonic() - webhook_started_at,
                        3,
                    ),
                },
                flush=True,
            )
            
            task_queue.enqueue(
                "worker_jobs.process_group_request_job",
                job_data,
                job_id=rq_job_id,
                job_timeout=900,
                result_ttl=0,
                failure_ttl=1200,
            )

            print(
                "[WEBHOOK ENQUEUED]",
                {
                    "instance": instance_name,
                    "group": remote_jid,
                    "msg_id": msg_id,
                    "rq_job_id": rq_job_id,
                    "elapsed_seconds": round(
                        time.monotonic() - webhook_started_at,
                        3,
                    ),
                },
                flush=True,
            )
                        
        except Exception as enqueue_err:
            err_text = str(enqueue_err).lower()
        
            if (
                "already exists" in err_text
                or "already exists in" in err_text
                or "duplicate" in err_text
            ):
                print(
                    "[RQ DUPLICATE JOB BLOCKED]",
                    rq_job_id,
                    repr(enqueue_err),
                    flush=True,
                )
        
                return jsonify({
                    "ok": True,
                    "ignored": "duplicate_job",
                }), 200
        
            # Si encolar realmente falló, libera el bloqueo.
            redis_conn.delete(inflight_key)
            raise

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
    view = _safe(request.args.get("view")).lower() or "day"

    if view == "month":
        rows = _panel_load_month_rows()
        period = "month"

    elif view == "prev_month":
        start_current = _panel_month_start()
        prev_ref = start_current - timedelta(days=1)

        prev_start = _panel_month_start(prev_ref)
        prev_end = _panel_month_end(prev_ref)

        rows = _panel_load_rows_for_days(_daterange_days(prev_start, prev_end))
        period = "prev_month"

    else:
        rows = _panel_load_today_rows()
        period = "day"

    summary = _panel_summary(rows)

    return jsonify({
        "ok": True,
        "period": period,
        "view": view,
        "summary": summary,
        "rows": rows,
    }), 200

@app.post("/panel/disable-cut-group")
def panel_disable_cut_group():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        view = _safe(request.form.get("view")).lower()

        if not group_jid:
            return "group_jid requerido", 400

        disable_cut_group(group_jid)

        if view == "month":
            return redirect("/panel?view=month")
        return redirect("/panel")
    except Exception as e:
        print("panel_disable_cut_group error:", repr(e), flush=True)
        traceback.print_exc()
        return "error desactivando corte del grupo", 500

@app.post("/panel/enable-cut-group")
def panel_enable_cut_group():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        view = _safe(request.form.get("view")).lower()

        if not group_jid:
            return "group_jid requerido", 400

        enable_cut_group(group_jid)

        if view == "month":
            return redirect("/panel?view=month")
        return redirect("/panel")
    except Exception as e:
        print("panel_enable_cut_group error:", repr(e), flush=True)
        traceback.print_exc()
        return "error activando corte del grupo", 500

@app.post("/panel/block-group")
def panel_block_group():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        view = _safe(request.form.get("view")).lower()

        if not group_jid:
            return "group_jid requerido", 400

        block_group(group_jid)

        if view == "month":
            return redirect("/panel?view=month")
        return redirect("/panel")
    except Exception as e:
        print("panel_block_group error:", repr(e), flush=True)
        traceback.print_exc()
        return "error bloqueando grupo", 500

@app.post("/panel/unblock-group")
def panel_unblock_group():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        view = _safe(request.form.get("view")).lower()

        if not group_jid:
            return "group_jid requerido", 400

        unblock_group(group_jid)

        if view == "month":
            return redirect("/panel?view=month")
        return redirect("/panel")
    except Exception as e:
        print("panel_unblock_group error:", repr(e), flush=True)
        traceback.print_exc()
        return "error desbloqueando grupo", 500

@app.post("/panel/set-group-name")
def panel_set_group_name():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        group_name = _safe(request.form.get("group_name"))
        view = _safe(request.form.get("view")).lower()

        if not group_jid:
            return "group_jid requerido", 400

        set_group_alias(group_jid, group_name)

        if view == "month":
            return redirect("/panel?view=month")
        return redirect("/panel")
    except Exception as e:
        print("panel_set_group_name error:", repr(e), flush=True)
        traceback.print_exc()
        return "error guardando nombre del grupo", 500

@app.post("/panel/ping-group")
def panel_ping_group():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        view = _safe(request.form.get("view")).lower()

        if not group_jid:
            return "group_jid requerido", 400

        group_name = resolve_group_name(group_jid)

        evolution_send_text(
            group_jid=group_jid,
            text=(
                "📍 *PING DE GRUPO*\n\n"
                f"Nombre: {group_name}\n"
                f"JID: {group_jid}"
            )
        )

        if view == "month":
            return redirect("/panel?view=month")
        return redirect("/panel")
    except Exception as e:
        print("panel_ping_group error:", repr(e), flush=True)
        traceback.print_exc()
        return "error enviando ping al grupo", 500

@app.get("/panel/api/cuts")
def panel_api_cuts():
    view = _safe(request.args.get("view")).lower() or "day"
    group_jid = _safe(request.args.get("group_jid"))

    days = _period_days(view)

    if group_jid:
        detail = _load_cut_detail_for_group(group_jid, days)
        return jsonify({
            "ok": True,
            "view": view,
            "detail": detail,
        }), 200

    rows = _load_cut_rows_for_days(days)
    summary = _cut_summary(rows)

    return jsonify({
        "ok": True,
        "view": view,
        "summary": summary,
        "rows": rows,
    }), 200

@app.post("/panel/send-daily-cut-group")
def panel_send_daily_cut_group():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        day_str = _safe(request.form.get("day")) or _panel_day_str()

        if not group_jid:
            return "group_jid requerido", 400

        send_daily_cut_for_group(group_jid=group_jid, day_str=day_str)
        return redirect(f"/panel/cuts?view=day&day={day_str}")
    except Exception as e:
        print("panel_send_daily_cut_group error:", repr(e), flush=True)
        traceback.print_exc()
        return "error enviando corte del grupo", 500

@app.post("/panel/update-group-prices")
def panel_update_group_prices():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        view = _safe(request.form.get("view")).lower() or "day"
        day_param = _safe(request.form.get("day"))

        clon = _safe_float(request.form.get("clon"))
        idcif = _safe_float(request.form.get("idcif"))

        if not group_jid:
            return "group_jid requerido", 400

        set_group_prices(group_jid, clon, idcif)

        url = f"/panel/cuts?view={view}&group_jid={group_jid}"
        if day_param:
            url += f"&day={day_param}"

        return redirect(url)

    except Exception as e:
        print("panel_update_group_prices error:", repr(e), flush=True)
        traceback.print_exc()
        return "error actualizando precios", 500

@app.post("/panel/send-daily-cuts")
def panel_send_daily_cuts():
    try:
        day_str = _safe(request.form.get("day")) or _panel_day_str()
        send_daily_cuts(day_str=day_str)
        return redirect(f"/panel/cuts?view=day&day={day_str}")
    except Exception as e:
        print("panel_send_daily_cuts error:", repr(e), flush=True)
        traceback.print_exc()
        return "error enviando cortes", 500

@app.get("/cron/reset")
def reset_cron():
    day = _panel_day_str()

    key_morning = f"cron_sent_morning_image:{day}"
    key_cuts = f"cron_sent_daily_cuts:{day}"

    redis_conn.delete(key_morning)
    redis_conn.delete(key_cuts)

    return {
        "ok": True,
        "deleted": [key_morning, key_cuts]
    }

def evolution_send_image_to_group(group_jid: str, media_url: str, file_name: str = "aviso.jpg", caption: str = "", instance_name=None):
    instance_name = _safe(instance_name) or EVOLUTION_INSTANCE

    url = f"{EVOLUTION_BASE_URL}/message/sendMedia/{instance_name}"
    payload = {
        "number": group_jid,
        "mediatype": "image",
        "media": media_url,
        "fileName": file_name,
        "caption": caption,
    }

    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=120)
    print("sendImage instance:", instance_name, flush=True)
    print("sendImage payload:", payload, flush=True)
    print("sendImage resp:", r.status_code, r.text, flush=True)
    r.raise_for_status()
    return r.json()

def send_image_to_one_group(group_jid: str, image_url: str, file_name: str = "aviso.jpg", caption: str = "", instance_name=None):
    instance_name = _safe(instance_name) or EVOLUTION_INSTANCE

    if not group_jid:
        return {"ok": False, "error": "group_jid vacío"}

    if is_group_blocked(group_jid):
        return {"ok": False, "error": "group_blocked", "group_jid": group_jid}

    try:
        evolution_send_image_to_group(
            group_jid=group_jid,
            media_url=image_url,
            file_name=file_name,
            caption=caption,
            instance_name=instance_name,
        )
        return {
            "ok": True,
            "group_jid": group_jid,
            "group_name": GROUP_NAME_MAP.get(group_jid) or group_jid,
            "instance_name": instance_name,
        }
    except Exception as e:
        print("send_image_to_one_group error:", group_jid, repr(e), flush=True)
        return {
            "ok": False,
            "group_jid": group_jid,
            "group_name": GROUP_NAME_MAP.get(group_jid) or group_jid,
            "error": str(e),
            "instance_name": instance_name,
        }

@app.post("/panel/test-send-warning-and-cut")
def panel_test_send_warning_and_cut():
    try:
        secret = request.headers.get("x-cron-secret", "").strip()
        if PANEL_CRON_SECRET and secret != PANEL_CRON_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        group_jid = _safe(request.args.get("group_jid")) or _safe(request.form.get("group_jid"))
        day_str = _safe(request.args.get("day")) or _safe(request.form.get("day")) or _panel_day_str()

        if not group_jid:
            return jsonify({"ok": False, "error": "group_jid requerido"}), 400

        aviso_img = "https://res.cloudinary.com/dxq7oqiig/image/upload/v1774050692/1000016581_pbs0hc.jpg"

        image_result = send_image_to_one_group(
            group_jid=group_jid,
            image_url=aviso_img,
            file_name="aviso.jpg",
            caption="",
        )

        import time
        time.sleep(2)

        cut_result = send_daily_cut_for_group(group_jid=group_jid, day_str=day_str)

        return jsonify({
            "ok": True,
            "group_jid": group_jid,
            "day": day_str,
            "image_result": image_result,
            "cut_result": cut_result,
        }), 200

    except Exception as e:
        print("panel_test_send_warning_and_cut error:", repr(e), flush=True)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/cron/test-morning-image", methods=["GET","POST"])
def cron_test_morning_image():
    try:
        secret = request.headers.get("x-cron-secret", "").strip()
        if PANEL_CRON_SECRET and secret != PANEL_CRON_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        group_jid = _safe(request.args.get("group_jid"))

        if not group_jid:
            return jsonify({"ok": False, "error": "group_jid requerido"}), 400

        MORNING_IMG = "https://res.cloudinary.com/dxq7oqiig/image/upload/v1774052305/WhatsApp_Image_2026-03-20_at_7.18.07_PM_byfou8.jpg"

        evolution_send_image_to_group(
            group_jid=group_jid,
            media_url=MORNING_IMG,
            file_name="inicio.jpg",
        )

        return jsonify({
            "ok": True,
            "group_jid": group_jid
        }), 200

    except Exception as e:
        print("cron_test_morning_image error:", repr(e), flush=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/panel/test-send-warning-image")
def panel_test_send_warning_image():
    try:
        secret = request.headers.get("x-cron-secret", "").strip()
        if PANEL_CRON_SECRET and secret != PANEL_CRON_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        group_jid = _safe(request.args.get("group_jid")) or _safe(request.form.get("group_jid"))
        if not group_jid:
            return jsonify({"ok": False, "error": "group_jid requerido"}), 400

        aviso_img = "https://res.cloudinary.com/dxq7oqiig/image/upload/v1774050692/1000016581_pbs0hc.jpg"

        result = send_image_to_one_group(
            group_jid=group_jid,
            image_url=aviso_img,
            file_name="aviso.jpg",
            caption="",
        )

        return jsonify(result), 200

    except Exception as e:
        print("panel_test_send_warning_image error:", repr(e), flush=True)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

def send_image_to_all_groups(image_url: str, file_name: str = "aviso.jpg", caption: str = "", instance_name=None):
    instance_name = _safe(instance_name) or EVOLUTION_INSTANCE

    sent = []
    failed = []

    target_groups = set(GROUP_NAME_MAP.keys())

    if ALLOWED_GROUPS:
        target_groups = target_groups.intersection(ALLOWED_GROUPS)

    for group_jid in sorted(target_groups):
        if is_group_blocked(group_jid):
            continue

        try:
            evolution_send_image_to_group(
                group_jid=group_jid,
                media_url=image_url,
                file_name=file_name,
                caption=caption,
                instance_name=instance_name,
            )
            sent.append(group_jid)
        except Exception as e:
            print("error enviando imagen a grupo:", group_jid, repr(e), flush=True)
            failed.append({
                "group_jid": group_jid,
                "error": str(e),
            })

    return {
        "sent": sent,
        "failed": failed,
        "instance_name": instance_name,
    }

@app.post("/cron/send-morning-image")
def cron_send_morning_image():
    try:
        secret = request.headers.get("x-cron-secret", "").strip()
        if PANEL_CRON_SECRET and secret != PANEL_CRON_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        day_str = _panel_day_str()
        lock_key = f"cron_sent_morning_image:{day_str}"

        # evitar duplicados
        if redis_conn.get(lock_key):
            return jsonify({
                "ok": True,
                "skipped": "already_sent",
                "day": day_str,
            }), 200

        MORNING_IMG = "https://res.cloudinary.com/dxq7oqiig/image/upload/v1774052305/WhatsApp_Image_2026-03-20_at_7.18.07_PM_byfou8.jpg"

        result = send_image_to_all_groups(
            image_url=MORNING_IMG,
            file_name="inicio.jpg",
            caption="🚀 Buen día, ya pueden enviar sus solicitudes"
        )

        if result.get("sent"):
            redis_conn.set(lock_key, "1", ex=60 * 60 * 24)

        return jsonify({
            "ok": True,
            "day": day_str,
            "sent": result.get("sent"),
            "failed": result.get("failed"),
        }), 200

    except Exception as e:
        print("cron_send_morning_image error:", repr(e), flush=True)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/cron/send-daily-cuts")
def cron_send_daily_cuts():
    try:
        secret = request.headers.get("x-cron-secret", "").strip()
        if PANEL_CRON_SECRET and secret != PANEL_CRON_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        day_str = _safe(request.args.get("day")) or _panel_day_str()
        lock_key = f"cron_sent_daily_cuts:{day_str}"

        if redis_conn.get(lock_key):
            return jsonify({
                "ok": True,
                "skipped": "already_sent",
                "day": day_str,
            }), 200

        AVISO_IMG = "https://res.cloudinary.com/dxq7oqiig/image/upload/v1774050692/1000016581_pbs0hc.jpg"

        image_result = send_image_to_all_groups(
            image_url=AVISO_IMG,
            file_name="aviso.jpg",
            caption=""
        )

        import time
        time.sleep(2)

        result = send_daily_cuts(day_str=day_str)

        sent = result.get("sent") or []

        # bloquea el día si mandó imagen o si mandó cortes
        if image_result.get("sent") or sent:
            redis_conn.set(lock_key, "1", ex=60 * 60 * 24)

        return jsonify({
            "ok": True,
            "day": day_str,
            "image_sent": image_result.get("sent") or [],
            "image_failed": image_result.get("failed") or [],
            "cuts_sent": sent,
            "cuts_skipped": result.get("skipped") or [],
        }), 200

    except Exception as e:
        print("cron_send_daily_cuts error:", repr(e), flush=True)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/panel/cuts")
def panel_cuts():
    view = _safe(request.args.get("view")).lower() or "day"
    group_jid = _safe(request.args.get("group_jid"))
    day_param = _safe(request.args.get("day"))
    search = _safe(request.args.get("search")).lower()

    if day_param:
        days = [day_param]
        subtitle = f"Corte diario: {_period_day_label_es(day_param)} ({PANEL_TZ})"
        view = "day"
    else:
        days = _period_days(view)

        if view == "month":
            subtitle = f"Historial mes actual: {days[0]} a {days[-1]} ({PANEL_TZ})"

        elif view == "prev_month":
            end_label = (
                datetime.strptime(days[-1], "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d") if days else ""

            subtitle = f"Historial mes anterior: {days[0]} a {end_label} ({PANEL_TZ})"

        else:
            subtitle = f"Corte diario: {_today_label_es()} ({PANEL_TZ})"

    def esc(v):
        if v is None:
            return ""
        return str(v)

    if group_jid:
        detail = _load_cut_detail_for_group(group_jid, days)
        totals = detail["totals"]

        price_form = f"""
            <div style="background:white; border-radius:18px; box-shadow:0 8px 24px rgba(15,23,42,.08); padding:16px; margin-bottom:16px;">
              <form method="post" action="/panel/update-group-prices" style="display:flex; gap:12px; flex-wrap:wrap; align-items:end;">
                <input type="hidden" name="group_jid" value="{esc(detail["group_jid"])}">
                <input type="hidden" name="view" value="{esc(view)}">
                <input type="hidden" name="day" value="{esc(day_param)}">
        
                <div>
                  <label style="display:block; margin-bottom:6px; font-weight:700;">Precio clon</label>
                  <input type="number" step="0.01" min="0" name="clon" value="{esc(detail["price_clon"])}"
                         style="padding:10px 12px; border:1px solid #cbd5e1; border-radius:10px;">
                </div>
        
                <div>
                  <label style="display:block; margin-bottom:6px; font-weight:700;">Precio idcif</label>
                  <input type="number" step="0.01" min="0" name="idcif" value="{esc(detail["price_idcif"])}"
                         style="padding:10px 12px; border:1px solid #cbd5e1; border-radius:10px;">
                </div>
        
                <div>
                  <button type="submit"
                          style="border:none; border-radius:10px; padding:10px 14px; font-weight:700; cursor:pointer; background:#2563eb; color:white;">
                    Guardar precios
                  </button>
                </div>
              </form>
            </div>
        """

        html = f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Detalle de cortes</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ font-family: Arial, sans-serif; background:#f5f7fb; margin:0; padding:16px; color:#0f172a; }}
    .wrap {{ max-width:1200px; margin:0 auto; }}
    .hero {{ background:#0f172a; color:white; padding:20px; border-radius:18px; margin-bottom:16px; }}
    .hero a {{ color:#93c5fd; text-decoration:none; font-weight:700; }}
    .box {{ background:white; border-radius:18px; box-shadow:0 8px 24px rgba(15,23,42,.08); overflow:hidden; }}
    table {{ width:100%; border-collapse:collapse; }}
    th, td {{ padding:12px; border-bottom:1px solid #e2e8f0; text-align:left; }}
    th {{ background:#0f172a; color:white; }}
    .right {{ text-align:right; }}
    .total-row td {{ font-weight:700; background:#f8fafc; }}
    .actions-cell {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }}
    
    .tool-link-solid {{
      background: #2563eb !important;
      color: #fff !important;
      border: none !important;
    }}
    
    .inline-form {{
      margin: 0;
    }}
    
    @media (max-width: 900px) {{
      .actions-cell {{
        flex-direction: column;
        align-items: stretch;
      }}
    
      .actions-cell .tool-link,
      .actions-cell .btn {{
        width: 100%;
        text-align: center;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div><a href="/panel/cuts?view={esc(view)}">← Volver al historial</a></div>
      <h2 style="margin:10px 0 6px;">{esc(detail["group_name"])}</h2>
      <div>{esc(subtitle)}</div>
    </div>
    
    {price_form}
    
    <div class="box">
      <table>
        <thead>
          <tr>
            <th>Día</th>
            <th>Fecha</th>
            <th class="right">RFC clon</th>
            <th class="right">RFC idcif</th>
            <th class="right">Precio clon</th>
            <th class="right">Precio idcif</th>
            <th class="right">$ clon</th>
            <th class="right">$ idcif</th>
            <th class="right">Total</th>
          </tr>
        </thead>
        <tbody>
        """

        for r in detail["rows"]:
            if r.get("is_cut"):
                html += f"""
                  <tr style="background:#dbeafe; font-weight:700;">
                    <td>{esc(r["label"])}</td>
                    <td>{esc(r["from_date"])} a {esc(r["to_date"])}</td>
                    <td class="right">{esc(r["week_clon"])}</td>
                    <td class="right">{esc(r["week_idcif"])}</td>
                    <td class="right">${esc(_fmt_money(r["price_clon"]))}</td>
                    <td class="right">${esc(_fmt_money(r["price_idcif"]))}</td>
                    <td class="right">${esc(_fmt_money(r["week_sub_clon"]))}</td>
                    <td class="right">${esc(_fmt_money(r["week_sub_idcif"]))}</td>
                    <td class="right">${esc(_fmt_money(r["week_total"]))}</td>
                  </tr>
                """
            else:
                html += f"""
                  <tr>
                    <td>{esc(r["day_name"])}</td>
                    <td>{esc(r["date"])}</td>
                    <td class="right">{esc(r["count_clon"])}</td>
                    <td class="right">{esc(r["count_idcif"])}</td>
                    <td class="right">${esc(_fmt_money(r["price_clon"]))}</td>
                    <td class="right">${esc(_fmt_money(r["price_idcif"]))}</td>
                    <td class="right">${esc(_fmt_money(r["subtotal_clon"]))}</td>
                    <td class="right">${esc(_fmt_money(r["subtotal_idcif"]))}</td>
                    <td class="right">${esc(_fmt_money(r["total"]))}</td>
                  </tr>
                """

        html += f"""
          <tr class="total-row">
            <td colspan="2">TOTAL</td>
            <td class="right">{esc(totals["count_clon"])}</td>
            <td class="right">{esc(totals["count_idcif"])}</td>
            <td></td>
            <td></td>
            <td class="right">${esc(_fmt_money(totals["subtotal_clon"]))}</td>
            <td class="right">${esc(_fmt_money(totals["subtotal_idcif"]))}</td>
            <td class="right">${esc(_fmt_money(totals["total"]))}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</body>
</html>
        """
        return Response(html, mimetype="text/html")

    rows = _load_cut_rows_for_days(days)
    summary = _cut_summary(rows)

    html = f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Historial de cortes</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ font-family: Arial, sans-serif; background:#f5f7fb; margin:0; padding:16px; color:#0f172a; }}
    .wrap {{ max-width:1400px; margin:0 auto; }}
    .hero {{ background:linear-gradient(135deg,#0f172a 0%, #1e293b 55%, #2563eb 100%); color:white; padding:22px; border-radius:20px; margin-bottom:16px; }}
    .toolbar {{ margin-top:12px; display:flex; gap:10px; flex-wrap:wrap; }}
    .tool-link {{ text-decoration:none; padding:10px 14px; border-radius:10px; background:rgba(255,255,255,.16); color:white; font-weight:700; }}
    .tool-link-active {{ background:white; color:#0f172a; }}
    .cards {{ display:grid; grid-template-columns:repeat(6,minmax(0,1fr)); gap:12px; margin-bottom:16px; }}
    .card {{ background:white; border-radius:16px; padding:16px; box-shadow:0 8px 24px rgba(15,23,42,.08); }}
    .label {{ color:#64748b; font-size:.9rem; margin-bottom:8px; }}
    .value {{ font-size:1.8rem; font-weight:800; }}
    .box {{ background:white; border-radius:18px; box-shadow:0 8px 24px rgba(15,23,42,.08); overflow:hidden; }}
    .head {{ display:flex; align-items:center; justify-content:space-between; padding:16px 18px; border-bottom:1px solid #e2e8f0; gap:12px; flex-wrap:wrap; }}
    .head h3 {{ margin:0; }}
    table {{ width:100%; border-collapse:collapse; }}
    th, td {{ padding:12px; border-bottom:1px solid #e2e8f0; text-align:left; }}
    th {{ background:#0f172a; color:white; }}
    .right {{ text-align:right; }}
    .btn {{
      border: none;
      border-radius: 8px;
      padding: 6px 10px;
      font-weight: 600;
      cursor: pointer;
      font-size: .78rem;
      line-height: 1;
      white-space: nowrap;
      transition: all .15s ease;
      box-shadow: 0 1px 2px rgba(0,0,0,.05);
    }}
    .btn-save {{
      background: #0ea5a4;
      color: white;
    }}
    .btn-green {{ background:#16a34a; }}
    .inline-form {{ margin:0; display:inline-block; }}

    .actions-cell {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }}

    .tool-link-solid {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 88px;
      padding: 9px 12px;
      border-radius: 10px;
      text-decoration: none;
      font-weight: 700;
      box-sizing: border-box;
      background: #2563eb !important;
      color: #fff !important;
      border: none !important;
    }}

    @media (max-width: 900px) {{
      .cards {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
      .table-wrap {{ overflow-x:auto; }}

      .actions-cell {{
        flex-direction: column;
        align-items: stretch;
      }}

      .actions-cell .tool-link-solid,
      .actions-cell .btn {{
        width: 100%;
        text-align: center;
      }}
    }}

    @media (max-width: 520px) {{
      .cards {{ grid-template-columns:1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1 style="margin:0 0 8px;">Historial de cortes</h1>
      <div>{esc(subtitle)}</div>

      <div class="toolbar">
        <form method="get" style="display:flex; gap:8px; align-items:center;">
          <input type="hidden" name="view" value="{esc(view)}">
          <input
            type="text"
            name="search"
            placeholder="Buscar grupo..."
            value="{esc(search)}"
            style="padding:8px 10px;border-radius:8px;border:1px solid #cbd5e1;"
          >
          <button class="btn btn-save" type="submit">Buscar</button>
        </form>
    
        <a href="/panel" class="tool-link">Panel</a>
        <a href="/panel/cuts?view=day" class="tool-link {'tool-link-active' if view == 'day' else ''}">Hoy</a>
        <a href="/panel/cuts?view=month" class="tool-link {'tool-link-active' if view == 'month' else ''}">Mes actual</a>
        <a href="/panel/cuts?view=prev_month" class="tool-link {'tool-link-active' if view == 'prev_month' else ''}">Mes anterior</a>
      </div>
    </div>

    <div class="cards">
      <div class="card">
        <div class="label">Grupos con actividad</div>
        <div class="value">{esc(summary["groups"])}</div>
      </div>
      <div class="card">
        <div class="label">RFC clon</div>
        <div class="value">{esc(summary["count_clon"])}</div>
      </div>
      <div class="card">
        <div class="label">RFC idcif</div>
        <div class="value">{esc(summary["count_idcif"])}</div>
      </div>
      <div class="card">
        <div class="label">$ clon</div>
        <div class="value">${esc(_fmt_money(summary["subtotal_clon"]))}</div>
      </div>
      <div class="card">
        <div class="label">$ idcif</div>
        <div class="value">${esc(_fmt_money(summary["subtotal_idcif"]))}</div>
      </div>
      <div class="card">
        <div class="label">Total</div>
        <div class="value">${esc(_fmt_money(summary["total"]))}</div>
      </div>
    </div>

    <div class="box">
      <div class="head">
        <h3>Resumen por grupo</h3>
        {"<form class='inline-form' method='post' action='/panel/send-daily-cuts'><input type='hidden' name='day' value='" + esc(_panel_day_str()) + "'><button class='btn btn-green' type='submit'>Enviar cortes de hoy</button></form>" if view == "day" else ""}
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Grupo</th>
              <th class="right">RFC clon</th>
              <th class="right">RFC idcif</th>
              <th class="right">Precio clon</th>
              <th class="right">Precio idcif</th>
              <th class="right">$ clon</th>
              <th class="right">$ idcif</th>
              <th class="right">Total</th>
              <th>Acciones</th>
            </tr>
          </thead>
          <tbody>
    """

    if rows:        
        for r in rows:
        
            if search:
                name = (r.get("group_name") or "").lower()
                jid = (r.get("group_jid") or "").lower()
            
                if search not in name and search not in jid:
                    continue
            html += f"""
            <tr>
              <td>{esc(r["group_name"])}<br><span style="color:#64748b; font-size:.82rem;">{esc(r["group_jid"])}</span></td>
              <td class="right">{esc(r["count_clon"])}</td>
              <td class="right">{esc(r["count_idcif"])}</td>
              <td class="right">${esc(_fmt_money(r["price_clon"]))}</td>
              <td class="right">${esc(_fmt_money(r["price_idcif"]))}</td>
              <td class="right">${esc(_fmt_money(r["subtotal_clon"]))}</td>
              <td class="right">${esc(_fmt_money(r["subtotal_idcif"]))}</td>
              <td class="right">${esc(_fmt_money(r["total"]))}</td>
              <td>
                <div class="actions-cell">
                  <a class="tool-link tool-link-solid" href="/panel/cuts?view={esc(view)}&group_jid={esc(r["group_jid"])}">Detalle</a>
                  {"<form class='inline-form' method='post' action='/panel/send-daily-cut-group'><input type='hidden' name='group_jid' value='" + esc(r["group_jid"]) + "'><input type='hidden' name='day' value='" + esc(_panel_day_str()) + "'><button class='btn btn-green' type='submit'>Enviar</button></form>" if view == "day" else ""}
                </div>
              </td>
            </tr>
            """
    else:
        html += """
            <tr>
              <td colspan="9" style="text-align:center; color:#64748b; padding:24px;">Sin actividad en este periodo.</td>
            </tr>
        """

    html += """
          </tbody>
        </table>
      </div>
    </div>
  </div>
</body>
</html>
    """

    return Response(html, mimetype="text/html")

@app.get("/panel")
def panel_stats():
    view = _safe(request.args.get("view")).lower() or "day"
    search = _safe(request.args.get("search")).lower()

    if view == "month":
        rows = _panel_load_month_rows()
        title_period = "Mes actual"
        month_start = _panel_month_start().strftime("%Y-%m-%d")
        month_end = _panel_month_end().strftime("%Y-%m-%d")
        subtitle = f"Corte mensual: {month_start} a {month_end} ({PANEL_TZ})"
        auto_reload = "false"
        section_note = "Vista mensual"

    elif view == "prev_month":
        start_current = _panel_month_start()
        prev_ref = start_current - timedelta(days=1)

        prev_start = _panel_month_start(prev_ref)
        prev_end = _panel_month_end(prev_ref)

        rows = _panel_load_rows_for_days(_daterange_days(prev_start, prev_end))
        title_period = "Mes anterior"
        subtitle = f"Corte mes anterior: {prev_start.strftime('%Y-%m-%d')} a {prev_end.strftime('%Y-%m-%d')} ({PANEL_TZ})"
        auto_reload = "false"
        section_note = "Vista mes anterior"

    else:
        rows = _panel_load_today_rows()
        title_period = "Hoy"
        subtitle = f"Corte diario automático: {_panel_day_str()} (reinicio lógico a las 00:00:00, {PANEL_TZ})"
        auto_reload = "true"
        section_note = "Actualización automática cada 1 minuto"

    summary = _panel_summary(rows)

    def esc(v):
        if v is None:
            return ""
        return str(v)

    no_corte_groups = get_no_corte_groups()
    
    html = f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Panel RFC</title>
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

    .status-pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 6px 10px;
      border-radius: 999px;
      font-weight: 700;
      font-size: .82rem;
    }}
    
    .status-active {{
      background: #ecfdf5;
      color: #15803d;
    }}
    
    .status-blocked {{
      background: #fef2f2;
      color: #b91c1c;
    }}
    
    .action-form {{
      margin: 0;
    }}
    
    .btn {{
      border: none;
      border-radius: 10px;
      padding: 9px 12px;
      font-weight: 700;
      cursor: pointer;
      font-size: .85rem;
    }}
    
    .btn-block {{
      background: #dc2626;
      color: white;
    }}
    
    .btn-unblock {{
      background: #16a34a;
      color: white;
    }}
    
    .btn:hover {{
      opacity: .92;
    }}

    .toolbar {{
      margin-top: 14px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      position: relative;
      z-index: 1;
    }}
    
    .tool-link {{
      text-decoration: none;
      padding: 10px 14px;
      border-radius: 10px;
      background: rgba(255,255,255,.14);
      color: white;
      font-weight: 700;
      border: 1px solid rgba(255,255,255,.18);
    }}
    
    .tool-link:hover {{
      background: rgba(255,255,255,.22);
    }}
    
    .tool-link-active {{
      background: white;
      color: #0f172a;
    }}

    .status-cut-on {{
      background: #ecfdf5;
      color: #15803d;
    }}
    
    .status-cut-off {{
      background: #fff7ed;
      color: #c2410c;
    }}
    
    .btn-cut-on {{
      background: #2563eb;
      color: white;
    }}
    
    .btn-cut-off {{
      background: #ea580c;
      color: white;
    }}
    
    .actions-stack {{
      display: flex;
      flex-direction: column;
      gap: 10px;
      min-width: 240px;
    }}
    
    .actions-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }}
    
    .action-form {{
      margin: 0;
    }}
    
    .rename-form {{
      display: flex;
      gap: 6px;
      align-items: center;
    }}
    
    .rename-form input[type="text"] {{
      flex: 1;
      min-width: 0;
      padding: 7px 9px;
      border: 1px solid var(--line);
      border-radius: 8px;
      font-size: .82rem;
      background: #fff;
      color: var(--text);
    }}
    
    .btn {{
      border: none;
      border-radius: 8px;
      padding: 6px 10px;
      font-weight: 600;
      cursor: pointer;
      font-size: .78rem;
      line-height: 1;
      white-space: nowrap;
      transition: all .15s ease;
      box-shadow: 0 1px 2px rgba(0,0,0,.05);
    }}
    
    .btn:hover {{
      transform: translateY(-1px);
      opacity: .95;
    }}
    
    .btn-block {{
      background: #ef4444;
      color: white;
    }}
    
    .btn-unblock {{
      background: #16a34a;
      color: white;
    }}
    
    .btn-cut-on {{
      background: #2563eb;
      color: white;
    }}
    
    .btn-cut-off {{
      background: #f97316;
      color: white;
    }}
    
    .btn-ping {{
      background: #6366f1;
      color: white;
    }}
    
    .btn-save {{
      background: #0ea5a4;
      color: white;
    }}
    
    @media (max-width: 720px) {{
      .actions-stack {{
        min-width: 0;
      }}
    
      .actions-row,
      .rename-form {{
        flex-direction: column;
      }}
    
      .btn,
      .rename-form input[type="text"] {{
        width: 100%;
      }}
    }}
    
    @media (max-width: 720px) {{
      .btn {{
        width: 100%;
      }}
    
      .action-form input[type="text"] {{
        min-width: 0;
        width: 100%;
      }}
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
    if ({auto_reload}) {{
      setTimeout(() => location.reload(), 60000);
    }}
  </script>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Panel de RFC</h1>
      <p class="sub">
        {subtitle}
      </p>
      <div class="toolbar">
        <form method="get" style="display:flex; gap:8px; align-items:center;">
          <input type="hidden" name="view" value="{esc(view)}">
          <input
            type="text"
            name="search"
            placeholder="Buscar grupo..."
            value="{esc(search)}"
            style="padding:8px 10px;border-radius:8px;border:1px solid #cbd5e1;"
          >
          <button class="btn btn-save" type="submit">Buscar</button>
        </form>
        <a href="/panel?view=day" class="tool-link {'tool-link-active' if view == 'day' else ''}">Hoy</a>
        <a href="/panel?view=month" class="tool-link {'tool-link-active' if view == 'month' else ''}">Mes actual</a>
        <a href="/panel?view=prev_month" class="tool-link {'tool-link-active' if view == 'prev_month' else ''}">Mes anterior</a>
        <a href="/panel/cuts?view=day" class="tool-link">Historial cortes</a>
      </div>
    </section>

    <section class="cards">
      <div class="card">
        <div class="label">Total exitosos {title_period.lower()}</div>
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

    <div class="section" style="margin-bottom:18px;">
      <div class="section-head">
        <h2 class="section-title">Agregar grupo manualmente</h2>
        <div class="section-note">Registra un grupo para este bot RFC</div>
      </div>
    
      <div style="padding:16px;">
        <div style="display:grid;grid-template-columns:1.4fr 1fr auto;gap:12px;align-items:end;">
          <div>
            <div class="small" style="margin-bottom:6px;">Group JID</div>
            <input id="manual_group_jid" placeholder="1203634XXXXXXXXXX@g.us"
              style="width:100%;padding:9px 10px;border-radius:8px;border:1px solid #cbd5e1;">
          </div>
    
          <div>
            <div class="small" style="margin-bottom:6px;">Nombre del grupo</div>
            <input id="manual_group_name" placeholder="Nombre visible del grupo"
              style="width:100%;padding:9px 10px;border-radius:8px;border:1px solid #cbd5e1;">
          </div>
    
          <div>
            <button class="btn btn-save" type="button" onclick="addManualRfcGroup()">
              Agregar grupo
            </button>
          </div>
        </div>
      </div>
    </div>
    
    <section class="section">
      <div class="section-head">
        <h2 class="section-title">Actividad por grupo</h2>
        <div class="section-note">{section_note}</div>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Grupo</th>
              <th>Estado</th>
              <th>Corte</th>
              <th class="right">Total</th>
              <th class="right">RFC_IDCIF</th>
              <th class="right">QR</th>
              <th class="right">CURP</th>
              <th class="right">RFC_solo</th>
              <th style="width:150px">Actualizado</th>
              <th style="width:260px">Acciones</th>
            </tr>
          </thead>
          <tbody>
    """

    if rows:
        for r in rows:
    
            if search:
                name = (r.get("group_name") or "").lower()
                jid = (r.get("group_jid") or "").lower()
    
                if search not in name and search not in jid:
                    continue
                    
            group_jid = esc(r["group_jid"])
            blocked = bool(r.get("blocked"))
            no_corte = r["group_jid"] in no_corte_groups

            status_html = (
                '<span class="status-pill status-blocked">BLOQUEADO</span>'
                if blocked else
                '<span class="status-pill status-active">ACTIVO</span>'
            )

            cut_html = (
                '<span class="status-pill status-cut-off">SIN CORTE</span>'
                if no_corte else
                '<span class="status-pill status-cut-on">CON CORTE</span>'
            )

            if blocked:
                block_action_html = f"""
                <form class="action-form" method="post" action="/panel/unblock-group">
                  <input type="hidden" name="group_jid" value="{group_jid}">
                  <input type="hidden" name="view" value="{esc(view)}">
                  <button class="btn btn-unblock" type="submit">Desbloquear</button>
                </form>
                """
            else:
                block_action_html = f"""
                <form class="action-form" method="post" action="/panel/block-group">
                  <input type="hidden" name="group_jid" value="{group_jid}">
                  <input type="hidden" name="view" value="{esc(view)}">
                  <button class="btn btn-block" type="submit">Bloquear</button>
                </form>
                """

            if no_corte:
                cut_action_html = f"""
                <form class="action-form" method="post" action="/panel/enable-cut-group">
                  <input type="hidden" name="group_jid" value="{group_jid}">
                  <input type="hidden" name="view" value="{esc(view)}">
                  <button class="btn btn-cut-on" type="submit">Activar corte</button>
                </form>
                """
            else:
                cut_action_html = f"""
                <form class="action-form" method="post" action="/panel/disable-cut-group">
                  <input type="hidden" name="group_jid" value="{group_jid}">
                  <input type="hidden" name="view" value="{esc(view)}">
                  <button class="btn btn-cut-off" type="submit">Quitar corte</button>
                </form>
                """

            ping_action_html = f"""
            <form class="action-form" method="post" action="/panel/ping-group">
              <input type="hidden" name="group_jid" value="{group_jid}">
              <input type="hidden" name="view" value="{esc(view)}">
              <button class="btn btn-ping" type="submit">Ping grupo</button>
            </form>
            """

            rename_action_html = f"""
            <form class="action-form rename-form" method="post" action="/panel/set-group-name">
              <input type="hidden" name="group_jid" value="{group_jid}">
              <input type="hidden" name="view" value="{esc(view)}">
              <input type="text" name="group_name" value="{esc(r["group_name"])}" placeholder="Nombre">
              <button class="btn btn-save" type="submit">Guardar</button>
            </form>
            """
            
            action_html = f"""
            <div class="actions-stack">
              <div class="actions-row">
                {ping_action_html}
                {block_action_html}
                {cut_action_html}
              </div>
              {rename_action_html}
            </div>
            """

            html += f"""
                <tr>
                  <td data-label="Grupo">
                    <div class="group-name">{esc(r["group_name"])}</div>
                    <div class="group-id">{group_jid}</div>
                  </td>
                  <td data-label="Estado">{status_html}</td>
                  <td data-label="Corte">{cut_html}</td>
                  <td data-label="Total" class="right"><span class="badge total-badge">{esc(r["total"])}</span></td>
                  <td data-label="RFC_IDCIF" class="right"><span class="badge">{esc(r["ok_rfc_idcif"])}</span></td>
                  <td data-label="QR" class="right"><span class="badge">{esc(r["ok_qr"])}</span></td>
                  <td data-label="CURP" class="right"><span class="badge">{esc(r["ok_curp"])}</span></td>
                  <td data-label="RFC_solo" class="right"><span class="badge">{esc(r["ok_rfc_only"])}</span></td>
                  <td data-label="Actualizado"><span class="muted">{esc(r["updated_at"])}</span></td>
                  <td data-label="Acciones">{action_html}</td>
                </tr>
            """
    else:
        html += """
            <tr>
              <td colspan="10" class="empty">Sin actividad en este periodo.</td>
            </tr>
        """

    html += """
          </tbody>
        </table>
      </div>
    </section>
  </div>

<script>
    async function addManualRfcGroup() {
      const groupJid = document.getElementById("manual_group_jid").value.trim();
      const groupName = document.getElementById("manual_group_name").value.trim();
    
      if (!groupJid) {
        alert("Falta el Group JID");
        return;
      }
    
      const formData = new FormData();
      formData.append("group_jid", groupJid);
      formData.append("group_name", groupName);
    
      const res = await fetch("/panel/add-manual-group", {
        method: "POST",
        body: formData
      });
    
      const data = await res.json();
    
      if (data.ok) {
        alert("Grupo agregado correctamente");
        location.reload();
      } else {
        alert(data.error || "No se pudo agregar el grupo");
      }
    }
</script>
</body>
</html>
    """
    return Response(html, mimetype="text/html")

@app.post("/panel/add-manual-group")
def panel_add_manual_group():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        group_name = _safe(request.form.get("group_name"))

        if not group_jid:
            return jsonify({"ok": False, "error": "Falta group_jid"}), 400

        if not group_jid.endswith("@g.us"):
            return jsonify({"ok": False, "error": "El JID debe terminar en @g.us"}), 400

        add_allowed_group(group_jid)

        if group_name:
            set_group_alias(group_jid, group_name)

        return jsonify({
            "ok": True,
            "group_jid": group_jid,
            "group_name": group_name
        })

    except Exception as e:
        print("panel_add_manual_group error:", repr(e), flush=True)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
