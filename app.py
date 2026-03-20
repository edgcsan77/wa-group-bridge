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
from datetime import datetime, timedelta
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

GROUP_NAME_MAP = {
    "120363407784035104@g.us": "VALLARTA",
    "120363424161924716@g.us": "DANIEL",
    "120363406744080404@g.us": "RUBI",
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
    "120363424256034923@g.us": "MAX 1",
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
    "120363408231220228@g.us": "MAX 2",
    "120363424846520578@g.us": "MAX 3",
    "120363404313875699@g.us": "MAX 4",
    "120363424117651122@g.us": "MAX 5",
    "120363425605880699@g.us": "MAX 6",
    "120363410011580719@g.us": "RODOLFO",
}

REDIS_URL = os.getenv("REDIS_URL", "").strip()
BOT_INTERNAL_URL = os.getenv("BOT_INTERNAL_URL", "").strip()
BOT_INTERNAL_TOKEN = os.getenv("BOT_INTERNAL_TOKEN", "").strip()

redis_conn = Redis.from_url(REDIS_URL)
task_queue = Queue("constancia_jobs", connection=redis_conn)

# =========================
# CORTES / PRECIOS / CRON
# =========================
PANEL_CRON_SECRET = os.getenv("PANEL_CRON_SECRET", "").strip()

BENEFICIARIO_CORTE = "JUAN DE DIOS MESINO MANZANO"
CLABE_CORTE = "63818001018336874"
ENTIDAD_CORTE = "Nu México"

# Precio por defecto si el grupo no está en el mapa
DEFAULT_PRICES = {
    "clon": 5.00,
    "idcif": 5.00,
}

# PERSONALIZA AQUÍ LOS PRECIOS POR GRUPO
GROUP_PRICES = {
    "120363425323721713@g.us": {"clon": 5.00, "idcif": 5.00},   # PRUEBA
}

# =========================
# BLOQUEO DE GRUPOS
# =========================
BLOCKED_GROUPS_KEY = "blocked_groups"

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

def _normalize_phone(v: str) -> str:
    return re.sub(r"\D+", "", v or "")

def _clean_spaces(text: str) -> str:
    text = (text or "").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()

def _normalize_upper(text: str) -> str:
    return _clean_spaces((text or "").upper())

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

        if (_looks_like_rfc(first) or re.fullmatch(rfc_pattern, first)) and _looks_like_idcif(second):
            return True

        if (_looks_like_curp(first) or re.fullmatch(curp_pattern, first)):
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
                            "⚠️ IDCIF inválido.\n"
                            "Debe contener únicamente 11 dígitos.\n"
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
                    "⚠️ CURP inválida.\n"
                    "Debe tener 18 caracteres con formato correcto.\n"
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
                    "⚠️ RFC inválido.\n"
                    "Persona física: 13 caracteres.\n"
                    "Persona moral: 12 caracteres.\n"
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
                    "⚠️ IDCIF inválido.\n"
                    "Debe contener únicamente 11 dígitos.\n"
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
                "⚠️ CURP inválida.\n"
                "Debe tener 18 caracteres con formato correcto.\n"
            )
        }

    # RFC sospechoso + posible IDCIF
    if maybe_rfc and not re.fullmatch(rfc_pattern, maybe_rfc):
        return {
            "ok": False,
            "type": "invalid_rfc",
            "query": None,
            "error": (
                "⚠️ RFC inválido.\n"
                "Persona física: 13 caracteres.\n"
                "Persona moral: 12 caracteres.\n"
            )
        }

    # RFC válido + IDCIF sospechoso inválido
    if maybe_rfc and re.fullmatch(rfc_pattern, maybe_rfc) and maybe_idcif and not re.fullmatch(idcif_pattern, maybe_idcif):
        return {
            "ok": False,
            "type": "invalid_idcif",
            "query": None,
            "error": (
                "⚠️ IDCIF inválido.\n"
                "Debe contener únicamente 11 dígitos.\n"
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
                    "⚠️ RFC inválido.\n"
                    "Persona física: 13 caracteres.\n"
                    "Persona moral: 12 caracteres.\n"
                )
            }

        if _looks_like_curp(line) and not re.fullmatch(curp_pattern, line):
            return {
                "ok": False,
                "type": "invalid_curp",
                "query": None,
                "error": (
                    "⚠️ CURP inválida.\n"
                    "Debe tener 18 caracteres con formato correcto.\n"
                )
            }

        if _looks_like_idcif(line) and not re.fullmatch(idcif_pattern, line):
            return {
                "ok": False,
                "type": "invalid_idcif",
                "query": None,
                "error": (
                    "⚠️ IDCIF inválido.\n"
                    "Debe contener únicamente 11 dígitos.\n"
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
            group_name = GROUP_NAME_MAP.get(group_jid) or raw.get("group_name") or group_jid

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

    # incluir bloqueados aunque no estén en mapa ni stats
    for group_jid in blocked:
        if group_jid not in rows_map:
            rows_map[group_jid] = {
                "group_jid": group_jid,
                "group_name": GROUP_NAME_MAP.get(group_jid) or group_jid,
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

def _get_group_prices(group_jid: str):
    p = GROUP_PRICES.get(group_jid) or DEFAULT_PRICES
    return {
        "clon": _safe_float(p.get("clon"), DEFAULT_PRICES["clon"]),
        "idcif": _safe_float(p.get("idcif"), DEFAULT_PRICES["idcif"]),
    }

def _cut_stats_key_for_day(day: str, group_jid: str) -> str:
    return f"cut_stats:{day}:group:{group_jid}"

def _period_days(view: str):
    view = (view or "day").strip().lower()

    if view == "week":
        start = _panel_week_start()
        end = _panel_week_end()
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
            group_name = GROUP_NAME_MAP.get(group_jid) or raw.get("group_name") or group_jid

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
    group_name = GROUP_NAME_MAP.get(group_jid) or group_jid
    detail = []

    total_clon = 0
    total_idcif = 0
    total_sub_clon = 0.0
    total_sub_idcif = 0.0
    total_general = 0.0

    for day in days:
        raw = redis_conn.hgetall(_cut_stats_key_for_day(day, group_jid)) or {}
        raw = {_to_str(k): _to_str(v) for k, v in raw.items()}

        if raw.get("group_name"):
            group_name = raw.get("group_name")

        count_clon = _safe_int(raw.get("count_clon"))
        count_idcif = _safe_int(raw.get("count_idcif"))

        subtotal_clon = round(count_clon * prices["clon"], 2)
        subtotal_idcif = round(count_idcif * prices["idcif"], 2)
        total = round(subtotal_clon + subtotal_idcif, 2)

        total_clon += count_clon
        total_idcif += count_idcif
        total_sub_clon += subtotal_clon
        total_sub_idcif += subtotal_idcif
        total_general += total

        detail.append({
            "date": day,
            "day_name": _day_name_es(day),
            "count_clon": count_clon,
            "count_idcif": count_idcif,
            "price_clon": prices["clon"],
            "price_idcif": prices["idcif"],
            "subtotal_clon": subtotal_clon,
            "subtotal_idcif": subtotal_idcif,
            "total": total,
        })

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

def send_daily_cut_for_group(group_jid: str, day_str: str = None):
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

    evolution_send_text(group_jid=group_jid, text=msg)

    return {
        "ok": True,
        "group_jid": group_jid,
        "group_name": target["group_name"],
        "day": day_str,
        "total": _safe_float(target["total"]),
    }

def send_daily_cuts(day_str: str = None):
    day_str = (day_str or _panel_day_str()).strip()
    rows = _load_cut_rows_for_days([day_str])

    sent = []
    skipped = []

    for r in rows:
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

        evolution_send_text(group_jid=r["group_jid"], text=msg)
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
        group_name = GROUP_NAME_MAP.get(remote_jid) or msg.get("group_name") or remote_jid

        print("[GROUP NAME FINAL BEFORE JOB]", repr(group_name), flush=True)
        print("[REMOTE JID]", repr(remote_jid), flush=True)

        if not remote_jid.endswith("@g.us"):
            return jsonify({"ok": True, "ignored": "not_group"}), 200

        if is_group_blocked(remote_jid):
            return jsonify({"ok": True, "ignored": "group_blocked"}), 200

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
                        text=parsed.get("error") or _format_input_error()
                    )
                except Exception as e:
                    print("validation sendText error:", repr(e), flush=True)
        
                return jsonify({
                    "ok": True,
                    "ignored": "invalid_input",
                    "reason": parsed.get("type")
                }), 200
        
        query = parsed.get("query")

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
            "query_type": parsed.get("type") if not is_media_candidate else msg_type,
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
    view = _safe(request.args.get("view")).lower()

    if view == "week":
        rows = _panel_load_week_rows()
        period = "week"
    else:
        rows = _panel_load_today_rows()
        period = "day"

    summary = _panel_summary(rows)
    return jsonify({
        "ok": True,
        "view": period,
        "summary": summary,
        "rows": rows,
    }), 200

@app.post("/panel/block-group")
def panel_block_group():
    try:
        group_jid = _safe(request.form.get("group_jid"))
        view = _safe(request.form.get("view")).lower()

        if not group_jid:
            return "group_jid requerido", 400

        block_group(group_jid)

        if view == "week":
            return redirect("/panel?view=week")
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

        if view == "week":
            return redirect("/panel?view=week")
        return redirect("/panel")
    except Exception as e:
        print("panel_unblock_group error:", repr(e), flush=True)
        traceback.print_exc()
        return "error desbloqueando grupo", 500

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
    key = f"cron_sent_daily_cuts:{_panel_day_str()}"
    redis_conn.delete(key)
    return {"ok": True, "deleted": key}

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
        
        result = send_daily_cuts(day_str=day_str)
        
        sent = result.get("sent") or []
        
        if sent:
            redis_conn.set(lock_key, "1", ex=60 * 60 * 24)

        return jsonify(result), 200

    except Exception as e:
        print("cron_send_daily_cuts error:", repr(e), flush=True)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/panel/cuts")
def panel_cuts():
    view = _safe(request.args.get("view")).lower() or "day"
    group_jid = _safe(request.args.get("group_jid"))
    day_param = _safe(request.args.get("day"))

    if day_param:
        days = [day_param]
        subtitle = f"Corte diario: {_period_day_label_es(day_param)} ({PANEL_TZ})"
        view = "day"
    else:
        days = _period_days(view)
        if view == "week":
            subtitle = f"Historial semanal: {days[0]} a {days[-1]} ({PANEL_TZ})"
        else:
            subtitle = f"Corte diario: {_today_label_es()} ({PANEL_TZ})"

    def esc(v):
        if v is None:
            return ""
        return str(v)

    if group_jid:
        detail = _load_cut_detail_for_group(group_jid, days)
        totals = detail["totals"]

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
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div><a href="/panel/cuts?view={esc(view)}">← Volver al historial</a></div>
      <h2 style="margin:10px 0 6px;">{esc(detail["group_name"])}</h2>
      <div>{esc(subtitle)}</div>
    </div>

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
    .cards {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:12px; margin-bottom:16px; }}
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
      border:none; border-radius:10px; padding:9px 12px; font-weight:700; cursor:pointer;
      background:#2563eb; color:white;
    }}
    .btn-green {{ background:#16a34a; }}
    .inline-form {{ margin:0; display:inline-block; }}
    @media (max-width: 900px) {{
      .cards {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
      .table-wrap {{ overflow-x:auto; }}
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
        <a href="/panel" class="tool-link">Panel</a>
        <a href="/panel/cuts?view=day" class="tool-link {'tool-link-active' if view == 'day' else ''}">Corte de hoy</a>
        <a href="/panel/cuts?view=week" class="tool-link {'tool-link-active' if view == 'week' else ''}">Lunes a domingo</a>
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
                <a class="tool-link" style="background:#2563eb; color:#fff; border:none;" href="/panel/cuts?view={esc(view)}&group_jid={esc(r["group_jid"])}">Detalle</a>
                {"<form class='inline-form' method='post' action='/panel/send-daily-cut-group' style='margin-left:8px;'><input type='hidden' name='group_jid' value='" + esc(r["group_jid"]) + "'><input type='hidden' name='day' value='" + esc(_panel_day_str()) + "'><button class='btn btn-green' type='submit'>Enviar</button></form>" if view == "day" else ""}
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
    view = _safe(request.args.get("view")).lower()

    if view == "week":
        rows = _panel_load_week_rows()
        title_period = "Semana actual"
        week_start = _panel_week_start().strftime("%Y-%m-%d")
        week_end = _panel_week_end().strftime("%Y-%m-%d")
        subtitle = f"Corte semanal: {week_start} a {week_end} ({PANEL_TZ})"
        auto_reload = "false"
        section_note = "Vista semanal"
    else:
        rows = _panel_load_today_rows()
        title_period = "Hoy"
        subtitle = f"Corte diario automático: {_panel_day_str()} (reinicio lógico a las 00:00:00, {PANEL_TZ})"
        auto_reload = "true"
        section_note = "Actualización automática cada 30 segundos"

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
    
    @media (max-width: 720px) {{
      .btn {{
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
      setTimeout(() => location.reload(), 30000);
    }}
  </script>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Panel puente WA</h1>
      <p class="sub">
        {subtitle}
      </p>
      <div class="toolbar">
        <a href="/panel" class="tool-link {'tool-link-active' if view != 'week' else ''}">Hoy</a>
        <a href="/panel?view=week" class="tool-link {'tool-link-active' if view == 'week' else ''}">Semana actual</a>
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
              <th class="right">Total</th>
              <th class="right">RFC_IDCIF</th>
              <th class="right">QR</th>
              <th class="right">CURP</th>
              <th class="right">RFC_solo</th>
              <th>Actualizado</th>
              <th>Acción</th>
            </tr>
          </thead>
          <tbody>
    """

    if rows:
        for r in rows:
            blocked = bool(r.get("blocked"))
            status_html = (
                '<span class="status-pill status-blocked">BLOQUEADO</span>'
                if blocked else
                '<span class="status-pill status-active">ACTIVO</span>'
            )
    
            if blocked:
                action_html = f"""
                <form class="action-form" method="post" action="/panel/unblock-group">
                  <input type="hidden" name="group_jid" value="{esc(r["group_jid"])}">
                  <input type="hidden" name="view" value="{esc(view)}">
                  <button class="btn btn-unblock" type="submit">Desbloquear</button>
                </form>
                """
            else:
                action_html = f"""
                <form class="action-form" method="post" action="/panel/block-group">
                  <input type="hidden" name="group_jid" value="{esc(r["group_jid"])}">
                  <input type="hidden" name="view" value="{esc(view)}">
                  <button class="btn btn-block" type="submit">Bloquear</button>
                </form>
                """
    
            html += f"""
                <tr>
                  <td data-label="Grupo">
                    <div class="group-name">{esc(r["group_name"])}</div>
                    <div class="group-id">{esc(r["group_jid"])}</div>
                  </td>
                  <td data-label="Estado">{status_html}</td>
                  <td data-label="Total" class="right"><span class="badge total-badge">{esc(r["total"])}</span></td>
                  <td data-label="RFC_IDCIF" class="right"><span class="badge">{esc(r["ok_rfc_idcif"])}</span></td>
                  <td data-label="QR" class="right"><span class="badge">{esc(r["ok_qr"])}</span></td>
                  <td data-label="CURP" class="right"><span class="badge">{esc(r["ok_curp"])}</span></td>
                  <td data-label="RFC_solo" class="right"><span class="badge">{esc(r["ok_rfc_only"])}</span></td>
                  <td data-label="Actualizado"><span class="muted">{esc(r["updated_at"])}</span></td>
                  <td data-label="Acción">{action_html}</td>
                </tr>
            """
    else:
        html += """
            <tr>
              <td colspan="9" class="empty">Sin actividad en este periodo.</td>
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
