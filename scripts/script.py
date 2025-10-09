"""Minimal, cleaned and optimized D4Sign app.

Improvements made:
- In-memory TTL cache for external API calls (listar_cofres, listar_documentos)
- Pre-parse dates once and store datetime object for fast filtering
- Limit number of rendered documents to avoid huge HTML payloads
- Log timing for index page generation
"""

from flask import Flask, render_template, request, send_file, jsonify
import requests
import io
import zipfile
import re
import base64
import logging
import time
from datetime import datetime, timedelta
import os
from functools import wraps
import json
import threading
import traceback

# Serve templates and static assets from the repository root (parent of
# this scripts/ folder) so the existing `index.html`, `scripts/` and
# `style/` folders work without moving files. Use an absolute path so the
# debug reloader doesn't change lookup behavior.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
app = Flask(__name__, static_folder=PROJECT_ROOT, static_url_path='', template_folder=PROJECT_ROOT)

# Logger (define early so modules that run at import can log)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load inline SVG icons from the icons folder so we can embed them without external deps
def _load_svg(name):
    try:
        base = os.path.dirname(__file__)
        p = os.path.join(base, 'icons', name)
        with open(p, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception:
        return ''

ICON_UP_SVG = _load_svg('angulo-para-cima.svg')
ICON_DOWN_SVG = _load_svg('angulo-para-baixo.svg')
ICON_SUN_SVG = _load_svg('sun.svg')
ICON_MOON_SVG = _load_svg('moon.svg')
# Fallback to <i> tags if SVG files are missing or empty
if not ICON_UP_SVG or ICON_UP_SVG.strip() == '':
    ICON_UP_SVG = '<i class="fi fi-br-angle-up"></i>'
if not ICON_DOWN_SVG or ICON_DOWN_SVG.strip() == '':
    ICON_DOWN_SVG = '<i class="fi fi-br-angle-down"></i>'

# Config API D4Sign (sandbox dev mode)
HOST_D4SIGN = "https://sandbox.d4sign.com.br/api/v1"
TOKEN_API = "live_0824b01d13a9c6885840804b24ef51ca8c5ac408e6a4409a70b116a5add8754e"
CRYPT_KEY = "live_crypt_a83yrcHUlM4V8j7F3LrF0Zx7bHO1LmFo"

# Optional Redis for persisting signature timestamps and background queue
REDIS_URL = os.environ.get('REDIS_URL') or os.environ.get('REDIS_URI') or ''
redis_client = None
try:
    if REDIS_URL:
        import redis
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        # quick ping
        redis_client.ping()
        logger.info('Connected to Redis')
except Exception:
    redis_client = None
    logger.info('Redis not available, continuing with in-memory caches')

# Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Simple TTL cache
CACHE = {}
CACHE_TTL = 60

# In-memory signature cache populated by manual refresh or webhooks
SIGNATURE_CACHE = {}

# Downloads tracking: prefer Redis set + hash, fallback to local JSON file
DOWNLOADS_SET_KEY = 'd4sign:downloads:set'
DOWNLOADS_META_KEY = 'd4sign:downloads:meta'
LOCAL_DOWNLOADS_FILE = os.path.join(os.path.dirname(__file__), 'downloads.json')

def _load_local_downloads():
    try:
        if os.path.exists(LOCAL_DOWNLOADS_FILE):
            with open(LOCAL_DOWNLOADS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        logger.exception('Erro lendo arquivo de downloads local')
    return {}

def _save_local_downloads(data):
    try:
        with open(LOCAL_DOWNLOADS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception('Erro salvando arquivo de downloads local')

def record_download(uuid_doc, meta: dict):
    """Record a download event. meta is a serializable dict with at least 'uuidDoc'"""
    if not uuid_doc:
        return
    try:
        if redis_client:
            try:
                redis_client.sadd(DOWNLOADS_SET_KEY, uuid_doc)
                redis_client.hset(DOWNLOADS_META_KEY, uuid_doc, json.dumps(meta, default=str))
                return
            except Exception:
                logger.exception('Redis record_download error, falling back to local file')
        # fallback to local file
        d = _load_local_downloads()
        d[uuid_doc] = meta
        _save_local_downloads(d)
    except Exception:
        logger.exception('record_download error')

def get_downloaded_uuids():
    try:
        if redis_client:
            try:
                return set(redis_client.smembers(DOWNLOADS_SET_KEY) or [])
            except Exception:
                logger.exception('Redis get_downloaded_uuids error, falling back to local file')
        d = _load_local_downloads()
        return set(d.keys())
    except Exception:
        logger.exception('get_downloaded_uuids error')
        return set()

def get_downloaded_meta():
    try:
        if redis_client:
            try:
                raw = redis_client.hgetall(DOWNLOADS_META_KEY) or {}
                # convert json strings to dicts
                return {k: json.loads(v) for k, v in raw.items()}
            except Exception:
                logger.exception('Redis get_downloaded_meta error, falling back to local file')
        return _load_local_downloads()
    except Exception:
        logger.exception('get_downloaded_meta error')
        return {}


def _redis_key(uuid):
    return f'd4sign:signature:{uuid}'


def get_signature(uuid_doc):
    """Return a datetime from Redis or in-memory cache for uuid_doc, or None."""
    if not uuid_doc:
        return None
    # check in-memory first
    v = SIGNATURE_CACHE.get(uuid_doc)
    if isinstance(v, datetime):
        return v
    # fallback to redis
    if redis_client:
        try:
            s = redis_client.get(_redis_key(uuid_doc))
            if s:
                try:
                    # isoformat stored
                    dt = datetime.fromisoformat(s)
                    # sync into memory for faster access
                    SIGNATURE_CACHE[uuid_doc] = dt
                    return dt
                except Exception:
                    return None
        except Exception:
            logger.exception('Redis get error')
    return None


def set_signature(uuid_doc, dt: datetime):
    """Persist signature datetime to Redis (if available) and in-memory cache."""
    if not uuid_doc or not dt:
        return
    SIGNATURE_CACHE[uuid_doc] = dt
    if redis_client:
        try:
            # store ISO format
            redis_client.set(_redis_key(uuid_doc), dt.isoformat())
        except Exception:
            logger.exception('Redis set error')


def enqueue_refresh_uuids(uuids):
    """Push UUIDs to Redis queue (one item per uuid). Returns number enqueued."""
    if not redis_client or not uuids:
        return 0
    try:
        # use LPUSH so worker can BRPOP from the other side, or use RPUSH/BLPOP consistently
        for u in uuids:
            redis_client.rpush('d4sign:refresh_queue', u)
        return len(uuids)
    except Exception:
        logger.exception('Redis enqueue error')
        return 0


def _worker_process_uuid(u, delay=0.35):
    """Process a single uuid: refresh signature via signers endpoint or document detail.
    Update Redis and in-memory cache via set_signature.
    """
    try:
        dt = get_signers_for_document(u)
        if not dt:
            url = f"{HOST_D4SIGN}/documents/{u}?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
            r = requests.get(url, timeout=12)
            if r.status_code == 200:
                try:
                    pl = r.json()
                    dt = extract_latest_from_payload(pl)
                except Exception:
                    dt = None
        if dt:
            set_signature(u, dt)
            logger.info(f'Worker refreshed {u} -> {dt}')
        else:
            logger.info(f'Worker could not find signature for {u}')
    except Exception:
        logger.error('Worker error processing %s:\n%s', u, traceback.format_exc())
    try:
        time.sleep(delay)
    except Exception:
        pass


def _background_worker_loop():
    """Background loop that BRPOP from Redis list 'd4sign:refresh_queue' and processes uuids."""
    if not redis_client:
        return
    logger.info('Starting background refresh worker')
    while True:
        try:
            # BRPOP returns a tuple (key, value) or None
            item = redis_client.brpop('d4sign:refresh_queue', timeout=5)
            if not item:
                continue
            # item[1] contains the value pushed (uuid)
            u = item[1]
            if not u:
                continue
            _worker_process_uuid(u)
        except Exception:
            logger.error('Background worker loop error:\n%s', traceback.format_exc())


# If Redis is available, start a background worker thread to process the refresh queue
if redis_client:
    t = threading.Thread(target=_background_worker_loop, daemon=True)
    t.start()
else:
    # Redis not available: skip running automatic background refresh to keep the app quiet.
    logger.info('Redis not available; auto-refresh disabled')


def cached(ttl: int = CACHE_TTL):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                key = (func.__name__, args, tuple(sorted(kwargs.items())))
            except Exception:
                key = (func.__name__,)
            now = time.time()
            entry = CACHE.get(key)
            if entry and now - entry[0] < ttl:
                return entry[1]
            result = func(*args, **kwargs)
            try:
                CACHE[key] = (now, result)
            except Exception:
                pass
            return result
        return wrapper
    return decorator


@cached()
def listar_cofres():
    url = f"{HOST_D4SIGN}/safes?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.exception("Erro listar_cofres")
    return []


@cached()
def listar_documentos(uuid_safe=None):
    if uuid_safe:
        url = f"{HOST_D4SIGN}/documents/{uuid_safe}/safe?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
    else:
        url = f"{HOST_D4SIGN}/documents?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return []
        docs = r.json()
        documentos = []
        for doc in docs:
            if doc.get("statusName") != "Finalizado":
                continue
            nome_original = doc.get("nameDoc") or doc.get("name") or ""
            nome_limpo = re.sub(r"^\d{8}\s*", "", nome_original)
            nome_limpo = re.sub(r"R\$\s*[\d\s.,]+", "", nome_limpo, flags=re.IGNORECASE)
            nome_limpo = re.sub(r"(\.pdf|\s+pdf)$", "", nome_limpo, flags=re.IGNORECASE).strip()

            # pre-parse date if available in name or known fields
            data_dt = None
            m = re.search(r"(\d{8})", nome_original)
            if m:
                try:
                    data_dt = datetime.strptime(m.group(1), "%Y%m%d")
                except Exception:
                    data_dt = None
            else:
                candidate = doc.get("dateSigned") or doc.get("lastSignerDate") or doc.get("lastSignDate")
                if candidate:
                    try:
                        if isinstance(candidate, str):
                            data_dt = datetime.fromisoformat(candidate.replace('Z', '+00:00'))
                        elif isinstance(candidate, (int, float)):
                            data_dt = datetime.fromtimestamp(candidate)
                    except Exception:
                        data_dt = None

            # extract the API's last signature date explicitly when available
            last_candidate = doc.get("lastSignerDate") or doc.get("lastSignDate") or doc.get("dateSigned")
            ultima_dt = None
            if last_candidate:
                try:
                    if isinstance(last_candidate, str):
                        ultima_dt = datetime.fromisoformat(last_candidate.replace('Z', '+00:00'))
                    elif isinstance(last_candidate, (int, float)):
                        ultima_dt = datetime.fromtimestamp(last_candidate)
                except Exception:
                    ultima_dt = None

            doc["nomeLimpo"] = nome_limpo
            doc["dataAssinatura_dt"] = data_dt
            doc["dataAssinatura"] = data_dt.strftime("%d/%m/%Y") if isinstance(data_dt, datetime) else "Não Consta"
            doc["ultimaAssinatura_dt"] = ultima_dt
            doc["ultimaAssinatura"] = ultima_dt.strftime("%d/%m/%Y %H:%M:%S") if isinstance(ultima_dt, datetime) else "Não Consta"
            doc["nomeOriginal"] = nome_original
            doc["uuidDoc"] = doc.get("uuidDoc") or doc.get("uuid")
            doc["cofre_uuid"] = doc.get("uuid_safe") or doc.get("uuidSafe")
            # If we have a cached signature timestamp from webhook/refresh, use it when list doesn't provide it
            cached_dt = SIGNATURE_CACHE.get(doc.get("uuidDoc"))
            if not doc.get('ultimaAssinatura_dt') and isinstance(cached_dt, datetime):
                doc['ultimaAssinatura_dt'] = cached_dt
                doc['ultimaAssinatura'] = cached_dt.strftime("%d/%m/%Y %H:%M:%S")
            documentos.append(doc)
        return documentos
    except Exception:
        logger.exception("Erro listar_documentos")
        return []


def baixar_documento(uuid_doc):
    url = f"{HOST_D4SIGN}/documents/{uuid_doc}/download?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
    try:
        r = requests.post(url, json={"type": "pdf", "language": "pt"}, timeout=30)
        if r.status_code != 200:
            return None
        result = r.json()
        if "content" in result:
            content_val = result["content"]
            if isinstance(content_val, str) and content_val.startswith("data:"):
                parts = content_val.split(",", 1)
                content_val = parts[1] if len(parts) > 1 else content_val
            return base64.b64decode(content_val + "=" * ((4 - len(content_val) % 4) % 4))
        if "url" in result:
            resp = requests.get(result["url"], timeout=30)
            if resp.status_code == 200:
                return resp.content
    except Exception:
        logger.exception(f"Erro baixar_documento {uuid_doc}")
    return None


# Fetch signers for a specific document and extract most recent signature timestamp
@cached(ttl=3600)
def get_signers_for_document(uuid_doc):
    """Call GET /documents/{uuid}/list to obtain signers and derive last signature date."""
    url = f"{HOST_D4SIGN}/documents/{uuid_doc}/list?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        payload = r.json()
        # payload expected to be a list or dict containing 'signers'
        signers = None
        if isinstance(payload, dict):
            signers = payload.get("signers") or payload.get("list") or payload.get("data")
        elif isinstance(payload, list):
            signers = payload
        if not signers:
            return None

        latest = None
        for s in signers:
            # try multiple candidate fields where a timestamp may appear
            candidate = s.get("signedAt") or s.get("signed_at") or s.get("dateSigned") or s.get("signedDate") or s.get("date")
            if not candidate:
                # some fields might contain nested 'signature' or 'history'
                if isinstance(s, dict):
                    for k in ("signature", "history", "events"):
                        v = s.get(k)
                        if isinstance(v, dict):
                            candidate = v.get("signedAt") or v.get("date") or candidate
                        elif isinstance(v, list) and v:
                            candidate = v[0].get("signedAt") or v[0].get("date") or candidate
            if candidate:
                try:
                    if isinstance(candidate, str):
                        dt = datetime.fromisoformat(candidate.replace('Z', '+00:00'))
                    elif isinstance(candidate, (int, float)):
                        dt = datetime.fromtimestamp(candidate)
                    else:
                        continue
                    if latest is None or dt > latest:
                        latest = dt
                except Exception:
                    continue
        return latest
    except Exception:
        logger.exception(f"Erro get_signers_for_document {uuid_doc}")
        return None


def extract_latest_from_payload(payload):
    """Generic extractor: search for timestamp-like fields in dict/list payloads."""
    candidates = []
    def push_candidate(v):
        if isinstance(v, (int, float, str)):
            candidates.append(v)
    if isinstance(payload, dict):
        for k, v in payload.items():
            if k.lower() in ('datesigned','lastsignerdate','lastsigndate','signedat','signed_at','signeddate','date'):
                push_candidate(v)
        # nested
        for v in payload.values():
            if isinstance(v, (dict, list)):
                try:
                    nested = extract_latest_from_payload(v)
                    if nested:
                        candidates.append(nested)
                except Exception:
                    pass
    elif isinstance(payload, list):
        for item in payload:
            nested = extract_latest_from_payload(item)
            if nested:
                candidates.append(nested)

    latest = None
    for c in candidates:
        try:
            if isinstance(c, str):
                dt = datetime.fromisoformat(c.replace('Z', '+00:00'))
            elif isinstance(c, (int, float)):
                dt = datetime.fromtimestamp(c)
            else:
                continue
            if latest is None or dt > latest:
                latest = dt
        except Exception:
            continue
    return latest


@app.route('/refresh-signature', methods=['POST'])
def refresh_signature():
    data = request.get_json() or {}
    uuid_doc = data.get('uuid') or data.get('uuidDoc')
    if not uuid_doc:
        return jsonify({'error': 'missing uuid'}), 400
    # try signers endpoint
    try:
        dt = get_signers_for_document(uuid_doc)
        # fallback: try document detail
        if not dt:
            url = f"{HOST_D4SIGN}/documents/{uuid_doc}?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
            r = requests.get(url, timeout=12)
            if r.status_code == 200:
                try:
                    pl = r.json()
                    dt = extract_latest_from_payload(pl)
                except Exception:
                    dt = None
        if dt:
            SIGNATURE_CACHE[uuid_doc] = dt
            return jsonify({'uuid': uuid_doc, 'ultimaAssinatura': dt.strftime('%d/%m/%Y %H:%M:%S')}), 200
        return jsonify({'uuid': uuid_doc, 'ultimaAssinatura': None}), 200
    except Exception:
        logger.exception('Erro refresh-signature')
        return jsonify({'error': 'internal error'}), 500


@app.route('/webhook/d4sign', methods=['POST'])
def webhook_d4sign():
    # minimal webhook receiver - expects JSON with document uuid and timestamp fields
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({'error': 'no json'}), 400
    # try to find uuid and timestamp
    uuid_doc = payload.get('uuid') or payload.get('uuidDoc') or payload.get('documentId')
    dt = extract_latest_from_payload(payload)
    if uuid_doc and dt:
        SIGNATURE_CACHE[uuid_doc] = dt
        logger.info(f'Webhook updated signature {uuid_doc} -> {dt}')
        return jsonify({'ok': True}), 200
    return jsonify({'ok': False}), 200


@app.route('/refresh-batch', methods=['POST'])
def refresh_batch():
    data = request.get_json() or {}
    uuids = data.get('uuids') or []
    if not isinstance(uuids, list) or not uuids:
        return jsonify({'error': 'missing uuids'}), 400
    results = {}
    # throttle settings to be gentle with the API
    delay = 0.35  # seconds between calls
    for u in uuids:
        try:
            dt = get_signers_for_document(u)
            # If the cached call returned None (possibly cached negative), force a fresh fetch
            if dt is None and hasattr(get_signers_for_document, '__wrapped__'):
                try:
                    dt = get_signers_for_document.__wrapped__(u)
                except Exception:
                    pass
            if not dt:
                # fallback to detail extraction
                url = f"{HOST_D4SIGN}/documents/{u}?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    try:
                        pl = r.json()
                        dt = extract_latest_from_payload(pl)
                    except Exception:
                        dt = None
            if dt:
                SIGNATURE_CACHE[u] = dt
                results[u] = dt.strftime('%d/%m/%Y %H:%M:%S')
            else:
                results[u] = None
        except Exception:
            results[u] = None
        # sleep a bit to avoid bursts
        try:
            time.sleep(delay)
        except Exception:
            pass
    return jsonify({'ok': True, 'result': results}), 200


@app.route('/refresh-from-downloads', methods=['POST'])
def refresh_from_downloads():
    """Read uuids from local downloads.json and refresh their latest signature dates.
    Returns a mapping uuid -> formatted date or None.
    """
    try:
        data = _load_local_downloads() or {}
        if not isinstance(data, dict) or not data:
            return jsonify({'ok': False, 'error': 'no downloads found', 'result': {}}), 200
        uuids = list(data.keys())
        results = {}
        # throttle a bit to avoid bursts
        for u in uuids:
            try:
                dt = get_signers_for_document(u)
                if dt is None and hasattr(get_signers_for_document, '__wrapped__'):
                    try:
                        dt = get_signers_for_document.__wrapped__(u)
                    except Exception:
                        pass
                if not dt:
                    # fallback to document detail
                    url = f"{HOST_D4SIGN}/documents/{u}?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
                    r = requests.get(url, timeout=10)
                    if r.status_code == 200:
                        try:
                            pl = r.json()
                            dt = extract_latest_from_payload(pl)
                        except Exception:
                            dt = None
                if dt:
                    SIGNATURE_CACHE[u] = dt
                    results[u] = dt.strftime('%d/%m/%Y %H:%M:%S')
                else:
                    results[u] = None
            except Exception:
                results[u] = None
            try:
                time.sleep(0.25)
            except Exception:
                pass
        return jsonify({'ok': True, 'result': results}), 200
    except Exception:
        logger.exception('refresh-from-downloads error')
        return jsonify({'ok': False, 'error': 'internal error', 'result': {}}), 500


@app.route('/register-dates', methods=['POST'])
def register_dates():
    """Persist available ultimaAssinatura for currently listed documents into downloads.json.
    Returns mapping uuid -> formatted date for UI update.
    """
    try:
        # Load existing stored metadata first: prefer persisted signatures when present
        all_meta = _load_local_downloads() or {}
        documentos = listar_documentos()
        results = {}
        changed = False
        for d in documentos:
            uuid = d.get('uuidDoc')
            if not uuid:
                continue

            candidate_dt = None
            # 1) prefer the in-memory/listing value if it's a datetime
            ua_dt = d.get('ultimaAssinatura_dt')
            if isinstance(ua_dt, datetime):
                candidate_dt = ua_dt

            # 2) fallback to already persisted downloads.json value (iso string)
            if not candidate_dt:
                meta = all_meta.get(uuid)
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except Exception:
                        meta = {}
                if isinstance(meta, dict):
                    iso = meta.get('ultimaAssinatura')
                    if iso:
                        try:
                            candidate_dt = datetime.fromisoformat(iso)
                        except Exception:
                            candidate_dt = None

            # 3) as a last resort, try the signers endpoint or document detail now
            if not candidate_dt:
                try:
                    dt = get_signers_for_document(uuid)
                    # if cached decorated call returned None, try underlying function
                    if dt is None and hasattr(get_signers_for_document, '__wrapped__'):
                        try:
                            dt = get_signers_for_document.__wrapped__(uuid)
                        except Exception:
                            dt = None
                    if not dt:
                        url = f"{HOST_D4SIGN}/documents/{uuid}?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
                        r = requests.get(url, timeout=12)
                        if r.status_code == 200:
                            try:
                                pl = r.json()
                                dt = extract_latest_from_payload(pl)
                            except Exception:
                                dt = None
                    if isinstance(dt, datetime):
                        candidate_dt = dt
                except Exception:
                    logger.exception('Error fetching signers/detail for %s', uuid)

            # Format result and persist only when we have a datetime
            if isinstance(candidate_dt, datetime):
                iso = candidate_dt.isoformat()
                # merge into persisted meta
                meta = all_meta.get(uuid)
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except Exception:
                        meta = {}
                meta = meta or {}
                if meta.get('ultimaAssinatura') != iso:
                    meta['uuidDoc'] = uuid
                    meta['ultimaAssinatura'] = iso
                    # mark source as registered when coming from listing, or api_list when from API
                    meta['ultimaAssinatura_source'] = meta.get('ultimaAssinatura_source') or 'registered'
                    # preserve nomeOriginal/downloaded_at if present in either meta or listing
                    if not meta.get('nomeOriginal') and d.get('nomeOriginal'):
                        meta['nomeOriginal'] = d.get('nomeOriginal')
                    if not meta.get('downloaded_at') and all_meta.get(uuid) and isinstance(all_meta.get(uuid), dict) and all_meta.get(uuid).get('downloaded_at'):
                        meta['downloaded_at'] = all_meta.get(uuid).get('downloaded_at')
                    all_meta[uuid] = meta
                    changed = True
                results[uuid] = candidate_dt.strftime('%d/%m/%Y %H:%M:%S')
            else:
                # do not overwrite anything in persisted meta; return existing persisted formatted value if any
                meta = all_meta.get(uuid)
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except Exception:
                        meta = {}
                existing_iso = (meta or {}).get('ultimaAssinatura')
                if existing_iso:
                    try:
                        dt = datetime.fromisoformat(existing_iso)
                        results[uuid] = dt.strftime('%d/%m/%Y %H:%M:%S')
                    except Exception:
                        results[uuid] = None
                else:
                    results[uuid] = None

        # persist any changes
        if changed:
            try:
                if redis_client:
                    for k, v in all_meta.items():
                        try:
                            redis_client.hset(DOWNLOADS_META_KEY, k, json.dumps(v, default=str, ensure_ascii=False))
                        except Exception:
                            logger.exception('Error writing registered dates to redis for %s', k)
                else:
                    _save_local_downloads(all_meta)
            except Exception:
                logger.exception('Error saving registered dates')

        return jsonify({'ok': True, 'result': results}), 200
    except Exception:
        logger.exception('register-dates error')
        return jsonify({'ok': False, 'error': 'internal error'}), 500


@app.route("/", methods=["GET", "POST"])
def index():
    t0 = time.time()
    cofres = listar_cofres()
    cofre_map = { (c.get("uuid") or c.get("uuid_safe") or c.get("uuid-safe")):
                  (c.get("name") or c.get("name_safe") or c.get("name-safe", "Sem Nome"))
                  for c in cofres }

    cofre_selecionado = request.form.get("cofre")
    documentos = listar_documentos(cofre_selecionado)

    # view_status filter requested by UI: default 'nao_baixado' (show non-downloaded documents)
    view_status = request.form.get('view_status') or request.args.get('view_status') or 'nao_baixado'

    # mark cofre and whether documento was previously downloaded within the last 60 days
    downloaded_meta = get_downloaded_meta() or {}
    recent_threshold = datetime.utcnow() - timedelta(days=60)
    recent_downloaded = set()
    for k, v in downloaded_meta.items():
        try:
            # v is expected to be a dict with 'downloaded_at' in ISO format
            if isinstance(v, str):
                v = json.loads(v)
        except Exception:
            pass
        try:
            dt_str = (v or {}).get('downloaded_at')
            if not dt_str and isinstance(v, str):
                # fallback: maybe stored as plain ISO string
                dt = datetime.fromisoformat(v)
            else:
                dt = datetime.fromisoformat(dt_str) if dt_str else None
        except Exception:
            dt = None
        if isinstance(dt, datetime) and dt >= recent_threshold:
            recent_downloaded.add(k)

    for d in documentos:
        d["cofre_nome"] = cofre_map.get(d.get("cofre_uuid"), "Desconhecido")
        d['baixado'] = (d.get('uuidDoc') in recent_downloaded)
        # signature-enrichment removed (we no longer show ultimaAssinatura)

    busca_nome = (request.form.get("busca_nome") or "").strip().lower()
    if busca_nome:
        documentos = [d for d in documentos if busca_nome in (d.get("nomeLimpo") or "").lower()]

    # Date filtering logic
    data_periodo = (request.form.get('data_periodo') or '').strip()
    data_inicio = request.form.get("data_inicio")
    data_fim = request.form.get("data_fim")
    # ultima_* filters removed

    # Default date range: if user didn't apply a filter, set to earliest and latest
    # 'dataAssinatura_dt' among the currently listed documents when available.
    date_filter_applied = any([data_periodo, data_inicio, data_fim])
    # Only auto-fill a default start date on initial GET load. If the user POSTs a filter
    # we must not override their submitted values with the oldest-document default.
    if not date_filter_applied and request.method == 'GET':
        try:
            dates = [d.get('dataAssinatura_dt') for d in documentos if isinstance(d.get('dataAssinatura_dt'), datetime)]
            if dates:
                # fill only the start of the period with the oldest document's date
                min_dt = min(dates)
                data_inicio = min_dt.strftime('%Y-%m-%d')
                # leave data_fim empty so user can pick an end date explicitly
                data_fim = ''
            else:
                # fallback to last 30 days if no parsed dates are available
                ontem = datetime.now() - timedelta(days=1)
                inicio_periodo = ontem - timedelta(days=29)
                data_inicio = inicio_periodo.strftime('%Y-%m-%d')
                data_fim = ontem.strftime('%Y-%m-%d')
        except Exception:
            ontem = datetime.now() - timedelta(days=1)
            inicio_periodo = ontem - timedelta(days=29)
            data_inicio = inicio_periodo.strftime('%Y-%m-%d')
            data_fim = ontem.strftime('%Y-%m-%d')

    # Filtro por campo "data" (dataAssinatura_dt)
    if data_periodo:
        try:
            # Accept either ISO (YYYY-MM-DD - YYYY-MM-DD) or display format (DD/MM/YYYY - DD/MM/YYYY)
            import re as _re
            # try ISO range: 2025-09-01 - 2025-09-30
            m = _re.search(r"(\d{4}-\d{2}-\d{2}).*(\d{4}-\d{2}-\d{2})", data_periodo)
            if m:
                dt_inicio = datetime.strptime(m.group(1), "%Y-%m-%d")
                dt_fim = datetime.strptime(m.group(2), "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                documentos = [d for d in documentos if d.get("dataAssinatura_dt") and dt_inicio <= d["dataAssinatura_dt"] <= dt_fim]
            else:
                # try DD/MM/YYYY - DD/MM/YYYY or single DD/MM/YYYY
                m2 = _re.search(r"(\d{2}/\d{2}/\d{4}).*(\d{2}/\d{2}/\d{4})", data_periodo)
                if m2:
                    dt_inicio = datetime.strptime(m2.group(1), "%d/%m/%Y")
                    dt_fim = datetime.strptime(m2.group(2), "%d/%m/%Y").replace(hour=23, minute=59, second=59)
                    documentos = [d for d in documentos if d.get("dataAssinatura_dt") and dt_inicio <= d["dataAssinatura_dt"] <= dt_fim]
                else:
                    # single date in either format
                    m3 = _re.search(r"(\d{4}-\d{2}-\d{2})", data_periodo)
                    if m3:
                        dt_inicio = datetime.strptime(m3.group(1), "%Y-%m-%d")
                        dt_fim = dt_inicio.replace(hour=23, minute=59, second=59)
                        documentos = [d for d in documentos if d.get("dataAssinatura_dt") and dt_inicio <= d["dataAssinatura_dt"] <= dt_fim]
                    else:
                        m4 = _re.search(r"(\d{2}/\d{2}/\d{4})", data_periodo)
                        if m4:
                            dt_inicio = datetime.strptime(m4.group(1), "%d/%m/%Y")
                            dt_fim = dt_inicio.replace(hour=23, minute=59, second=59)
                            documentos = [d for d in documentos if d.get("dataAssinatura_dt") and dt_inicio <= d["dataAssinatura_dt"] <= dt_fim]
        except Exception:
            pass
    elif data_inicio and data_fim:
        try:
            dt_inicio = datetime.strptime(data_inicio, "%Y-%m-%d")
            dt_fim = datetime.strptime(data_fim, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            documentos = [d for d in documentos if d.get("dataAssinatura_dt") and dt_inicio <= d["dataAssinatura_dt"] <= dt_fim]
        except Exception:
            pass

    # ultimaAssinatura filtering removed

    # ordering (default: most recent first by 'Data')
    ordenar_por = request.form.get("ordenar_por")
    if not ordenar_por:
        ordenar_por = 'data_desc'
    # ordering by ultimaAssinatura removed (keep data ordering)
    if ordenar_por == "data_desc":
        documentos.sort(key=lambda d: d.get("dataAssinatura_dt") or datetime.min, reverse=True)
    elif ordenar_por == "data_asc":
        documentos.sort(key=lambda d: d.get("dataAssinatura_dt") or datetime.max)

    # Apply view filter
    if view_status == 'baixado':
        documentos = [d for d in documentos if d.get('baixado')]
    elif view_status == 'nao_baixado':
        documentos = [d for d in documentos if not d.get('baixado')]

    # Limit rendering size
    MAX_RENDER = 2000
    if len(documentos) > MAX_RENDER:
        documentos = documentos[:MAX_RENDER]

    # Downloads
    if request.method == "POST" and "download" in request.form:
        selecionados = request.form.getlist("documentos")
        if selecionados:
            mem = io.BytesIO()
            with zipfile.ZipFile(mem, "w", zipfile.ZIP_STORED) as zf:
                used = set()
                counts = {}
                for uuid_doc in selecionados:
                    content = baixar_documento(uuid_doc)
                    if not content:
                        continue
                    nome_original = request.form.get(f"doc_nomes[{uuid_doc}]") or f"{uuid_doc}.pdf"
                    safe_name = re.sub(r'[<>:"/\\|?*]', "_", nome_original).strip()
                    if not os.path.splitext(safe_name)[1]:
                        safe_name += ".pdf"
                    base, ext = os.path.splitext(safe_name)
                    candidate = safe_name
                    if candidate in used:
                        n = counts.get(base, 1)
                        candidate = f"{base} ({n}){ext}"
                        counts[base] = n + 1
                    used.add(candidate)
                    zf.writestr(candidate, content)
                    # try to persist that this uuid was downloaded (server-side)
                    try:
                        record_download(uuid_doc, {'uuidDoc': uuid_doc, 'nomeOriginal': nome_original, 'downloaded_at': datetime.utcnow().isoformat()})
                    except Exception:
                        logger.exception('Erro ao registrar download')
            mem.seek(0)
            # return a response with a header indicating how many files are inside the zip
            resp = send_file(mem, as_attachment=True, download_name="documentos_assinados.zip", mimetype="application/zip")
            try:
                resp.headers['X-Zip-Count'] = str(len(used))
            except Exception:
                pass
            return resp

    # persistence of ultimaAssinatura removed (column no longer shown)

    logger.info(f"Index generated in {time.time()-t0:.2f}s, documentos={len(documentos)}")
    # prepare auto-refresh timestamps for UI
    try:
        last_run = globals().get('AUTO_REFRESH_LAST_RUN')
        if isinstance(last_run, datetime):
            auto_refresh_last = last_run.strftime('%Y-%m-%d %H:%M:%S UTC')
        else:
            auto_refresh_last = None
        interval = int(os.environ.get('D4SIGN_AUTO_REFRESH_INTERVAL', '3600'))
        if auto_refresh_last:
            try:
                # compute next run time naively as last_run + interval
                next_run_dt = last_run + timedelta(seconds=interval)
                auto_refresh_next = next_run_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            except Exception:
                auto_refresh_next = None
        else:
            auto_refresh_next = None
    except Exception:
        auto_refresh_last = None
        auto_refresh_next = None
    # pre-wrap icons for safe JS injection
    ICON_UP_WRAPPED = '<span class="sort-icon">' + ICON_UP_SVG + '</span>'
    ICON_DOWN_WRAPPED = '<span class="sort-icon">' + ICON_DOWN_SVG + '</span>'
    # Render the `index.html` template (found in project root because
    # we configured `template_folder='.'` when creating the Flask app).
    return render_template('index.html', documentos=documentos, cofres=cofres,
                           cofre_selecionado=cofre_selecionado,
                           busca_nome=request.form.get("busca_nome", ""), data_inicio=data_inicio,
                           data_fim=data_fim, ordenar_por=ordenar_por,
                           ICON_UP=ICON_UP_SVG, ICON_DOWN=ICON_DOWN_SVG,
                           ICON_SUN=ICON_SUN_SVG, ICON_MOON=ICON_MOON_SVG,
                           ICON_UP_JS=json.dumps(ICON_UP_SVG), ICON_DOWN_JS=json.dumps(ICON_DOWN_SVG),
                           ICON_UP_WRAPPED_JS=json.dumps(ICON_UP_WRAPPED), ICON_DOWN_WRAPPED_JS=json.dumps(ICON_DOWN_WRAPPED),
                           total_downloaded=len(get_downloaded_uuids()),
                           auto_refresh_last=auto_refresh_last, auto_refresh_next=auto_refresh_next)


if __name__ == "__main__":
    app.run(debug=True)