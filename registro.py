"""Registro de quais documentos cada pessoa já baixou.

Com DATABASE_URL configurada, grava no PostgreSQL (Supabase) e o histórico sobrevive a
reinícios e novos deploys. Sem ela, usa o arquivo downloads.json (útil só em desenvolvimento:
no Render o disco é apagado a cada reinício e no Vercel ele é somente leitura).

Cada registro guarda apenas quem baixou, o identificador do documento na D4Sign e quando.
As contas de login (usuarios) só existem com o banco configurado.
"""
import json
import logging
import os
import threading
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL', '').strip()
ARQUIVO_LOCAL = os.path.join(os.path.dirname(__file__), 'downloads.json')
USUARIO_COMPARTILHADO = {'id': 'compartilhado', 'email': None}

_pool = None
_trava_arquivo = threading.Lock()

if DATABASE_URL:
    from psycopg_pool import ConnectionPool
    # prepare_threshold=None: sem prepared statements, compatível com o pooler do Supabase
    _pool = ConnectionPool(DATABASE_URL, min_size=0, max_size=4, open=True,
                           kwargs={'autocommit': True, 'prepare_threshold': None})


def usando_banco():
    return _pool is not None


def registrar(usuario, uuids):
    """Marca os documentos como baixados pela pessoa (ou atualiza a data, se já estavam)."""
    uuids = sorted({u for u in uuids if u})
    if not uuids:
        return
    usuario = usuario or USUARIO_COMPARTILHADO
    if _pool:
        with _pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO downloads (usuario_id, usuario_email, uuid_doc) VALUES (%s, %s, %s)
                       ON CONFLICT (usuario_id, uuid_doc)
                       DO UPDATE SET baixado_em = now(), usuario_email = EXCLUDED.usuario_email""",
                    [(usuario['id'], usuario.get('email'), u) for u in uuids])
        return
    agora = datetime.now(timezone.utc).isoformat()
    with _trava_arquivo:
        dados = _ler_arquivo()
        pessoa = dados.setdefault(usuario['id'], {})
        for u in uuids:
            pessoa[u] = agora
        _salvar_arquivo(dados)


def remover(usuario, uuid_doc):
    """Tira a marcação de baixado de um documento para a pessoa (os das outras pessoas não mudam)."""
    usuario = usuario or USUARIO_COMPARTILHADO
    if _pool:
        with _pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM downloads WHERE usuario_id = %s AND uuid_doc = %s", (usuario['id'], uuid_doc))
        return
    with _trava_arquivo:
        dados = _ler_arquivo()
        if dados.get(usuario['id'], {}).pop(uuid_doc, None) is not None:
            _salvar_arquivo(dados)


def baixados(usuario):
    """Retorna {uuid_doc: datetime (UTC, sem fuso)} dos documentos que a pessoa já baixou."""
    usuario = usuario or USUARIO_COMPARTILHADO
    if _pool:
        with _pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT uuid_doc, baixado_em FROM downloads WHERE usuario_id = %s", (usuario['id'],))
                return {u: dt.astimezone(timezone.utc).replace(tzinfo=None) for u, dt in cur.fetchall()}
    with _trava_arquivo:
        pessoa = _ler_arquivo().get(usuario['id'], {})
    resultado = {}
    for u, iso in pessoa.items():
        try:
            dt = datetime.fromisoformat(iso)
            resultado[u] = dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
        except (TypeError, ValueError):
            continue
    return resultado


def baixados_por_documento(desde=None):
    """Quem baixou cada documento: {uuid_doc: [{'usuario_id', 'nome', 'email', 'baixado_em'}, ...]} de todas as pessoas.

    desde (datetime UTC sem fuso) ignora os downloads anteriores a essa data; None traz todos.
    O nome e o e-mail vêm do cadastro (usuarios); sem ele, ficam o e-mail ou o id gravados no download.
    """
    resultado = {}
    if _pool:
        sql = """SELECT d.uuid_doc, d.usuario_id, COALESCE(u.nome, d.usuario_email, d.usuario_id),
                        COALESCE(u.email, d.usuario_email), d.baixado_em
                 FROM downloads d LEFT JOIN usuarios u ON u.id::text = d.usuario_id"""
        params = ()
        if desde:
            sql += " WHERE d.baixado_em >= %s"
            params = (desde.replace(tzinfo=timezone.utc),)
        with _pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for uuid_doc, usuario_id, nome, email, dt in cur.fetchall():
                    resultado.setdefault(uuid_doc, []).append(
                        {'usuario_id': usuario_id, 'nome': nome, 'email': email, 'baixado_em': dt.astimezone(timezone.utc).replace(tzinfo=None)})
        return resultado
    with _trava_arquivo:
        dados = _ler_arquivo()
    for usuario_id, pessoa in dados.items():
        nome = 'Sem login' if usuario_id == USUARIO_COMPARTILHADO['id'] else usuario_id
        for uuid_doc, iso in pessoa.items():
            try:
                dt = datetime.fromisoformat(iso)
            except (TypeError, ValueError):
                continue
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
            if desde and dt < desde:
                continue
            resultado.setdefault(uuid_doc, []).append({'usuario_id': usuario_id, 'nome': nome, 'email': None, 'baixado_em': dt})
    return resultado


CAMPOS_USUARIO = ('id', 'nome', 'email', 'senha_hash', 'confirmado_em')


def buscar_usuario(email):
    """Conta cadastrada com esse e-mail ({id, nome, email, senha_hash, confirmado_em}) ou None."""
    with _pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, nome, email, senha_hash, confirmado_em FROM usuarios WHERE email = %s", (email,))
            linha = cur.fetchone()
    return dict(zip(CAMPOS_USUARIO, linha)) if linha else None


def criar_usuario(nome, email, senha_hash):
    """Cria a conta (ainda sem e-mail confirmado) e devolve seus dados.

    Se já existe um cadastro com esse e-mail que nunca foi confirmado, ele é substituído: assim
    ninguém "reserva" o e-mail de um colega. Devolve None se o e-mail já tem conta confirmada.
    """
    with _pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO usuarios (nome, email, senha_hash) VALUES (%s, %s, %s)
                           ON CONFLICT (email) DO UPDATE SET nome = EXCLUDED.nome, senha_hash = EXCLUDED.senha_hash
                           WHERE usuarios.confirmado_em IS NULL
                           RETURNING id, nome, email, senha_hash, confirmado_em""",
                        (nome, email, senha_hash))
            linha = cur.fetchone()
    return dict(zip(CAMPOS_USUARIO, linha)) if linha else None


def confirmar_usuario(usuario_id):
    """Marca o e-mail da conta como confirmado (mantém a data da primeira confirmação)."""
    with _pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""UPDATE usuarios SET confirmado_em = COALESCE(confirmado_em, now())
                           WHERE id = %s RETURNING id, nome, email, senha_hash, confirmado_em""", (usuario_id,))
            linha = cur.fetchone()
    return dict(zip(CAMPOS_USUARIO, linha)) if linha else None


def _ler_arquivo():
    try:
        with open(ARQUIVO_LOCAL, 'r', encoding='utf-8') as f:
            dados = json.load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception:
        logger.exception('Erro lendo downloads.json')
        return {}
    # Formato antigo: {uuid: {downloaded_at: ...}} sem separar por pessoa
    if dados and all(isinstance(v, dict) and ('uuidDoc' in v or 'downloaded_at' in v) for v in dados.values()):
        return {USUARIO_COMPARTILHADO['id']: {u: v.get('downloaded_at') for u, v in dados.items() if v.get('downloaded_at')}}
    return dados


def _salvar_arquivo(dados):
    try:
        with open(ARQUIVO_LOCAL, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception('Erro salvando downloads.json')
