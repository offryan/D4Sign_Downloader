"""Login do app.

Hoje: cadastro e login com e-mail e senha, salvos no Supabase (tabela usuarios). Só e-mails do
domínio DOMINIO_PERMITIDO (padrão: sitawi.net) conseguem se cadastrar, e a conta só entra depois
que a pessoa abre o link de confirmação enviado para o e-mail (ver email_envio.py). Fica ativo
quando DATABASE_URL está configurada; sem banco o app funciona sem login, como antes.

Mais pra frente: login com conta Microsoft (Entra ID). O código está comentado no fim deste
arquivo, esperando a liberação do T.I.
"""
import hashlib
import logging
import os
import re
import secrets
import threading
import time

from flask import current_app, redirect, render_template_string, request, session, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.security import check_password_hash, generate_password_hash

import email_envio
import registro

logger = logging.getLogger(__name__)

DOMINIO_PERMITIDO = os.environ.get('DOMINIO_PERMITIDO', 'sitawi.net').strip().lower().lstrip('@')
SENHA_MINIMA = 8
LOGIN_ATIVO = registro.usando_banco()

# Rotas que funcionam sem login: as telas de acesso e o webhook chamado pela D4Sign
ROTAS_PUBLICAS = {'login', 'cadastro', 'confirmar', 'reenviar_confirmacao', 'logout', 'webhook_d4sign', 'static', 'favicon'}

VALIDADE_LINK = 24 * 60 * 60
# Endereço público do site para montar o link do e-mail (o Render preenche RENDER_EXTERNAL_URL sozinho)
URL_DO_SITE = (os.environ.get('APP_URL') or os.environ.get('RENDER_EXTERNAL_URL') or '').strip().rstrip('/')
# Limite de e-mails de confirmação por endereço: 1 por minuto e 5 por hora
_envios = {}
_trava_envios = threading.Lock()

# Trava simples contra força bruta: 5 senhas erradas para o mesmo e-mail bloqueiam por 15 minutos
MAX_TENTATIVAS = 5
JANELA_BLOQUEIO = 15 * 60
_falhas = {}
_trava_falhas = threading.Lock()
# compara com um hash qualquer quando o e-mail não existe, para a resposta levar o mesmo tempo
_HASH_FALSO = generate_password_hash(secrets.token_hex(16))


def usuario_atual():
    """Pessoa logada ({id, nome, email}) ou None quando o login está desligado."""
    return session.get('usuario') if LOGIN_ATIVO else None


def email_permitido(email):
    return bool(re.fullmatch(r'[^@\s]+@' + re.escape(DOMINIO_PERMITIDO), email))


def _bloqueado(email):
    agora = time.time()
    with _trava_falhas:
        recentes = [t for t in _falhas.get(email, []) if agora - t < JANELA_BLOQUEIO]
        _falhas[email] = recentes
        return len(recentes) >= MAX_TENTATIVAS


def _registrar_falha(email):
    with _trava_falhas:
        _falhas.setdefault(email, []).append(time.time())


def _pode_enviar(email):
    agora = time.time()
    with _trava_envios:
        recentes = [t for t in _envios.get(email, []) if agora - t < 3600]
        if len(recentes) >= 5 or (recentes and agora - recentes[-1] < 60):
            _envios[email] = recentes
            return False
        _envios[email] = recentes + [agora]
        return True


def _assinador():
    return URLSafeTimedSerializer(current_app.secret_key, salt='confirmar-conta')


def _marca_da_senha(usuario):
    # o link só vale para o cadastro que o gerou: se alguém refizer o cadastro com outra senha, o link antigo morre
    return hashlib.sha256(usuario['senha_hash'].encode()).hexdigest()[:16]


def _enviar_confirmacao(usuario):
    if not _pode_enviar(usuario['email']):
        return
    token = _assinador().dumps({'email': usuario['email'], 'id': str(usuario['id']), 'marca': _marca_da_senha(usuario)})
    link = (URL_DO_SITE or request.url_root.rstrip('/')) + url_for('confirmar', token=token)
    primeiro_nome = usuario['nome'].split()[0]
    texto = (f'Olá, {primeiro_nome}!\n\nPara ativar sua conta no D4Sign Downloader, abra o link abaixo '
             f'(vale por 24 horas):\n\n{link}\n\nSe você não pediu esse cadastro, ignore este e-mail.')
    html = render_template_string(EMAIL_CONFIRMACAO, nome=primeiro_nome, link=link)
    email_envio.enviar(usuario['email'], usuario['nome'], 'Confirme seu e-mail · D4Sign Downloader', texto, html)


def _entrar(usuario):
    destino = session.get('depois_do_login', '/')
    session.clear()
    session['usuario'] = {'id': str(usuario['id']), 'nome': usuario['nome'], 'email': usuario['email']}
    session.permanent = True
    return redirect(destino)


def _destino_seguro(destino):
    return destino if destino and destino.startswith('/') and not destino.startswith('//') else '/'


def configurar(app):
    app.secret_key = os.environ.get('SECRET_KEY') or secrets.token_hex(32)
    app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE='Lax',
                      # no Render (HTTPS) o cookie de sessão só trafega por conexão segura
                      SESSION_COOKIE_SECURE=bool(os.environ.get('RENDER')),
                      PERMANENT_SESSION_LIFETIME=60 * 60 * 12)

    if not LOGIN_ATIVO:
        logger.warning('Login desligado: configure DATABASE_URL para guardar as contas no Supabase.')
    elif not os.environ.get('SECRET_KEY'):
        logger.warning('SECRET_KEY não configurada: as sessões caem a cada reinício do servidor.')

    @app.before_request
    def exigir_login():
        if not LOGIN_ATIVO or request.endpoint in ROTAS_PUBLICAS or session.get('usuario'):
            return None
        # navegação do navegador vai para o login; chamadas fetch recebem 401 (a página trata e redireciona)
        if request.headers.get('Sec-Fetch-Mode', 'navigate') != 'navigate':
            return {'ok': False, 'error': 'login necessário'}, 401
        return redirect(url_for('login', next=request.full_path if request.query_string else request.path))

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if not LOGIN_ATIVO:
            return redirect('/')
        if request.method == 'GET':
            session['depois_do_login'] = _destino_seguro(request.args.get('next'))
            return _tela('login')

        email = request.form.get('email', '').strip().lower()
        senha = request.form.get('senha', '')
        if _bloqueado(email):
            return _tela('login', email=email, erro='Muitas tentativas erradas. Espere 15 minutos e tente de novo.'), 429
        usuario = registro.buscar_usuario(email) if email else None
        if not check_password_hash(usuario['senha_hash'] if usuario else _HASH_FALSO, senha) or not usuario:
            _registrar_falha(email)
            return _tela('login', email=email, erro='E-mail ou senha incorretos.'), 401
        with _trava_falhas:
            _falhas.pop(email, None)
        if not usuario['confirmado_em']:
            return _tela('pendente', email=email,
                         aviso='Sua conta ainda não foi ativada. Abra o link que enviamos para o seu e-mail.'), 403
        return _entrar(usuario)

    @app.route('/cadastro', methods=['GET', 'POST'])
    def cadastro():
        if not LOGIN_ATIVO:
            return redirect('/')
        if request.method == 'GET':
            return _tela('cadastro')

        nome = ' '.join(request.form.get('nome', '').split())
        email = request.form.get('email', '').strip().lower()
        senha = request.form.get('senha', '')
        erro = None
        if not nome:
            erro = 'Informe seu nome.'
        elif not email_permitido(email):
            erro = f'Use seu e-mail @{DOMINIO_PERMITIDO} para criar a conta.'
        elif len(senha) < SENHA_MINIMA:
            erro = f'A senha precisa ter pelo menos {SENHA_MINIMA} caracteres.'
        elif senha != request.form.get('confirmacao', ''):
            erro = 'As senhas não conferem.'
        if erro:
            return _tela('cadastro', nome=nome, email=email, erro=erro), 400

        usuario = registro.criar_usuario(nome, email, generate_password_hash(senha))
        if not usuario:
            return _tela('cadastro', nome=nome, email=email,
                         erro='Já existe uma conta com esse e-mail. Entre com a sua senha.'), 409
        logger.info('Cadastro aguardando confirmação: %s', email)
        _enviar_confirmacao(usuario)
        return _tela('pendente', email=email)

    @app.route('/confirmar/<token>')
    def confirmar(token):
        if not LOGIN_ATIVO:
            return redirect('/')
        try:
            dados = _assinador().loads(token, max_age=VALIDADE_LINK)
        except SignatureExpired:
            return _tela('login', erro='Esse link de confirmação expirou. Entre com seu e-mail e senha para receber outro.'), 400
        except BadSignature:
            return _tela('login', erro='Link de confirmação inválido.'), 400
        usuario = registro.buscar_usuario(dados.get('email', ''))
        if not usuario or str(usuario['id']) != dados.get('id') or _marca_da_senha(usuario) != dados.get('marca'):
            return _tela('login', erro='Esse link não vale mais: o cadastro foi refeito depois dele. '
                                       'Use o link do e-mail mais recente.'), 400
        if usuario['confirmado_em']:
            # link já usado: não serve de "senha" para entrar de novo
            return _tela('login', email=usuario['email'], aviso='Sua conta já está ativa. Entre com seu e-mail e senha.')
        usuario = registro.confirmar_usuario(usuario['id'])
        logger.info('E-mail confirmado: %s', usuario['email'])
        session['depois_do_login'] = '/'
        return _entrar(usuario)

    @app.route('/confirmar/reenviar', methods=['POST'])
    def reenviar_confirmacao():
        if not LOGIN_ATIVO:
            return redirect('/')
        email = request.form.get('email', '').strip().lower()
        usuario = registro.buscar_usuario(email) if email_permitido(email) else None
        if usuario and not usuario['confirmado_em']:
            _enviar_confirmacao(usuario)
        # mesma resposta exista ou não o cadastro
        return _tela('pendente', email=email, aviso='Se houver um cadastro pendente com esse e-mail, '
                                                    'enviamos um novo link. Pode levar alguns minutos.')

    @app.route('/logout')
    def logout():
        session.clear()
        return redirect(url_for('login') if LOGIN_ATIVO else '/')


def _tela(modo, nome='', email='', erro=None, aviso=None):
    return render_template_string(TELA_ACESSO, modo=modo, nome=nome, email=email, erro=erro, aviso=aviso,
                                  dominio=DOMINIO_PERMITIDO, senha_minima=SENHA_MINIMA)


TELA_ACESSO = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="icon" href="/static/logo.svg" type="image/svg+xml">
<title>{{ {'login': 'Entrar', 'cadastro': 'Criar conta'}.get(modo, 'Confirme seu e-mail') }} · D4Sign Downloader</title>
<style>
  /* mesma identidade visual da página principal (Tema Sitawi): creme, grafite e azul #276DBD */
  :root{--card-bg:#fff;--page-bg:#F2F1EA;--muted:#5F5E57;--border:#E3E1D6;--text-color:#282828;--accent:#276DBD;--accent-hover:#1F5A9E;--accent-soft:#E4EDF8;--btn-text:#fff;--topbar-bg:#276DBD;--topbar-text:#E8E8DC;--erro-bg:#FDECE2;--erro-text:#A33B00;--aviso-bg:#E4EDF8;--aviso-text:#1F5A9E}
  .dark-mode{--card-bg:#242423;--page-bg:#1B1B1A;--muted:#A9A89E;--border:#3A3936;--text-color:#E8E8DC;--accent:#5B9BE0;--accent-hover:#7DB1E8;--accent-soft:#1E3550;--btn-text:#111;--topbar-bg:#1F4F88;--erro-bg:#3D1E0B;--erro-text:#FFC6A3;--aviso-bg:#1E3550;--aviso-text:#C7DDF5}
  *,*::before,*::after{box-sizing:border-box}
  html:has(body.dark-mode){background:#1B1B1A}
  html{height:100%}
  html,body{margin:0;background:var(--page-bg);color:var(--text-color);font-family:"Segoe UI", system-ui, -apple-system, Roboto, Arial, sans-serif}
  body{min-height:100%;display:flex;flex-direction:column}
  .topbar{background:var(--topbar-bg);color:var(--topbar-text);padding:14px 28px;display:flex;align-items:baseline;gap:14px}
  .logo-app{display:block;flex:none;align-self:center;width:34px;height:34px}
  .wordmark{font-weight:700;font-size:30px;letter-spacing:-.04em;line-height:1}
  .app-nome{font-size:15px;font-weight:600;opacity:.9}
  .conteudo{flex:1;display:flex;align-items:center;justify-content:center;padding:32px 16px}
  .card{width:100%;max-width:420px;background:var(--card-bg);border:1px solid var(--border);border-radius:16px;padding:32px;box-shadow:0 1px 2px rgba(40,40,40,.04), 0 6px 20px rgba(40,40,40,.06)}
  h1{margin:0 0 6px;font-size:26px;letter-spacing:-0.02em}
  .sub{margin:0 0 24px;color:var(--muted);font-size:15px}
  form{display:flex;flex-direction:column;gap:16px}
  label{display:flex;flex-direction:column;gap:6px;font-size:13px;font-weight:600;color:var(--muted)}
  input{height:44px;padding:0 12px;border:1px solid var(--border);border-radius:10px;background:var(--card-bg);color:var(--text-color);font:inherit;font-size:15px}
  input:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-soft)}
  button{margin-top:4px;height:46px;border:0;border-radius:10px;background:var(--accent);color:var(--btn-text);font:inherit;font-size:15px;font-weight:600;cursor:pointer}
  button:hover{background:var(--accent-hover)}
  .erro,.aviso{margin:0 0 16px;padding:12px 14px;border-radius:10px;background:var(--erro-bg);color:var(--erro-text);font-size:14px}
  .aviso{background:var(--aviso-bg);color:var(--aviso-text)}
  .texto{margin:0 0 20px;font-size:15px;line-height:1.55}
  button.secundario{background:transparent;color:var(--text-color);border:1px solid var(--border)}
  button.secundario:hover{border-color:var(--muted)}
  .troca{margin:20px 0 0;text-align:center;font-size:14px;color:var(--muted)}
  .troca a{color:var(--accent);font-weight:600}
  @media (max-width:480px){.card{padding:24px}.topbar{padding:12px 16px}.wordmark{font-size:26px}.logo-app{width:30px;height:30px}}
</style>
<script>
  // mesmo tema escolhido na página principal (abre no claro; escuro só se a pessoa escolheu)
  try{ if(localStorage.getItem('d4sign:dark_mode') === 'dark') document.documentElement.classList.add('tema-escuro'); }catch(e){}
</script>
</head>
<body>
<script>if(document.documentElement.classList.contains('tema-escuro')) document.body.classList.add('dark-mode');</script>
<header class="topbar"><img class="logo-app" src="/static/logo.svg" alt="" width="34" height="34"><span class="wordmark" aria-label="Sitawi">stwi</span><span class="app-nome">D4Sign Downloader</span></header>
<div class="conteudo">
<main class="card">
  {% if modo == 'login' %}
  <h1>Entrar</h1>
  <p class="sub">D4Sign Downloader</p>
  {% if erro %}<p class="erro" role="alert">{{ erro }}</p>{% endif %}
  {% if aviso %}<p class="aviso" role="status">{{ aviso }}</p>{% endif %}
  <form method="post" action="/login">
    <label>E-mail<input type="email" name="email" value="{{ email }}" autocomplete="username" required autofocus></label>
    <label>Senha<input type="password" name="senha" autocomplete="current-password" required></label>
    <button type="submit">Entrar</button>
  </form>
  <p class="troca">Ainda não tem conta? <a href="/cadastro">Criar conta</a></p>
  {% elif modo == 'cadastro' %}
  <h1>Criar conta</h1>
  <p class="sub">Disponível para e-mails @{{ dominio }}</p>
  {% if erro %}<p class="erro" role="alert">{{ erro }}</p>{% endif %}
  <form method="post" action="/cadastro">
    <label>Nome<input type="text" name="nome" value="{{ nome }}" autocomplete="name" required autofocus></label>
    <label>E-mail<input type="email" name="email" value="{{ email }}" autocomplete="username" placeholder="voce@{{ dominio }}" required></label>
    <label>Senha<input type="password" name="senha" autocomplete="new-password" minlength="{{ senha_minima }}" required></label>
    <label>Confirme a senha<input type="password" name="confirmacao" autocomplete="new-password" minlength="{{ senha_minima }}" required></label>
    <button type="submit">Criar conta</button>
  </form>
  <p class="troca">Já tem conta? <a href="/login">Entrar</a></p>
  {% else %}
  <h1>Confirme seu e-mail</h1>
  <p class="sub">D4Sign Downloader</p>
  {% if aviso %}<p class="aviso" role="status">{{ aviso }}</p>{% endif %}
  <p class="texto">Enviamos um link de ativação para <strong>{{ email }}</strong>. Abra o e-mail e clique no link
    para entrar. Ele vale por 24 horas. Não chegou? Confira a caixa de spam.</p>
  <form method="post" action="/confirmar/reenviar">
    <input type="hidden" name="email" value="{{ email }}">
    <button type="submit" class="secundario">Reenviar link</button>
  </form>
  <p class="troca"><a href="/login">Voltar para o login</a></p>
  {% endif %}
</main>
</div>
</body>
</html>"""


EMAIL_CONFIRMACAO = """<!DOCTYPE html>
<html lang="pt-BR"><body style="margin:0;padding:24px;background:#F2F1EA;font-family:'Segoe UI',Arial,Helvetica,sans-serif;color:#282828">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr><td align="center">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:480px;background:#fff;border:1px solid #E3E1D6;border-radius:14px">
      <tr><td style="padding:28px">
        <h1 style="margin:0 0 16px;font-size:22px">Olá, {{ nome }}!</h1>
        <p style="margin:0 0 22px;font-size:15px;line-height:1.5">Para ativar sua conta no D4Sign Downloader, clique no botão abaixo. O link vale por 24 horas.</p>
        <p style="margin:0 0 22px"><a href="{{ link }}" style="display:inline-block;padding:12px 22px;background:#276DBD;color:#fff;border-radius:10px;text-decoration:none;font-weight:bold">Ativar minha conta</a></p>
        <p style="margin:0 0 8px;font-size:13px;color:#5F5E57">Se o botão não funcionar, copie este endereço no navegador:</p>
        <p style="margin:0 0 22px;font-size:13px;word-break:break-all"><a href="{{ link }}" style="color:#276DBD">{{ link }}</a></p>
        <p style="margin:0;font-size:13px;color:#5F5E57">Se você não pediu esse cadastro, ignore este e-mail.</p>
      </td></tr>
    </table>
  </td></tr></table>
</body></html>"""


# ---------------------------------------------------------------------------------------------
# Login com conta Microsoft (Microsoft Entra ID) — DESATIVADO, aguardando liberação do T.I.
#
# Para ativar: registrar o app no Entra (single-tenant, redirect URI <site>/auth/callback),
# configurar MS_CLIENT_ID, MS_TENANT_ID e MS_CLIENT_SECRET, descomentar `msal` no
# requirements.txt e trocar as rotas /login, /cadastro e /logout acima pelas de baixo
# (incluindo 'auth_callback' em ROTAS_PUBLICAS). O id da pessoa passa a ser o oid da Microsoft.
#
# import msal
# from urllib.parse import quote
#
# CLIENT_ID = os.environ.get('MS_CLIENT_ID', '').strip()
# TENANT_ID = os.environ.get('MS_TENANT_ID', '').strip()
# CLIENT_SECRET = os.environ.get('MS_CLIENT_SECRET', '').strip()
# REDIRECT_URI = os.environ.get('MS_REDIRECT_URI', '').strip()  # opcional; padrão: <site>/auth/callback
#
# def _cliente():
#     return msal.ConfidentialClientApplication(
#         CLIENT_ID,
#         authority=f'https://login.microsoftonline.com/{TENANT_ID}',
#         client_credential=CLIENT_SECRET,
#     )
#
# def _redirect_uri():
#     return REDIRECT_URI or url_for('auth_callback', _external=True)
#
# @app.route('/login')
# def login():
#     session['depois_do_login'] = _destino_seguro(request.args.get('next'))
#     session['fluxo_login'] = _cliente().initiate_auth_code_flow(scopes=[], redirect_uri=_redirect_uri())
#     return redirect(session['fluxo_login']['auth_uri'])
#
# @app.route('/auth/callback')
# def auth_callback():
#     fluxo = session.pop('fluxo_login', None)
#     if not fluxo:
#         return redirect(url_for('login'))
#     try:
#         resultado = _cliente().acquire_token_by_auth_code_flow(fluxo, request.args.to_dict())
#     except ValueError:
#         # state inválido ou fluxo expirado: recomeça o login
#         return redirect(url_for('login'))
#     if 'error' in resultado:
#         logger.warning('Falha no login Microsoft: %s', resultado.get('error_description') or resultado.get('error'))
#         return ('Não foi possível entrar com a conta Microsoft. Verifique se você usa a conta da empresa '
#                 'e tente de novo.'), 403
#     claims = resultado.get('id_token_claims') or {}
#     return _entrar({
#         'id': claims.get('oid') or claims.get('sub'),
#         'nome': claims.get('name') or claims.get('preferred_username'),
#         'email': claims.get('preferred_username') or claims.get('email'),
#     })
#
# @app.route('/logout')
# def logout():
#     session.clear()
#     volta = quote(url_for('index', _external=True), safe='')
#     return redirect(f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/logout?post_logout_redirect_uri={volta}')
# ---------------------------------------------------------------------------------------------
