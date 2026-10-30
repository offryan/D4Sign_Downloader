"""Login do app.

Hoje: cadastro e login com e-mail e senha, salvos no Supabase (tabela usuarios). Só e-mails do
domínio DOMINIO_PERMITIDO (padrão: sitawi.net) conseguem se cadastrar, e a conta só entra depois
que a pessoa abre o link de confirmação enviado para o e-mail (ver email_envio.py). Fica ativo
quando DATABASE_URL está configurada; sem banco o app funciona sem login, como antes.

Login com conta Microsoft (Entra ID): liga sozinho quando MS_CLIENT_ID, MS_TENANT_ID e
MS_CLIENT_SECRET estão configuradas (além do DATABASE_URL). Aí a tela de login mostra só o botão
"Entrar com a conta Microsoft" e o cadastro com senha sai do ar. A pessoa continua sendo a mesma
linha de usuarios, achada pelo e-mail, então o histórico de downloads de quem já usava o app segue
ligado à conta. Sem as três variáveis, vale o login com e-mail e senha.
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

# Login Microsoft (Entra ID, app single-tenant com redirect URI <site>/auth/callback)
MS_CLIENT_ID = os.environ.get('MS_CLIENT_ID', '').strip()
MS_TENANT_ID = os.environ.get('MS_TENANT_ID', '').strip()
MS_CLIENT_SECRET = os.environ.get('MS_CLIENT_SECRET', '').strip()
MS_REDIRECT_URI = os.environ.get('MS_REDIRECT_URI', '').strip()  # opcional; padrão: <site>/auth/callback
MS_ATIVO = LOGIN_ATIVO and bool(MS_CLIENT_ID and MS_TENANT_ID and MS_CLIENT_SECRET)
if MS_ATIVO:
    import msal

# Rotas que funcionam sem login: as telas de acesso e o webhook chamado pela D4Sign
ROTAS_PUBLICAS = {'login', 'login_microsoft', 'auth_callback', 'cadastro', 'confirmar', 'reenviar_confirmacao',
                  'logout', 'webhook_d4sign', 'static', 'favicon'}

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


def _cliente_microsoft():
    return msal.ConfidentialClientApplication(MS_CLIENT_ID, client_credential=MS_CLIENT_SECRET,
                                              authority=f'https://login.microsoftonline.com/{MS_TENANT_ID}')


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
    if LOGIN_ATIVO and not MS_ATIVO and (MS_CLIENT_ID or MS_TENANT_ID or MS_CLIENT_SECRET):
        logger.warning('Login Microsoft desligado: faltam variáveis (MS_CLIENT_ID, MS_TENANT_ID e MS_CLIENT_SECRET). '
                       'Valendo o login com e-mail e senha.')
    elif MS_ATIVO:
        logger.info('Login com conta Microsoft ativo.')

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
            return _tela('microsoft' if MS_ATIVO else 'login')
        if MS_ATIVO:
            return redirect(url_for('login'))

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

    @app.route('/login/microsoft')
    def login_microsoft():
        if not MS_ATIVO:
            return redirect(url_for('login'))
        redirect_uri = MS_REDIRECT_URI or url_for('auth_callback', _external=True)
        try:
            session['fluxo_login'] = _cliente_microsoft().initiate_auth_code_flow(scopes=[], redirect_uri=redirect_uri)
        except Exception:
            # tenant errado ou Microsoft fora do ar: o MSAL não consegue ler a configuração do tenant
            logger.exception('Não foi possível iniciar o login Microsoft (confira MS_TENANT_ID)')
            return _tela('microsoft', erro='O login com a conta Microsoft está indisponível agora. Tente de novo mais tarde.'), 503
        return redirect(session['fluxo_login']['auth_uri'])

    @app.route('/auth/callback')
    def auth_callback():
        if not MS_ATIVO:
            return redirect(url_for('login'))
        fluxo = session.pop('fluxo_login', None)
        if not fluxo:
            return redirect(url_for('login'))
        try:
            resultado = _cliente_microsoft().acquire_token_by_auth_code_flow(fluxo, request.args.to_dict())
        except ValueError:
            # state inválido ou fluxo expirado: recomeça o login
            return redirect(url_for('login'))
        if 'error' in resultado:
            logger.warning('Falha no login Microsoft: %s', resultado.get('error_description') or resultado.get('error'))
            return _tela('microsoft', erro='Não foi possível entrar com a conta Microsoft. Tente de novo.'), 403
        claims = resultado.get('id_token_claims') or {}
        email = (claims.get('preferred_username') or claims.get('email') or '').strip().lower()
        if not email_permitido(email):
            logger.warning('Login Microsoft recusado (fora do domínio): %s', email)
            return _tela('microsoft', erro=f'Entre com a sua conta @{DOMINIO_PERMITIDO}.'), 403
        # mesma conta de quem já tinha cadastro com senha: o histórico de downloads continua ligado a ela
        usuario = registro.usuario_microsoft(claims.get('name') or email.split('@')[0], email,
                                             generate_password_hash(secrets.token_hex(32)))
        logger.info('Login Microsoft: %s', email)
        return _entrar(usuario)

    @app.route('/cadastro', methods=['GET', 'POST'])
    def cadastro():
        if not LOGIN_ATIVO or MS_ATIVO:
            return redirect(url_for('login') if MS_ATIVO else '/')
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
        if not LOGIN_ATIVO or MS_ATIVO:
            return redirect(url_for('login') if MS_ATIVO else '/')
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
        if not LOGIN_ATIVO or MS_ATIVO:
            return redirect(url_for('login') if MS_ATIVO else '/')
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
<title>{{ {'login': 'Entrar', 'microsoft': 'Entrar', 'cadastro': 'Criar conta'}.get(modo, 'Confirme seu e-mail') }} · D4Sign Downloader</title>
<style>
  /* mesma identidade visual da página principal (Tema Sitawi): creme, grafite e azul #276DBD */
  :root{--card-bg:#fff;--page-bg:#F2F1EA;--muted:#5F5E57;--border:#E3E1D6;--text-color:#282828;--accent:#276DBD;--accent-hover:#1F5A9E;--accent-soft:#E4EDF8;--btn-text:#fff;--painel-bg:#276DBD;--painel-text:#E8E8DC;--erro-bg:#FDECE2;--erro-text:#A33B00;--aviso-bg:#E4EDF8;--aviso-text:#1F5A9E;--ms-bg:#fff;--ms-text:#5E5E5E;--ms-border:#8C8C8C;--ms-hover:#F3F3F3}
  .dark-mode{--card-bg:#242423;--page-bg:#1B1B1A;--muted:#A9A89E;--border:#3A3936;--text-color:#E8E8DC;--accent:#5B9BE0;--accent-hover:#7DB1E8;--accent-soft:#1E3550;--btn-text:#111;--painel-bg:#1F4F88;--erro-bg:#3D1E0B;--erro-text:#FFC6A3;--aviso-bg:#1E3550;--aviso-text:#C7DDF5;--ms-bg:#2F2F2F;--ms-text:#fff;--ms-border:#2F2F2F;--ms-hover:#3B3B3B}
  *,*::before,*::after{box-sizing:border-box}
  html:has(body.dark-mode){background:#1B1B1A}
  html{height:100%}
  html,body{margin:0;background:var(--page-bg);color:var(--text-color);font-family:"Segoe UI", system-ui, -apple-system, Roboto, Arial, sans-serif}
  body{min-height:100%;display:grid;grid-template-columns:minmax(360px,42%) 1fr}
  /* painel da marca: logo oficial da Sitawi sobre o azul */
  .painel{background:var(--painel-bg);color:var(--painel-text);padding:48px 56px;display:flex;flex-direction:column;justify-content:space-between;gap:40px;position:relative;overflow:hidden}
  .painel::after{content:"";position:absolute;right:-120px;bottom:-120px;width:360px;height:360px;border-radius:50%;background:rgba(232,232,220,.07);pointer-events:none}
  .logo-sitawi{display:block;width:220px;height:auto}
  .painel-meio{max-width:420px}
  .app-selo{display:flex;align-items:center;gap:12px;margin-bottom:20px}
  .app-selo img{width:44px;height:44px;border-radius:10px}
  .app-selo span{font-size:15px;font-weight:600;letter-spacing:.01em;opacity:.92}
  .painel h2{margin:0 0 12px;font-size:34px;line-height:1.15;letter-spacing:-.02em;color:#fff}
  .painel p{margin:0;font-size:16px;line-height:1.55;opacity:.88}
  .creditos{font-size:12px;opacity:.75}
  .creditos a{color:inherit;text-decoration:underline;text-underline-offset:2px}
  .lado{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px 24px}
  .lado .creditos{display:none;margin-top:20px;color:var(--muted)}
  .card{width:100%;max-width:420px;background:var(--card-bg);border:1px solid var(--border);border-radius:16px;padding:36px;box-shadow:0 1px 2px rgba(40,40,40,.04), 0 6px 20px rgba(40,40,40,.06)}
  .card-topo{display:none}
  .logo-card{display:block;width:150px;height:auto;margin:0 0 22px}
  .logo-card.escuro, .dark-mode .logo-card.claro{display:none}
  .dark-mode .logo-card.escuro{display:block}
  h1{margin:0 0 6px;font-size:26px;letter-spacing:-0.02em}
  .sub{margin:0 0 24px;color:var(--muted);font-size:15px}
  form{display:flex;flex-direction:column;gap:16px}
  label{display:flex;flex-direction:column;gap:6px;font-size:13px;font-weight:600;color:var(--muted)}
  input{height:44px;padding:0 12px;border:1px solid var(--border);border-radius:10px;background:var(--card-bg);color:var(--text-color);font:inherit;font-size:15px}
  input:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-soft)}
  button{margin-top:4px;height:46px;border:0;border-radius:10px;background:var(--accent);color:var(--btn-text);font:inherit;font-size:15px;font-weight:600;cursor:pointer}
  button:hover{background:var(--accent-hover)}
  button:focus-visible{outline:none;box-shadow:0 0 0 3px var(--accent-soft), 0 0 0 5px var(--accent)}
  /* botão no padrão "Entrar com a Microsoft": fundo neutro e o logo de quatro quadrados */
  button.ms{display:flex;align-items:center;justify-content:center;gap:12px;background:var(--ms-bg);color:var(--ms-text);border:1px solid var(--ms-border);border-radius:6px}
  button.ms:hover{background:var(--ms-hover)}
  .ms-logo{width:21px;height:21px;flex:none}
  .erro,.aviso{margin:0 0 16px;padding:12px 14px;border-radius:10px;background:var(--erro-bg);color:var(--erro-text);font-size:14px}
  .aviso{background:var(--aviso-bg);color:var(--aviso-text)}
  .texto{margin:0 0 20px;font-size:15px;line-height:1.55}
  .nota{margin:18px 0 0;font-size:13px;line-height:1.5;color:var(--muted)}
  button.secundario{background:transparent;color:var(--text-color);border:1px solid var(--border)}
  button.secundario:hover{border-color:var(--muted)}
  .troca{margin:20px 0 0;text-align:center;font-size:14px;color:var(--muted)}
  .troca a{color:var(--accent);font-weight:600}
  /* tablet e celular: o painel sai e o logo vai para dentro do cartão */
  @media (max-width:860px){
    body{grid-template-columns:1fr}
    .painel{display:none}
    .lado{justify-content:flex-start;padding:32px 16px}
    .card{padding:28px 24px}
    .card-topo, .lado .creditos{display:block}
  }
</style>
<script>
  // mesmo tema escolhido na página principal (abre no claro; escuro só se a pessoa escolheu)
  try{ if(localStorage.getItem('d4sign:dark_mode') === 'dark') document.documentElement.classList.add('tema-escuro'); }catch(e){}
</script>
</head>
<body>
<script>if(document.documentElement.classList.contains('tema-escuro')) document.body.classList.add('dark-mode');</script>
<aside class="painel">
  <img class="logo-sitawi" src="/static/sitawi-branco.svg" alt="Sitawi Finanças do Bem" width="220" height="45">
  <div class="painel-meio">
    <div class="app-selo"><img src="/static/logo.svg" alt="" width="44" height="44"><span>D4Sign Downloader</span></div>
    <h2>Documentos assinados, num só lugar</h2>
    <p>Lista os documentos finalizados de todos os cofres da D4Sign, baixa um a um ou em lote e mostra quem da equipe já baixou cada um.</p>
  </div>
  <div class="creditos">Criado por <a href="https://github.com/offryan" target="_blank" rel="noopener">Ryan G. Santos</a></div>
</aside>
<div class="lado">
<main class="card">
  <div class="card-topo">
    <img class="logo-card claro" src="/static/sitawi-preto.svg" alt="Sitawi Finanças do Bem" width="150" height="31">
    <img class="logo-card escuro" src="/static/sitawi-branco.svg" alt="Sitawi Finanças do Bem" width="150" height="31">
  </div>
  {% if modo == 'microsoft' %}
  <h1>Entrar</h1>
  <p class="sub">D4Sign Downloader</p>
  {% if erro %}<p class="erro" role="alert">{{ erro }}</p>{% endif %}
  <form method="get" action="/login/microsoft">
    <button type="submit" class="ms" autofocus>
      <svg class="ms-logo" viewBox="0 0 21 21" aria-hidden="true"><rect x="1" y="1" width="9" height="9" fill="#F25022"/><rect x="11" y="1" width="9" height="9" fill="#7FBA00"/><rect x="1" y="11" width="9" height="9" fill="#00A4EF"/><rect x="11" y="11" width="9" height="9" fill="#FFB900"/></svg>
      Entrar com a conta Microsoft
    </button>
  </form>
  <p class="nota">Use a sua conta da Sitawi (@{{ dominio }}), a mesma do Outlook e do Teams.</p>
  {% elif modo == 'login' %}
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
<div class="creditos">Criado por <a href="https://github.com/offryan" target="_blank" rel="noopener">Ryan G. Santos</a></div>
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

