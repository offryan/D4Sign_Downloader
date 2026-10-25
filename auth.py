"""Login com conta Microsoft (Microsoft Entra ID).

Fica ativo quando MS_CLIENT_ID, MS_TENANT_ID e MS_CLIENT_SECRET estão configuradas. O app é
registrado como "single-tenant", então só contas da organização dona do registro conseguem entrar.
Sem essas variáveis o app funciona sem login, como antes, e o registro de baixados é compartilhado.
"""
import logging
import os
import secrets
from urllib.parse import quote

import msal
from flask import redirect, request, session, url_for

logger = logging.getLogger(__name__)

CLIENT_ID = os.environ.get('MS_CLIENT_ID', '').strip()
TENANT_ID = os.environ.get('MS_TENANT_ID', '').strip()
CLIENT_SECRET = os.environ.get('MS_CLIENT_SECRET', '').strip()
REDIRECT_URI = os.environ.get('MS_REDIRECT_URI', '').strip()  # opcional; padrão: <site>/auth/callback

LOGIN_ATIVO = bool(CLIENT_ID and TENANT_ID and CLIENT_SECRET)

# Rotas que funcionam sem login: o próprio fluxo de login e o webhook chamado pela D4Sign
ROTAS_PUBLICAS = {'login', 'auth_callback', 'logout', 'webhook_d4sign', 'static'}


def _cliente():
    return msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f'https://login.microsoftonline.com/{TENANT_ID}',
        client_credential=CLIENT_SECRET,
    )


def _redirect_uri():
    return REDIRECT_URI or url_for('auth_callback', _external=True)


def usuario_atual():
    """Pessoa logada ({id, nome, email}) ou None quando o login está desligado."""
    return session.get('usuario') if LOGIN_ATIVO else None


def configurar(app):
    app.secret_key = os.environ.get('SECRET_KEY') or secrets.token_hex(32)
    app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE='Lax',
                      # no Render (HTTPS) o cookie de sessão só trafega por conexão segura
                      SESSION_COOKIE_SECURE=bool(os.environ.get('RENDER')),
                      PERMANENT_SESSION_LIFETIME=60 * 60 * 12)

    if not LOGIN_ATIVO:
        logger.warning('Login Microsoft desligado: configure MS_CLIENT_ID, MS_TENANT_ID e MS_CLIENT_SECRET para ativar.')
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

    @app.route('/login')
    def login():
        if not LOGIN_ATIVO:
            return redirect('/')
        destino = request.args.get('next') or '/'
        session['depois_do_login'] = destino if destino.startswith('/') and not destino.startswith('//') else '/'
        session['fluxo_login'] = _cliente().initiate_auth_code_flow(scopes=[], redirect_uri=_redirect_uri())
        return redirect(session['fluxo_login']['auth_uri'])

    @app.route('/auth/callback')
    def auth_callback():
        fluxo = session.pop('fluxo_login', None)
        if not fluxo:
            return redirect(url_for('login'))
        try:
            resultado = _cliente().acquire_token_by_auth_code_flow(fluxo, request.args.to_dict())
        except ValueError:
            # state inválido ou fluxo expirado: recomeça o login
            return redirect(url_for('login'))
        if 'error' in resultado:
            logger.warning('Falha no login Microsoft: %s', resultado.get('error_description') or resultado.get('error'))
            return ('Não foi possível entrar com a conta Microsoft. Verifique se você usa a conta da empresa '
                    'e tente de novo.'), 403
        claims = resultado.get('id_token_claims') or {}
        destino = session.get('depois_do_login', '/')
        session.clear()
        session['usuario'] = {
            'id': claims.get('oid') or claims.get('sub'),
            'nome': claims.get('name') or claims.get('preferred_username'),
            'email': claims.get('preferred_username') or claims.get('email'),
        }
        session.permanent = True
        return redirect(destino)

    @app.route('/logout')
    def logout():
        session.clear()
        if not LOGIN_ATIVO:
            return redirect('/')
        volta = quote(url_for('index', _external=True), safe='')
        return redirect(f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/logout?post_logout_redirect_uri={volta}')
