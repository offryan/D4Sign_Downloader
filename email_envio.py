"""Envio de e-mails do app (hoje só o link de confirmação de conta).

O plano gratuito do Render bloqueia as portas SMTP, então o caminho principal é a API HTTP da
Brevo (gratuita até 300 e-mails/dia). Por ordem:
  1. BREVO_API_KEY + EMAIL_REMETENTE  -> API da Brevo
  2. SMTP_HOST (+ SMTP_PORT, SMTP_USUARIO, SMTP_SENHA) + EMAIL_REMETENTE -> SMTP com STARTTLS
     (para desenvolvimento ou servidores pagos)
  3. nenhum dos dois -> o e-mail não sai; o link aparece no log do servidor
"""
import logging
import os
import smtplib
from email.message import EmailMessage

import requests

logger = logging.getLogger(__name__)

BREVO_API_KEY = os.environ.get('BREVO_API_KEY', '').strip()
SMTP_HOST = os.environ.get('SMTP_HOST', '').strip()
REMETENTE = os.environ.get('EMAIL_REMETENTE', '').strip()
NOME_REMETENTE = os.environ.get('EMAIL_REMETENTE_NOME', 'D4Sign Downloader').strip()


def configurado():
    return bool(REMETENTE and (BREVO_API_KEY or SMTP_HOST))


def enviar(para, nome, assunto, texto, html):
    """Envia o e-mail; devolve True se o provedor aceitou."""
    if not configurado():
        logger.warning('Envio de e-mail não configurado (BREVO_API_KEY ou SMTP_HOST + EMAIL_REMETENTE). '
                       'E-mail para %s não enviado:\n%s', para, texto)
        return False
    try:
        if BREVO_API_KEY:
            r = requests.post('https://api.brevo.com/v3/smtp/email', timeout=15,
                              headers={'api-key': BREVO_API_KEY, 'accept': 'application/json'},
                              json={'sender': {'name': NOME_REMETENTE, 'email': REMETENTE},
                                    'to': [{'email': para, 'name': nome}],
                                    'subject': assunto, 'textContent': texto, 'htmlContent': html})
            if r.status_code >= 300:
                logger.error('Brevo recusou o e-mail para %s: %s %s', para, r.status_code, r.text[:300])
                return False
            return True
        msg = EmailMessage()
        msg['From'] = f'{NOME_REMETENTE} <{REMETENTE}>'
        msg['To'] = para
        msg['Subject'] = assunto
        msg.set_content(texto)
        msg.add_alternative(html, subtype='html')
        with smtplib.SMTP(SMTP_HOST, int(os.environ.get('SMTP_PORT', '587')), timeout=15) as smtp:
            smtp.starttls()
            if os.environ.get('SMTP_USUARIO'):
                smtp.login(os.environ['SMTP_USUARIO'], os.environ.get('SMTP_SENHA', ''))
            smtp.send_message(msg)
        return True
    except Exception:
        logger.exception('Falha enviando e-mail para %s', para)
        return False
