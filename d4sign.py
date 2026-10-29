"""Minimal, cleaned and optimized D4Sign app.

Improvements made:
- In-memory TTL cache for external API calls (listar_cofres, listar_documentos)
- Pre-parse dates once and store datetime object for fast filtering
- Limit number of rendered documents to avoid huge HTML payloads
- Log timing for index page generation
"""

from flask import Flask, Response, redirect, render_template, render_template_string, request, jsonify
import requests
import itertools
import uuid
import zipfile
import re
import base64
import logging
import time
from datetime import datetime, timedelta, timezone
import os
import unicodedata
from functools import wraps
import json
import threading
import traceback
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

app = Flask(__name__)

# Load .env for local development (keeps secrets out of code)
try:
    load_dotenv()
except Exception:
    pass

# Logger (define early so modules that run at import can log)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

from werkzeug.middleware.proxy_fix import ProxyFix
import auth
import registro

# O Render termina o HTTPS no proxy: sem isso as URLs de retorno do login sairiam com http://
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
auth.configurar(app)

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
# Provide simple SVG fallbacks for sun/moon using currentColor so CSS can recolor them
if not ICON_SUN_SVG or ICON_SUN_SVG.strip() == '':
    ICON_SUN_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" xmlns="http://www.w3.org/2000/svg" fill="currentColor" aria-hidden="true"><path d="M6.76 4.84l-1.8-1.79L3.17 4.84l1.79 1.8 1.8-1.8zM1 13h3v-2H1v2zm10 8h2v-3h-2v3zm7.03-1.88l1.8 1.79 1.79-1.8-1.79-1.79-1.8 1.8zM20 11v2h3v-2h-3zM4.22 19.78l1.79-1.79-1.79-1.8-1.79 1.8 1.79 1.79zM11 4V1h2v3h-2zm1 4a5 5 0 100 10 5 5 0 000-10z"/></svg>'
if not ICON_MOON_SVG or ICON_MOON_SVG.strip() == '':
    ICON_MOON_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" xmlns="http://www.w3.org/2000/svg" fill="currentColor" aria-hidden="true"><path d="M20.742 13.045A8.088 8.088 0 0111 4a8 8 0 108.742 9.045z"/></svg>'

# Config API D4Sign (use environment variables in deploy)
# Provide sensible defaults for local development; override in production via env.
HOST_D4SIGN = os.environ.get('HOST_D4SIGN', 'https://sandbox.d4sign.com.br/api/v1')
TOKEN_API = os.environ.get('TOKEN_API', '')
CRYPT_KEY = os.environ.get('CRYPT_KEY', '')

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

# Small HTML template (kept inline)
TEMPLATE = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8">
    <title>D4Sign Downloader</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <link rel="icon" href="/static/logo.svg" type="image/svg+xml">
    <style>
        /* Identidade visual Sitawi (Tema Sitawi do Power BI): creme #E8E8DC, grafite #282828,
           azul #276DBD (cor principal), verde #008F42, amarelo #D9D30E, laranja #E45300; fonte Segoe UI */
        :root{
            --page-bg:#F2F1EA;
            --card-bg:#FFFFFF;
            --surface-2:#F8F7F2;
            --text-color:#282828;
            --muted:#5F5E57;
            --border:#E3E1D6;
            --accent:#276DBD;
            --accent-hover:#1F5A9E;
            --accent-soft:#E4EDF8;
            --accent-text:#FFFFFF;
            --topbar-bg:#276DBD;
            --topbar-text:#E8E8DC;
            --btn-bg:var(--accent);
            --btn-text:var(--accent-text);
            --badge-bg:#F1F0EA;
            --selected-count-text:#282828;
            --row-height:60px;
            --row-hover-bg:#F8F7F2;
            --shadow:0 1px 2px rgba(40,40,40,.04), 0 6px 20px rgba(40,40,40,.06);
            --radius:14px;
            --spinner-border:rgba(40,40,40,.12);
            --spinner-top:#276DBD;
            --success-bg:#008F42;
            --error-bg:#E45300;
            --download-badge-bg:#E3F2E9;
            --download-badge-text:#00672F;
            --other-badge-bg:#F7F3C2;
            --other-badge-text:#5E5A00;
            --counter-flash-bg:#276DBD;
            --counter-flash-text:#FFFFFF;
            --modal-bg:#FFFFFF;
            --modal-text:var(--text-color);
        }
        /* Modo escuro: grafite com textos no creme da marca */
        .dark-mode{
            --page-bg:#1B1B1A;
            --card-bg:#242423;
            --surface-2:#2C2C2A;
            --text-color:#E8E8DC;
            --muted:#A9A89E;
            --border:#3A3936;
            --accent:#5B9BE0;
            --accent-hover:#7DB1E8;
            --accent-soft:#1E3550;
            --accent-text:#111111;
            --topbar-bg:#1F4F88;
            --topbar-text:#E8E8DC;
            --badge-bg:#2F2F2D;
            --selected-count-text:#E8E8DC;
            --row-hover-bg:#2A2A28;
            --shadow:0 1px 2px rgba(0,0,0,.3), 0 6px 20px rgba(0,0,0,.25);
            --spinner-border:rgba(232,232,220,.15);
            --spinner-top:#5B9BE0;
            --success-bg:#00A84E;
            --error-bg:#F06A1F;
            --download-badge-bg:#123D26;
            --download-badge-text:#BFE8CF;
            --other-badge-bg:#3D3A07;
            --other-badge-text:#EDE77A;
            --counter-flash-bg:#5B9BE0;
            --counter-flash-text:#111111;
            --modal-bg:#242423;
        }
        *,*::before,*::after{box-sizing:border-box}
        /* o modo escuro fica no <body>; sem isso a margem do topo aparecia clara */
        html:has(body.dark-mode){background:#1B1B1A}
        html{height:100%}
        html,body{margin:0;background:var(--page-bg);color:var(--text-color);font-family:"Segoe UI", system-ui, -apple-system, Roboto, Arial, sans-serif;font-size:15px;line-height:1.45;transition:background-color .25s ease,color .25s ease}
        body{min-height:100%}
        a{color:var(--accent)}
        :focus-visible{outline:2px solid var(--accent);outline-offset:2px}

        /* Faixa do topo com a marca */
        .topbar{background:var(--topbar-bg);color:var(--topbar-text)}
        .topbar-inner{max-width:1400px;margin:0 auto;padding:14px 28px;display:flex;align-items:center;justify-content:space-between;gap:16px}
        .marca{display:flex;align-items:baseline;gap:14px;min-width:0}
        .logo-app{display:block;flex:none;align-self:center;width:34px;height:34px}
        .wordmark{font-family:"Segoe UI", system-ui, sans-serif;font-weight:700;font-size:30px;letter-spacing:-.04em;line-height:1;color:var(--topbar-text)}
        .app-nome{font-size:15px;font-weight:600;opacity:.9;white-space:nowrap}
        .topbar-acoes{display:flex;align-items:center;gap:14px}
        .usuario-logado{font-size:14px;white-space:nowrap;color:var(--topbar-text)}
        .usuario-logado a{color:var(--topbar-text);font-weight:600}

        .page{max-width:1400px;margin:0 auto;padding:32px 28px 48px;display:flex;flex-direction:column;gap:24px}
        .page-head h1{margin:0;font-size:28px;font-weight:700;letter-spacing:-.02em}
        .page-head .sub{margin:6px 0 0;color:var(--muted);font-size:15px}
        .card{background:var(--card-bg);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);transition:background-color .25s ease,border-color .25s ease}

        /* Filtros: cartão no topo, em grade (no celular, um embaixo do outro) */
        .filtros{padding:20px 24px}
        .controls{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px 20px;align-items:end}
        .controls .group{display:flex;flex-direction:column;gap:6px;position:relative;min-width:0}
        label{font-size:13px;font-weight:600;color:var(--muted)}
        select,input[type=text],input[type=date]{width:100%;height:42px;padding:0 12px;border:1px solid var(--border);border-radius:10px;background:var(--card-bg);color:var(--text-color);font:inherit;font-size:14px;transition:border-color .15s ease,box-shadow .15s ease}
        select:hover,input[type=text]:hover{border-color:var(--muted)}
        select:focus,input[type=text]:focus,input[type=date]:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-soft)}
        select option{background:var(--card-bg);color:var(--text-color)}
        .campo-periodo{display:flex;gap:8px;align-items:center}
        .campo-periodo input{flex:1;cursor:pointer}
        .btn-icone{flex:0 0 42px;height:42px;display:inline-grid;place-items:center;border:1px solid var(--border);border-radius:10px;background:var(--surface-2);color:var(--text-color);cursor:pointer;font-size:17px}
        .btn-icone:hover{border-color:var(--accent)}
        .range-popup{position:absolute;top:100%;left:0;margin-top:6px;z-index:80;background:var(--card-bg);padding:14px;border:1px solid var(--border);box-shadow:var(--shadow);border-radius:12px;min-width:300px}
        .range-popup .datas{display:flex;gap:8px;align-items:center}
        .range-popup .acoes{display:flex;gap:8px;justify-content:flex-end;margin-top:12px}
        .range-error{margin-top:8px;color:var(--error-bg);font-size:13px}

        /* Resumo em cartões pequenos */
        .resumo{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}
        .stat{padding:16px 20px;display:flex;flex-direction:column;gap:4px}
        .stat-label{font-size:13px;font-weight:600;color:var(--muted)}
        .stat-val{font-size:26px;font-weight:700;letter-spacing:-.02em;line-height:1.15;align-self:flex-start;border-radius:8px;padding:0 4px;margin-left:-4px}
        .stat.mim .stat-val{color:var(--success-bg)}
        .stat.outros .stat-val{color:var(--other-badge-text)}
        .dark-mode .stat.outros .stat-val{color:var(--other-badge-text)}

        /* Tabela */
        .tabela{padding:8px 8px 0;overflow:hidden}
        table{width:100%;border-collapse:separate;border-spacing:0}
        thead th{font-size:13px;font-weight:600;color:var(--muted);text-align:left;padding:14px 16px;border-bottom:1px solid var(--border);background:var(--card-bg);white-space:nowrap}
        tbody td{padding:16px;border-bottom:1px solid var(--border);color:var(--text-color);vertical-align:middle}
        tbody tr:last-child td{border-bottom:none}
        tbody tr{transition:background-color .12s ease}
        tbody tr:hover td{background:var(--row-hover-bg)}
        .doc-name{font-weight:600;min-width:240px}
        .date-col{color:var(--muted);white-space:nowrap}
        input[type=checkbox]{width:18px;height:18px;accent-color:var(--accent);cursor:pointer}
        .master-wrap{display:flex;align-items:center;gap:10px}
        #selected-count{font-size:12px;font-weight:600;color:var(--selected-count-text);background:var(--badge-bg);padding:4px 9px;border-radius:999px;min-width:26px;text-align:center}
        th.sortable{cursor:pointer;user-select:none}
        th.sortable:hover,th.sortable.active-sort{color:var(--text-color)}
        #sort-arrow, #sort-arrow-data{display:inline-flex;align-items:center;margin-left:6px;color:inherit}
        .sort-icon{width:14px;height:14px;display:inline-flex;align-items:center;justify-content:center;vertical-align:middle;line-height:1;color:inherit}
        .sort-icon svg{width:100%;height:100%;display:block}
        .sort-icon svg, .sort-icon svg *, #dark-mode-toggle svg, #sort-arrow svg, #sort-arrow svg *, #sort-arrow-data svg, #sort-arrow-data svg *{fill:currentColor !important;stroke:currentColor !important}
        .sort-icon i{font-style:normal;font-size:10px;line-height:10px;display:inline-block}
        .table-container{background:transparent}
        .table-container::-webkit-scrollbar{width:10px}
        .table-container::-webkit-scrollbar-thumb{background:rgba(40,40,40,.18);border-radius:8px}
        .table-container::-webkit-scrollbar-track{background:transparent}
        .dark-mode .table-container::-webkit-scrollbar-thumb{background:rgba(232,232,220,.16)}
        .table-container{scrollbar-color:rgba(40,40,40,.18) transparent}
        .dark-mode .table-container{scrollbar-color:rgba(232,232,220,.16) transparent}
        .vazio{padding:48px 16px;text-align:center;color:var(--muted)}

        /* Rodapé da tabela com o botão de download */
        .download-area{display:flex;justify-content:space-between;align-items:center;gap:16px;padding:16px;border-top:1px solid var(--border);margin:0 -8px;background:var(--surface-2)}
        .download-area .dica{color:var(--muted);font-size:14px}
        .download-btn{background:var(--btn-bg);color:var(--btn-text);height:44px;padding:0 22px;border-radius:10px;border:none;cursor:pointer;font:inherit;font-weight:600;font-size:15px;transition:background-color .15s ease}
        .download-btn:hover{background:var(--accent-hover)}
        .btn-secundario{background:transparent;color:var(--text-color);height:38px;padding:0 16px;border-radius:10px;border:1px solid var(--border);cursor:pointer;font:inherit;font-size:14px}
        .btn-secundario:hover{border-color:var(--muted)}
        .range-popup .download-btn{height:38px;padding:0 16px;font-size:14px}

        /* Botão de tema (fica na faixa azul) */
        #dark-mode-toggle{width:38px;height:38px;border:1px solid rgba(232,232,220,.45);border-radius:10px;cursor:pointer;background:transparent !important;color:var(--topbar-text) !important;display:inline-grid;place-items:center}
        #dark-mode-toggle:hover{background:rgba(232,232,220,.12) !important}
        #dark-mode-toggle .toggle-icon{grid-area:1/1;display:inline-block;line-height:0;transition:opacity .25s ease,transform .25s ease;opacity:0;transform:scale(.9);width:18px;height:18px}
        #dark-mode-toggle .toggle-icon svg{width:18px;height:18px;display:block}
        #dark-mode-toggle .toggle-icon.visible{opacity:1;transform:scale(1)}

        .spinner{width:10px;height:10px;border:1.5px solid var(--spinner-border);border-top-color:var(--spinner-top);border-radius:50%;display:inline-block;vertical-align:middle;animation:spin .8s linear infinite}
        @keyframes spin{from{transform:rotate(0)}to{transform:rotate(360deg)}}

        /* Lista com rolagem própria quando há muitos documentos; cabeçalho fixo */
        .table-container.scroll-enabled{max-height:calc(var(--row-height) * 9);overflow-y:auto;scroll-behavior:smooth;-webkit-overflow-scrolling:touch}
        @media (min-width:721px){
            .table-container.scroll-enabled{max-height:max(320px, min(calc(var(--row-height) * 10), calc(100vh - 470px)))}
            .table-container.scroll-enabled thead th{position:sticky;top:0;z-index:3}
        }

        /* Celular: filtros empilhados e cada documento vira um cartão */
        @media (max-width:720px){
            .topbar-inner{padding:12px 16px}
            .wordmark{font-size:26px}
            .logo-app{width:30px;height:30px}
            .app-nome{display:none}
            .page{padding:20px 14px 32px;gap:18px}
            .page-head h1{font-size:24px}
            .filtros{padding:16px}
            .controls{grid-template-columns:1fr;gap:14px}
            .resumo{grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}
            .stat{padding:12px 14px}
            .stat-label{font-size:12px}
            .stat-val{font-size:22px}
            .tabela{background:transparent;border:none;box-shadow:none;padding:0;overflow:visible}
            table thead{display:none}
            table, tbody, tr, td{display:block;width:100%}
            tbody tr{margin:0 0 14px;padding:6px 16px;border-radius:var(--radius);background:var(--card-bg);border:1px solid var(--border);box-shadow:var(--shadow)}
            tbody tr:hover td{background:transparent}
            tbody td{display:flex;justify-content:space-between;align-items:center;gap:14px;padding:12px 0;text-align:right;overflow-wrap:anywhere}
            tbody tr td:last-child{border-bottom:none}
            tbody td::before{content:attr(data-label);color:var(--muted);font-size:13px;font-weight:600;text-align:left;flex-shrink:0}
            tbody td.doc-name{display:block;text-align:left;font-size:16px;min-width:0}
            tbody td.doc-name::before{content:none}
            .download-area{flex-direction:column;align-items:stretch;margin:0;border:1px solid var(--border);border-radius:var(--radius);position:sticky;bottom:12px;box-shadow:var(--shadow);background:var(--card-bg)}
            .download-area .download-btn{width:100%}
            .remover-baixado{width:24px;height:24px;font-size:17px}
            .baixado-por .pessoas{justify-content:flex-end}
            .range-popup{min-width:0;left:0;right:0}
            .range-popup .datas{flex-direction:column;align-items:stretch}
            .voltar-topo.visivel{display:flex}
        }
        /* Rodapé com os créditos: pequeno e apagado, só uma menção */
        .creditos{max-width:1400px;margin:0 auto;padding:0 28px 24px;text-align:center;font-size:12px;color:var(--muted);opacity:.7}
        .creditos a{color:inherit;text-decoration:underline;text-underline-offset:2px}
        .creditos a:hover{color:var(--text-color)}
        /* Seta de voltar ao topo: só no celular, depois de rolar a lista; fica acima da barra de download */
        .voltar-topo{display:none;position:fixed;right:16px;bottom:calc(140px + env(safe-area-inset-bottom));z-index:1500;width:46px;height:46px;align-items:center;justify-content:center;border:0;border-radius:999px;background:var(--accent);color:var(--accent-text);box-shadow:0 4px 14px rgba(0,0,0,.22);cursor:pointer}
        .voltar-topo:hover{background:var(--accent-hover)}

    /* Estilo do modal */
    .modal{display:none;position:fixed;z-index:2000;left:0;top:0;width:100%;height:100%;background:rgba(27,27,26,.6)}
    .modal-content{position:relative;background:var(--modal-bg);margin:12% auto;padding:28px 24px;border-radius:16px;width:min(420px, calc(100% - 32px));text-align:center;box-shadow:0 12px 40px rgba(0,0,0,.25);color:var(--modal-text)}
    .modal-content h2{font-size:18px;margin:0 0 6px}
    .modal-content .modal-body{display:flex;flex-direction:column;align-items:center;gap:12px;padding:6px}
    .modal-spinner{width:64px;height:64px;border:7px solid var(--spinner-border);border-top-color:var(--spinner-top);border-radius:50%;animation:spin .8s linear infinite}
    .modal-icon{width:60px;height:60px;border-radius:999px;display:flex;align-items:center;justify-content:center;font-size:28px}
    .modal-icon.success{background:var(--success-bg);color:#fff}
    .modal-icon.error{background:var(--error-bg);color:#fff}
    .modal-content .msg{font-size:15px;color:var(--muted);max-width:360px}
    .modal-actions{display:flex;gap:8px;margin-top:6px}
    .modal .close{position:absolute;top:10px;right:15px;font-size:22px;font-weight:bold;color:var(--muted);opacity:.5;cursor:not-allowed;pointer-events:none}
    .modal .close.enabled{opacity:1;color:var(--text-color);cursor:pointer;pointer-events:auto}

    /* Status de download: verde = baixado por mim (com ×), amarelo = só outras pessoas baixaram, sem cor = não baixado */
    .baixado-badge{display:inline-flex;align-items:center;gap:4px;background:var(--download-badge-bg);color:var(--download-badge-text);padding:4px 6px 4px 12px;border-radius:999px;font-size:13px;font-weight:600;white-space:nowrap;cursor:default}
    .baixado-badge.baixado-outro{background:var(--other-badge-bg);color:var(--other-badge-text);padding-right:12px}
    .nao-baixado{font-size:13px;color:var(--muted);white-space:nowrap}
    /* Coluna "Baixado por": um nome ao lado do outro (e-mail ao passar o mouse); o meu em negrito */
    .baixado-por .pessoas{display:inline-flex;flex-wrap:wrap;gap:6px}
    .pessoa-chip{display:inline-block;padding:3px 10px;border-radius:999px;border:1px solid var(--border);background:var(--badge-bg);color:var(--text-color);font-size:13px;white-space:nowrap;cursor:default}
    .pessoa-chip.eu{font-weight:700}
    /* × dentro da tag: tira a marcação de baixado só daquele documento */
    .remover-baixado{display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;padding:0;border:0;border-radius:50%;background:transparent;color:inherit;font-size:16px;line-height:1;cursor:pointer;opacity:.7}
    .remover-baixado:hover,.remover-baixado:focus-visible{opacity:1;background:rgba(0,0,0,.08)}
    .dark-mode .remover-baixado:hover,.dark-mode .remover-baixado:focus-visible{background:rgba(255,255,255,.15)}
    .remover-baixado:disabled{cursor:wait;opacity:.4}

    /* micro-flash quando um contador muda */
    .count-val.flash{animation:counterFlash .45s ease both}
    @keyframes counterFlash{
        0% { transform: scale(1); background: transparent }
        30% { transform: scale(1.12); background: var(--counter-flash-bg); color: var(--counter-flash-text) }
        70% { transform: scale(1.03); background: var(--counter-flash-bg); color: var(--counter-flash-text) }
        100% { transform: scale(1); background: transparent }
    }
    </style>
</head>
<body>
    <header class="topbar">
        <div class="topbar-inner">
            <div class="marca"><img class="logo-app" src="/static/logo.svg" alt="" width="34" height="34"><span class="wordmark" aria-label="Sitawi">stwi</span><span class="app-nome">D4Sign Downloader</span></div>
            <div class="topbar-acoes">
                {% if usuario %}
                <div class="usuario-logado" title="{{ usuario.email }}">{{ usuario.nome }} · <a href="/logout">Sair</a></div>
                {% endif %}
                <button id="dark-mode-toggle" title="Alternar modo noturno" aria-label="Alternar modo noturno">
                    <span class="toggle-icon toggle-sun" aria-hidden="true"></span>
                    <span class="toggle-icon toggle-moon" aria-hidden="true"></span>
                </button>
            </div>
        </div>
    </header>
    <div class="page">
        <div class="page-head">
            <h1>Documentos assinados</h1>
            <p class="sub">Documentos finalizados no D4Sign, prontos para baixar</p>
        </div>

        <section class="card filtros" aria-label="Filtros">
            <form method="POST" id="filtro-form" class="controls">
                <div class="group">
                    <label for="filtro-cofre">Lote por fundo</label>
                    <select id="filtro-cofre" name="cofre" onchange="this.form.submit()">
                        <option value="">Todos os cofres</option>
                        {% for cofre in cofres %}
                            <option value="{{ cofre.get('uuid') or cofre.get('uuid_safe') or cofre.get('uuid-safe') }}"
                                {% if cofre_selecionado == (cofre.get('uuid') or cofre.get('uuid_safe') or cofre.get('uuid-safe')) %}selected{% endif %}>
                                {{ cofre.get('name') or cofre.get('name_safe') or cofre.get('name-safe') }}
                            </option>
                        {% endfor %}
                    </select>
                </div>
                <div class="group">
                    <label for="busca-nome">Buscar por nome</label>
                    <input type="text" id="busca-nome" name="busca_nome" value="{{ busca_nome }}" placeholder="Pesquisar documento..." autocomplete="off">
                </div>

                <div class="group">
                    <label for="data-periodo">Período (assinatura)</label>
                    <div class="campo-periodo">
                        <input type="text" id="data-periodo" name="data_periodo" placeholder="AAAA-MM-DD - AAAA-MM-DD" value="{{ (data_inicio and data_fim) and (data_inicio ~ ' - ' ~ data_fim) or (data_periodo or '') }}" readonly>
                        <button type="button" class="btn-icone" id="btn-data-periodo" title="Escolher período" aria-label="Escolher período">📅</button>
                    </div>
                    <!-- hidden fields to submit legacy names -->
                    <input type="hidden" name="data_inicio" id="data_inicio_hidden" value="{{ data_inicio or '' }}">
                    <input type="hidden" name="data_fim" id="data_fim_hidden" value="{{ data_fim or '' }}">
                    <div id="data-periodo-popup" class="range-popup" style="display:none">
                        <div class="datas"><input type="date" id="data-periodo-start"> <span style="font-size:13px;color:var(--muted)">até</span> <input type="date" id="data-periodo-end"></div>
                        <div id="data-periodo-error" class="range-error" style="display:none"></div>
                        <div class="acoes"><button type="button" id="data-periodo-cancel" class="btn-secundario">Cancelar</button><button type="button" class="download-btn" id="data-periodo-apply">Aplicar</button></div>
                    </div>
                </div>

                <!-- filtros de download: no topo para funcionarem também no celular -->
                <div class="group">
                    <label for="status-filter">Status</label>
                    <select id="status-filter" onchange="setViewStatusAndSubmit(this.value)">
                        <option value="nao_baixado_mim" {% if view_status == 'nao_baixado_mim' %}selected{% endif %}>Não baixado por mim</option>
                        <option value="todos" {% if view_status == 'todos' %}selected{% endif %}>Todos</option>
                        <option value="baixado_mim" {% if view_status == 'baixado_mim' %}selected{% endif %}>Baixado por mim</option>
                        <option value="baixado_outro" {% if view_status == 'baixado_outro' %}selected{% endif %}>Baixado por outro</option>
                        <option value="nao_baixado" {% if view_status == 'nao_baixado' %}selected{% endif %}>Não baixado</option>
                    </select>
                </div>
                <div class="group">
                    <label for="pessoa-filter">Baixado por</label>
                    <select id="pessoa-filter" onchange="setBaixadoPorAndSubmit(this.value)">
                        <option value="" {% if not baixado_por %}selected{% endif %}>Todas as pessoas</option>
                        {% for p in pessoas_filtro %}
                        <option value="{{ p['usuario_id'] }}" {% if baixado_por == p['usuario_id'] %}selected{% endif %}>{{ p['nome'] }}</option>
                        {% endfor %}
                    </select>
                </div>

                <!-- ordering is now controlled by clicking the table header -->
                <input type="hidden" name="ordenar_por" id="ordenar_por" value="{{ ordenar_por or '' }}">
                <!-- filtro de status de download: nao_baixado (padrão), nao_baixado_mim, todos, baixado_mim ou baixado_outro -->
                <input type="hidden" name="view_status" id="view_status_hidden" value="{{ view_status }}">
                <!-- filtro por pessoa que baixou (id da conta); vazio = todas -->
                <input type="hidden" name="baixado_por" id="baixado_por_hidden" value="{{ baixado_por }}">
            </form>
        </section>

        <section class="resumo" aria-label="Resumo da lista">
            <div class="card stat"><span class="stat-label">Documentos na lista</span><span id="resumo-lista" class="stat-val count-val">{{ documentos|length }}</span></div>
            <div class="card stat mim" title="Documentos da lista atual (cofre, nome e período filtrados) que eu baixei"><span class="stat-label">Baixados por mim</span><span id="downloaded-count" class="stat-val count-val">{{ total_mim or 0 }}</span></div>
            <div class="card stat outros" title="Documentos da lista atual baixados só por outras pessoas"><span class="stat-label">Baixados por outros</span><span id="downloaded-others-count" class="stat-val count-val">{{ total_outros or 0 }}</span></div>
        </section>

        <section class="card tabela" aria-label="Documentos">
            <form method="POST" id="download-form">
                <div class="table-container {% if documentos|length >= 10 %}scroll-enabled{% endif %}">
                    <table>
                    <thead>
                        <tr>
                            <th style="width:76px"><div class="master-wrap"><input type="checkbox" id="master-check" title="Selecionar todos" aria-label="Selecionar todos"><span id="selected-count">0</span></div></th>
                            <th>Documento</th>
                            <th style="width:130px" class="sortable {% if ordenar_por in ['data_desc','data_asc'] %}active-sort{% endif %}" id="th-data">Data <span id="sort-arrow-data">{% if ordenar_por=='data_desc' %}<span class="sort-icon">{{ ICON_UP|safe }}</span>{% elif ordenar_por=='data_asc' %}<span class="sort-icon">{{ ICON_DOWN|safe }}</span>{% else %}<span class="sort-icon">{{ ICON_UP|safe }}</span>{% endif %}</span></th>
                            <th style="width:130px">Cofre</th>
                            <th style="width:380px">Baixado por</th>
                            <th style="width:205px">Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for doc in documentos %}
                        <tr>
                            <td data-label="Selecionar">
                                <input type="checkbox" name="documentos" value="{{ doc['uuidDoc'] }}" aria-label="Selecionar {{ doc['nomeLimpo'] }}">
                                <input type="hidden" name="doc_nomes[{{ doc['uuidDoc'] }}]" value="{{ doc['nomeOriginal'] }}">
                            </td>
                            <td class="doc-name">{{ doc['nomeLimpo'] }}</td>
                            <td class="date-col" data-label="Data">{{ doc['dataAssinatura'] }}</td>
                            <td data-label="Cofre">{{ doc['cofre_nome'] }}</td>
                            <td data-label="Baixado por" class="baixado-por"><span class="pessoas">{% for p in doc.get('pessoas', []) %}<span class="pessoa-chip{% if p['eu'] %} eu{% endif %}" title="{{ p['email'] or 'e-mail não cadastrado' }}">{{ p['nome'] }}</span>{% endfor %}</span></td>
                            <td data-label="Status">
                                {# o servidor só lista documentos Finalizados; aqui aparece o status de download #}
                                {% if doc.get('status_download') == 'mim' %}
                                    <span class="baixado-badge" title="{{ doc['quem_baixou'] }}" aria-label="{{ doc['quem_baixou'] }}">{{ 'Baixado por mim e outros' if doc.get('outros_baixaram') else 'Baixado por mim' }}<button type="button" class="remover-baixado" data-uuid="{{ doc['uuidDoc'] }}" title="Remover a marcação de baixado por mim" aria-label="Remover a marcação de baixado por mim deste documento">&times;</button></span>
                                {% elif doc.get('status_download') == 'outro' %}
                                    <span class="baixado-badge baixado-outro" title="{{ doc['quem_baixou'] }}" aria-label="{{ doc['quem_baixou'] }}">Baixado por outro</span>
                                {% else %}
                                    <span class="nao-baixado">Não baixado</span>
                                {% endif %}
                            </td>
                        </tr>
                        {% else %}
                        <tr class="linha-vazia"><td colspan="6" class="vazio">Nenhum documento encontrado com esses filtros.</td></tr>
                        {% endfor %}
                    </tbody>
                    </table>
                </div>

                <div class="download-area">
                    <div class="dica">Selecione documentos para baixar</div>
                    <button name="download" class="download-btn">Baixar selecionados</button>
                </div>
            </form>
        </section>
    <!-- icon templates (hidden) - wrapped so injected HTML includes .sort-icon -->
    <template id="icon-up-tpl"><span class="sort-icon">{{ ICON_UP|safe }}</span></template>
    <template id="icon-down-tpl"><span class="sort-icon">{{ ICON_DOWN|safe }}</span></template>
    <!-- theme icons -->
    <template id="icon-sun-tpl"><span class="sort-icon">{{ ICON_SUN|safe }}</span></template>
    <template id="icon-moon-tpl"><span class="sort-icon">{{ ICON_MOON|safe }}</span></template>
    </div>
    <footer class="creditos">Criado por <a href="https://github.com/offryan" target="_blank" rel="noopener">Ryan G. Santos</a></footer>

        <div id="downloadModal" class="modal">
        <div class="modal-content">
            <span id="closeModal" class="close">&times;</span>
            <div class="modal-body">
                <div id="modalSpinner" class="modal-spinner" aria-hidden="true"></div>
                <div id="modalIcon" class="modal-icon" style="display:none" aria-hidden="true"></div>
                <h2 id="modalTitle">Aguarde enquanto o download inicia...</h2>
                <div id="modalMsg" class="msg"></div>
                <div class="modal-actions"></div>
            </div>
        </div>
    </div>




    <script>
        // Se a sessão expirar, qualquer chamada ao servidor recebe 401: leva a pessoa de volta ao login
        (function(){
            var fetchOriginal = window.fetch;
            window.fetch = function(){
                return fetchOriginal.apply(this, arguments).then(function(resp){
                    if(resp.status === 401){ window.location.href = '/login?next=' + encodeURIComponent(window.location.pathname); }
                    return resp;
                });
            };
        })();
    </script>
    <script>
                (function(){
            var master = document.getElementById('master-check');
            // Limit used when selecting all from the visible list
            var MASTER_SELECT_LIMIT = 50;

            // Select only the first N visible documents when master checkbox is used.
            function setAll(checked){
                var inputs = document.querySelectorAll('input[name="documentos"]');
                if(checked){
                    var sel = 0;
                    for(var i=0;i<inputs.length;i++){
                        if(sel < MASTER_SELECT_LIMIT){ inputs[i].checked = true; sel++; }
                        else { inputs[i].checked = false; }
                    }
                } else {
                    for(var i=0;i<inputs.length;i++) inputs[i].checked = false;
                }
            }

            function updateCounter(){
                var inputs = document.querySelectorAll('input[name="documentos"]');
                var checked = Array.prototype.slice.call(inputs).filter(function(i){return i.checked;}).length;
                var countEl = document.getElementById('selected-count');
                if(countEl) countEl.textContent = checked;
                // Also update master checkbox visual state based on the first N visible inputs
                updateMasterState();
                return {checked: checked, total: inputs.length};
            }

            function updateMasterState(){
                if(!master) return;
                var inputs = document.querySelectorAll('input[name="documentos"]');
                var all = true, any = false;
                var considered = 0;
                for(var i=0;i<inputs.length && considered < MASTER_SELECT_LIMIT;i++){
                    considered++;
                    if(inputs[i].checked) any = true; else all = false;
                }
                if(considered === 0){ master.checked = false; master.indeterminate = false; return; }
                master.checked = all;
                master.indeterminate = (!all && any);
            }

            if(master){
                master.addEventListener('change', function(){ setAll(master.checked); updateCounter(); });
            }

            document.addEventListener('change', function(e){
                if(!e.target || e.target.name !== 'documentos') return;
                // Recompute master state only considering the first N visible inputs
                var inputs = document.querySelectorAll('input[name="documentos"]');
                var all = true, any = false;
                var considered = 0;
                for(var i=0;i<inputs.length && considered < MASTER_SELECT_LIMIT;i++){
                    considered++;
                    if(inputs[i].checked) any = true; else all = false;
                }
                if(master){ master.checked = all; master.indeterminate = (!all && any); }
                updateCounter();
            });
            if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', updateCounter); else updateCounter();
            // ordering toggle by clicking the table headers
            var thData = document.getElementById('th-data');
            var ordenarInput = document.getElementById('ordenar_por');
            var filtroForm = document.getElementById('filtro-form');
            var sortArrowData = document.getElementById('sort-arrow-data');
            // Elements for the removed 'ultima' column may not exist; define safely to avoid reference errors
            var thUltima = document.getElementById('th-ultima');
            var sortArrow = document.getElementById('sort-arrow-ultima');
            // read icon templates from <template> so the wrapper .sort-icon is included
            var ICON_UP = document.getElementById('icon-up-tpl').innerHTML;
            var ICON_DOWN = document.getElementById('icon-down-tpl').innerHTML;
            function clearArrowsExcept(except){
                // safely clear other arrows; some elements may be null
                if(except !== 'ultima' && sortArrow) sortArrow.innerHTML = ICON_DOWN;
                if(except !== 'data' && sortArrowData) sortArrowData.innerHTML = ICON_DOWN;
            }
            // AJAX sort helper: posts the filter form and replaces the table body and summary
            function doAjaxSort(nextValue, active){
                if(!filtroForm) return;
                ordenarInput.value = nextValue;
                // optimistic UI: show spinner on active header
                clearArrowsExcept(active);
                var spinnerHtml = '<span class="sort-icon"><span class="spinner" aria-hidden="true"></span></span>';
                if(active === 'ultima' && sortArrow) sortArrow.innerHTML = spinnerHtml;
                if(active === 'data' && sortArrowData) sortArrowData.innerHTML = spinnerHtml;
                // send form as urlencoded
                var body = new URLSearchParams(new FormData(filtroForm));
                fetch(window.location.pathname, {method:'POST', body: body, headers: {'Accept':'text/html'}})
                    .then(function(r){ return r.text(); })
                    .then(function(html){
                        try{
                            var parser = new DOMParser();
                            var doc = parser.parseFromString(html, 'text/html');
                            var newTbody = doc.querySelector('table tbody');
                            var oldTbody = document.querySelector('table tbody');
                            if(newTbody && oldTbody) oldTbody.parentNode.replaceChild(newTbody, oldTbody);
                            // update the summary 'Mostrando X documentos'
                            // update only the inner summary div (preserve the dark-mode toggle button)
                            var newSummary = doc.getElementById('resumo-lista');
                            var oldSummary = document.getElementById('resumo-lista');
                            if(newSummary && oldSummary) oldSummary.textContent = newSummary.textContent;
                            // update active header class and restore icons
                            if(active === 'ultima'){
                                if(thUltima) thUltima.classList.add('active-sort'); if(thData) thData.classList.remove('active-sort');
                                if(sortArrow) sortArrow.innerHTML = (nextValue === 'ultima_desc')? ICON_DOWN : ICON_UP;
                                if(sortArrowData) sortArrowData.innerHTML = ICON_DOWN;
                            } else {
                                if(thData) thData.classList.add('active-sort'); if(thUltima) thUltima.classList.remove('active-sort');
                                // Map: data_desc => newest first => UP icon; data_asc => oldest first => DOWN icon
                                if(sortArrowData) sortArrowData.innerHTML = (nextValue === 'data_desc')? ICON_UP : ICON_DOWN;
                                if(sortArrow) sortArrow.innerHTML = ICON_DOWN;
                            }
                        }catch(e){ console.error('parse error', e); window.location.reload(); }
                    }).catch(function(){ window.location.reload(); });
            }

            // Debounce helper
            function debounce(fn, wait){
                var t = null;
                return function(){
                    var args = arguments;
                    clearTimeout(t);
                    t = setTimeout(function(){ fn.apply(null, args); }, wait);
                };
            }

            // Live search: submit filtro-form on each keystroke (debounced) and replace tbody/summary
            function liveSearchSubmit(){
                if(!filtroForm) return;
                // ensure ordering input is preserved
                var body = new URLSearchParams(new FormData(filtroForm));
                fetch(window.location.pathname, {method:'POST', body: body, headers: {'Accept':'text/html'}})
                    .then(function(r){ return r.text(); })
                    .then(function(html){
                        try{
                            var parser = new DOMParser();
                            var doc = parser.parseFromString(html, 'text/html');
                            var newTbody = doc.querySelector('table tbody');
                            var oldTbody = document.querySelector('table tbody');
                            if(newTbody && oldTbody) oldTbody.parentNode.replaceChild(newTbody, oldTbody);
                            // update only the inner summary div (preserve the dark-mode toggle button)
                            var newSummary = doc.getElementById('resumo-lista');
                            var oldSummary = document.getElementById('resumo-lista');
                            if(newSummary && oldSummary) oldSummary.textContent = newSummary.textContent;
                            // opções do filtro "Baixado por" acompanham a lista (período, cofre, nome)
                            var novoFiltro = doc.getElementById('pessoa-filter'), filtroAtual = document.getElementById('pessoa-filter');
                            if(novoFiltro && filtroAtual){ filtroAtual.innerHTML = novoFiltro.innerHTML; filtroAtual.value = novoFiltro.value; }
                            // contadores "Baixados por mim" e "Por outros" seguem os filtros da lista
                            ['downloaded-count', 'downloaded-others-count'].forEach(function(id){
                                var novo = doc.getElementById(id), atual = document.getElementById(id);
                                if(novo && atual && novo.textContent !== atual.textContent){ atual.textContent = novo.textContent; flashCounter(atual); }
                            });
                            // refresh selection counter and master checkbox state
                            updateCounter();
                        }catch(e){ console.error('live-parse error', e); }
                    }).catch(function(e){ console.error('live-search error', e); });
            }

            // usado depois de um download ou de um × para o status das linhas mudar sem recarregar a página
            window.atualizarLista = liveSearchSubmit;

            // micro-flash no contador que mudou
            function flashCounter(cv){
                try{
                    cv.classList.remove('flash');
                    void cv.offsetWidth; // força reflow para reiniciar a animação
                    cv.classList.add('flash');
                    setTimeout(function(){ cv.classList.remove('flash'); }, 700);
                }catch(e){}
            }

            var buscarInput = document.getElementById('busca-nome');
            if(buscarInput){
                var debounced = debounce(function(){
                    // when empty, the server will return all documents (as existing index logic does)
                    liveSearchSubmit();
                }, 300);
                buscarInput.addEventListener('input', debounced);
            }

            if(thData && ordenarInput){
                thData.addEventListener('click', function(){
                    var cur = ordenarInput.value || '';
                    var next = '';
                    if(cur === '' || cur === 'data_asc') next = 'data_desc'; else next = 'data_asc';
                    doAjaxSort(next, 'data');
                });
            }
            // Utility to format ISO date to DD/MM/YYYY for display
            function formatIsoToDisplay(iso){
                if(!iso) return '';
                try{ var p = iso.split('-'); if(p.length===3) return p[2] + '/' + p[1] + '/' + p[0]; }catch(e){}
                return iso;
            }

            // Ensure default ordering is newest-first (data_desc) on initial load when not set by server
            document.addEventListener('DOMContentLoaded', function(){
                try{
                    if(ordenarInput && !ordenarInput.value){
                        ordenarInput.value = 'data_desc';
                    }
                    // reflect icon and active class to match default
                    if(sortArrowData){ sortArrowData.innerHTML = ICON_UP; }
                    if(thData) thData.classList.add('active-sort');

                    // Populate the visible period input from hidden ISO fields so the calendar shows the start date
                    try{
                        var hiddenStart = document.getElementById('data_inicio_hidden');
                        var hiddenEnd = document.getElementById('data_fim_hidden');
                        var visible = document.getElementById('data-periodo');
                        if(visible && hiddenStart && hiddenStart.value){
                            if(hiddenEnd && hiddenEnd.value){ visible.value = formatIsoToDisplay(hiddenStart.value) + ' - ' + formatIsoToDisplay(hiddenEnd.value); }
                            else { visible.value = formatIsoToDisplay(hiddenStart.value); }
                        }
                    }catch(e){}

                }catch(e){/* ignore */}
            });
            // batch refresh logic
            var refreshBtn = document.getElementById('refresh-batch-btn');
            function fadeUpdate(el, text){
                if(!el) return;
                el.style.transition = 'opacity 0.35s';
                el.style.opacity = '0.3';
                setTimeout(function(){ el.textContent = text; el.style.opacity = '1'; }, 360);
            }
            // refresh-batch button removed

            // register-dates button removed
            // date-range popup helpers
            function setupRange(buttonId, inputId, popupId, startId, endId, applyId, cancelId, hiddenStartId, hiddenEndId){
                var btn = document.getElementById(buttonId);
                var input = document.getElementById(inputId);
                var popup = document.getElementById(popupId);
                var start = document.getElementById(startId);
                var end = document.getElementById(endId);
                var apply = document.getElementById(applyId);
                var cancel = document.getElementById(cancelId);
                var hiddenStart = document.getElementById(hiddenStartId);
                var hiddenEnd = document.getElementById(hiddenEndId);
                if(!btn || !input || !popup) return;
                // helper to format YYYY-MM-DD to DD/MM/YYYY
                function fmt(iso){
                    if(!iso) return '';
                    try{ var parts = iso.split('-'); if(parts.length===3) return parts[2]+'/'+parts[1]+'/'+parts[0]; }catch(e){}
                    return iso;
                }
                btn.addEventListener('click', function(e){
                    popup.style.display = 'block';
                    // prefill if hidden values exist
                    if(hiddenStart && hiddenStart.value) start.value = hiddenStart.value;
                    if(hiddenEnd && hiddenEnd.value) end.value = hiddenEnd.value;
                    // set visible text in formatted form
                    if(hiddenStart && hiddenStart.value){
                        if(hiddenEnd && hiddenEnd.value){ input.value = fmt(hiddenStart.value) + ' - ' + fmt(hiddenEnd.value); }
                        else { input.value = fmt(hiddenStart.value); }
                    }
                    var errorEl = popup.querySelector('.range-error'); if(errorEl) errorEl.style.display = 'none';
                });
                var errorEl = popup.querySelector('.range-error');
                apply.addEventListener('click', function(){
                    // validation: start must be <= end when both present
                    if(start.value && end.value){
                        if(start.value > end.value){
                            if(errorEl){ errorEl.textContent = 'Data inicial não pode ser posterior à data final.'; errorEl.style.display = 'block'; }
                            return;
                        }
                        if(errorEl){ errorEl.style.display = 'none'; }
                        input.value = fmt(start.value) + ' - ' + fmt(end.value);
                        if(hiddenStart) hiddenStart.value = start.value;
                        if(hiddenEnd) hiddenEnd.value = end.value;
                    } else {
                        // when only start present, show formatted start; if none, clear
                        if(start.value){ input.value = fmt(start.value); if(hiddenStart) hiddenStart.value = start.value; }
                        else { input.value = ''; if(hiddenStart) hiddenStart.value = ''; }
                        if(hiddenEnd) hiddenEnd.value = '';
                        if(errorEl){ errorEl.style.display = 'none'; }
                    }
                    popup.style.display = 'none';
                    // submit the filter form so server-side filtering (or AJAX live update) is applied
                    try{
                        if(typeof liveSearchSubmit === 'function'){
                            liveSearchSubmit();
                        } else if(filtroForm){
                            filtroForm.submit();
                        } else {
                            var f = document.getElementById('filtro-form'); if(f) f.submit();
                        }
                    }catch(e){ /* ignore submission errors */ }
                });
                cancel.addEventListener('click', function(){ if(errorEl) errorEl.style.display = 'none'; popup.style.display = 'none'; });
                // click outside popup closes it
                document.addEventListener('click', function(ev){ if(popup.style.display==='block' && !popup.contains(ev.target) && ev.target !== btn && ev.target !== input) { if(errorEl) errorEl.style.display = 'none'; popup.style.display='none'; } });
            }
            setupRange('btn-data-periodo','data-periodo','data-periodo-popup','data-periodo-start','data-periodo-end','data-periodo-apply','data-periodo-cancel','data_inicio_hidden','data_fim_hidden');
            setupRange('btn-ultima-periodo','ultima-periodo','ultima-periodo-popup','ultima-periodo-start','ultima-periodo-end','ultima-periodo-apply','ultima-periodo-cancel','ultima_inicio_hidden','ultima_fim_hidden');
            // × da etiqueta "Baixado por mim": tira só a marcação da pessoa logada naquele documento.
            // Delegado no document porque a tabela é substituída a cada busca/ordenação.
            document.addEventListener('click', function(ev){
                var btn = ev.target.closest && ev.target.closest('.remover-baixado');
                if(!btn) return;
                ev.preventDefault();
                btn.disabled = true;
                fetch('/baixados/remover', { method: 'POST', body: new URLSearchParams({ uuid: btn.dataset.uuid }) })
                    .then(function(r){ return r.json().then(function(j){ if(!r.ok || !j.ok) throw new Error(j.error || r.status); return j; }); })
                    .then(function(){
                        // o servidor recalcula a linha (amarela se outra pessoa baixou, "Não baixado" se ninguém),
                        // tira da lista se ela não bate mais com o filtro e atualiza os contadores
                        if(window.atualizarLista) window.atualizarLista();
                    })
                    .catch(function(){
                        btn.disabled = false;
                        alert('Não foi possível remover a marcação. Tente de novo.');
                    });
            });

            // Dark mode toggle: persist in localStorage
            (function(){
                var toggle = document.getElementById('dark-mode-toggle');
                // prefer explicit sun/moon templates; fallback to emoji
                var TOGGLE_ICON_SUN = (document.getElementById('icon-sun-tpl') && document.getElementById('icon-sun-tpl').innerHTML) || '🌞';
                var TOGGLE_ICON_MOON = (document.getElementById('icon-moon-tpl') && document.getElementById('icon-moon-tpl').innerHTML) || '🌙';
                function applyMode(mode){
                    if(mode === 'dark') document.body.classList.add('dark-mode'); else document.body.classList.remove('dark-mode');
                    // update button color to contrast and swap visible icon
                    if(toggle){
                        var sunSpan = toggle.querySelector('.toggle-sun');
                        var moonSpan = toggle.querySelector('.toggle-moon');
                        if(sunSpan) sunSpan.innerHTML = TOGGLE_ICON_SUN;
                        if(moonSpan) moonSpan.innerHTML = TOGGLE_ICON_MOON;
                        if(mode === 'dark'){
                            toggle.style.background = 'var(--btn-bg)'; toggle.style.color = 'var(--btn-text)';
                            if(sunSpan) sunSpan.classList.add('visible'); if(moonSpan) moonSpan.classList.remove('visible');
                        } else {
                            toggle.style.background = 'transparent'; toggle.style.color = 'var(--muted)';
                            if(sunSpan) sunSpan.classList.remove('visible'); if(moonSpan) moonSpan.classList.add('visible');
                        }
                    }
                    // refresh-downloads-file button removed
                }
                // initialize from localStorage or system preference
                try{
                    var saved = localStorage.getItem('d4sign:dark_mode');
                    if(saved === 'dark'){
                        applyMode('dark');
                    } else {
                        applyMode('light');
                    }
                }catch(e){ /* ignore */ }
                if(toggle){
                    // ensure icons are populated on init
                    var initSun = document.createElement('span'); initSun.innerHTML = TOGGLE_ICON_SUN;
                    var initMoon = document.createElement('span'); initMoon.innerHTML = TOGGLE_ICON_MOON;
                    var s = toggle.querySelector('.toggle-sun'); var mEl = toggle.querySelector('.toggle-moon');
                    if(s) s.innerHTML = TOGGLE_ICON_SUN;
                    if(mEl) mEl.innerHTML = TOGGLE_ICON_MOON;
                    toggle.addEventListener('click', function(){
                        var isDark = document.body.classList.contains('dark-mode');
                        var next = isDark ? 'light' : 'dark';
                        applyMode(next);
                        try{ localStorage.setItem('d4sign:dark_mode', next); }catch(e){}
                    });
                }
            })();

            // ensure status-filter selection updates the hidden view_status before submitting
            var statusFilter = document.getElementById('status-filter');
            var viewStatusHidden = document.getElementById('view_status_hidden');
            // helper exposed for inline onchange to guarantee hidden value is set before submit
            window.setViewStatusAndSubmit = function(v){ try{ var vh = document.getElementById('view_status_hidden'); if(vh) vh.value = v; var f = document.getElementById('filtro-form'); if(f) f.submit(); }catch(e){} };
            // filtro "Baixado por": ao escolher uma pessoa, o Status vai para "Todos"
            // (com "Não baixado" a lista ficaria sempre vazia); depois dá para combinar os dois
            window.setBaixadoPorAndSubmit = function(v){
                try{
                    var bh = document.getElementById('baixado_por_hidden'); if(bh) bh.value = v;
                    var vh = document.getElementById('view_status_hidden'); if(v && vh && vh.value === 'nao_baixado') vh.value = 'todos';
                    var f = document.getElementById('filtro-form'); if(f) f.submit();
                }catch(e){}
            };
            if(statusFilter && viewStatusHidden){
                // initialize select based on hidden input
                try{ if(viewStatusHidden.value) statusFilter.value = (viewStatusHidden.value || 'nao_baixado'); }catch(e){}
                statusFilter.addEventListener('change', function(){ viewStatusHidden.value = statusFilter.value; document.getElementById('filtro-form').submit(); });
            }

            // Intercept download form submit to show modal while downloading
            (function(){
                var downloadForm = document.getElementById('download-form');
                var downloadBtn = document.querySelector('button[name="download"]');
                var downloadModal = document.getElementById('downloadModal');
                var modalTitle = downloadModal && downloadModal.querySelector('.modal-content h2');
                var closeModalBtn = document.getElementById('closeModal');
                var downloadClicked = false;
                // no cancel button: simple flow

                if(downloadBtn){
                    downloadBtn.addEventListener('click', function(){ downloadClicked = true; });
                }

                var modalSpinner = document.getElementById('modalSpinner');
                var modalIcon = document.getElementById('modalIcon');
                var modalTitleEl = document.getElementById('modalTitle');
                var modalMsgEl = document.getElementById('modalMsg');

                function showModal(msg){
                    if(!downloadModal) return;
                    if(modalTitleEl) modalTitleEl.textContent = msg || 'Aguarde...';
                    // keep a single informative line under the title; start with a neutral preparing text
                    if(modalMsgEl) modalMsgEl.textContent = 'Preparando download...';
                    // show spinner, hide icon
                    if(modalSpinner) modalSpinner.style.display = 'block';
                    if(modalIcon) { modalIcon.style.display = 'none'; modalIcon.className = 'modal-icon'; }
                    // disable close until finished
                    if(closeModalBtn){ closeModalBtn.classList.remove('enabled'); closeModalBtn.style.pointerEvents = 'none'; }
                    downloadModal.style.display = 'block';
                }
                function hideModal(){ if(downloadModal) downloadModal.style.display = 'none'; }

                function showSuccess(msg){
                    if(modalSpinner) modalSpinner.style.display = 'none';
                    if(modalIcon){ modalIcon.style.display = 'flex'; modalIcon.className = 'modal-icon success'; modalIcon.textContent = '✓'; }
                    if(modalTitleEl) modalTitleEl.textContent = msg || 'Concluído';
                    // preserve the modalMsgEl content (we want to keep the total MB visible)
                    if(closeModalBtn){ closeModalBtn.classList.add('enabled'); closeModalBtn.style.pointerEvents = 'auto'; }
                }

                function showError(msg){
                    if(modalSpinner) modalSpinner.style.display = 'none';
                    if(modalIcon){ modalIcon.style.display = 'flex'; modalIcon.className = 'modal-icon error'; modalIcon.textContent = '!'; }
                    if(modalTitleEl) modalTitleEl.textContent = msg || 'Erro';
                    if(modalMsgEl) modalMsgEl.textContent = '';
                    if(closeModalBtn){ closeModalBtn.classList.add('enabled'); closeModalBtn.style.pointerEvents = 'auto'; }
                }

                // cancel removed: no showCanceled

                if(closeModalBtn){
                    closeModalBtn.addEventListener('click', function(){ if(closeModalBtn.classList.contains('enabled')) hideModal(); });
                }

                if(downloadForm){
                    downloadForm.addEventListener('submit', function(e){
                        // Only intercept when the download button was used
                        if(!downloadClicked){ return; }
                        e.preventDefault();
                        downloadClicked = false;
                        // show waiting modal
                        showModal('Aguarde enquanto o download não finaliza...');

                        var fd = new FormData(downloadForm);
                        // ensure server receives the download flag (normal submit would include the clicked button name)
                        fd.append('download', '1');

                        fetch(window.location.pathname, { method: 'POST', body: fd })
                            .then(function(resp){
                                if(!resp.ok) throw new Error('Resposta inválida: ' + resp.status);
                                var ct = (resp.headers.get('Content-Type') || '');
                                // If server returned a zip, treat as file; otherwise assume HTML and replace page
                                if(ct.indexOf('application/zip') !== -1 || ct.indexOf('application/octet-stream') !== -1){
                                    // o zip chega em pedaços; quantos arquivos entraram e quantos falharam vem de /lote/<id> no fim
                                    var loteId = resp.headers.get('X-Lote');
                                    return resp.blob().then(function(blob){
                                        if(!loteId) return { blob: blob, lote: null };
                                        return fetch('/lote/' + encodeURIComponent(loteId))
                                            .then(function(r){ return r.ok ? r.json() : null; })
                                            .catch(function(){ return null; })
                                            .then(function(lote){ return { blob: blob, lote: lote }; });
                                    }).then(function(res){
                                        var blob = res.blob;
                                        var zipCountHeader = res.lote ? String(res.lote.arquivos) : null;
                                        var falhasHeader = res.lote ? (res.lote.falhas || 0) : 0;
                                        // try to extract filename from Content-Disposition
                                        var cd = resp.headers.get('Content-Disposition') || '';
                                        var filename = 'documentos_assinados.zip';
                                        try{
                                            var m = cd.match(/filename\*=UTF-8''([^;]+)|filename="([^\"]+)"|filename=([^;\n]+)/i);
                                            if(m){ filename = decodeURIComponent(m[1] || m[2] || m[3]); }
                                        }catch(e){}
                                        var url = window.URL.createObjectURL(blob);
                                        var a = document.createElement('a');
                                        a.href = url;
                                        a.download = filename;
                                        document.body.appendChild(a);
                                        a.click();
                                        setTimeout(function(){
                                            window.URL.revokeObjectURL(url);
                                            if(a.parentNode) a.parentNode.removeChild(a);
                                        }, 1500);
                                            // show total size in MB in the modal message before marking success
                                            try{
                                                if(modalMsgEl){
                                                    var sizeMB = (blob.size / (1024*1024));
                                                    // show with two decimals, using comma as decimal separator for pt-BR readability
                                                    var sizeStr = sizeMB.toFixed(2).replace('.', ',');
                                                    var filesPart = '';
                                                    try{
                                                        if(zipCountHeader){
                                                            var n = parseInt(zipCountHeader, 10) || 0;
                                                            filesPart = n + (n === 1 ? ' arquivo' : ' arquivos') + ' — ';
                                                        }
                                                    }catch(e){}
                                                    modalMsgEl.textContent = (filesPart ? filesPart : '') + 'Tamanho total: ' + sizeStr + ' MB'
                                                        + (falhasHeader ? ' — ' + falhasHeader + (falhasHeader === 1 ? ' documento não pôde' : ' documentos não puderam') + ' ser baixado' + (falhasHeader === 1 ? '' : 's') + ', tente de novo' : '');
                                                }
                                            }catch(e){}
                                            // update modal to completed and enable close
                                            showSuccess('Download Concluído');
                                        // recarrega a lista: os documentos baixados ficam verdes e os contadores
                                        // "Baixados por mim" / "Por outros" mudam (com o flash) conforme os filtros
                                        try{ if(window.atualizarLista) window.atualizarLista(); }catch(e){}
                                        // auto-hide after short delay (5s)
                                        setTimeout(function(){ hideModal(); }, 5000);
                                    });
                                }
                                // non-zip response: load as text (likely the HTML page with errors or no-selection)
                                return resp.text().then(function(txt){
                                    // replace entire document with returned HTML so server-side validation/errors are visible
                                    document.open(); document.write(txt); document.close();
                                });
                            })
                            .catch(function(err){
                                console.error('Download error', err);
                                showError('Erro no download');
                            });
                    });
                }
            })();
})();
    </script>
    <button type="button" class="voltar-topo" id="voltar-topo" title="Voltar ao topo" aria-label="Voltar ao topo">
        <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 19V5M5.5 11.5 12 5l6.5 6.5"/></svg>
    </button>
    <script>
    // Seta de voltar ao topo: aparece depois de rolar a página ou a lista (o CSS só a mostra no celular).
    // A lista tem rolagem própria e é trocada quando os filtros mudam, por isso ela é buscada a cada vez.
    (function(){
        var seta = document.getElementById('voltar-topo');
        if(!seta) return;
        function listas(){ return document.querySelectorAll('.table-container'); }
        function atualizar(){
            var rolou = window.scrollY > 400;
            listas().forEach(function(l){ if(l.scrollTop > 400) rolou = true; });
            seta.classList.toggle('visivel', rolou);
        }
        // scroll não sobe pelo documento; na fase de captura dá para ouvir a rolagem da página e da lista
        document.addEventListener('scroll', atualizar, {passive: true, capture: true});
        seta.addEventListener('click', function(){
            listas().forEach(function(l){ l.scrollTo({top: 0, behavior: 'smooth'}); });
            window.scrollTo({top: 0, behavior: 'smooth'});
        });
        atualizar();
    })();
    </script>
</body>
</html>"""


# Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Simple TTL cache
CACHE = {}
CACHE_TTL = 60

# If env vars were just loaded, clear any pre-existing cache to avoid stale empty results
try:
    if TOKEN_API:
        CACHE.clear()
except Exception:
    pass

# In-memory signature cache populated by manual refresh or webhooks
SIGNATURE_CACHE = {}

# Quem baixou o quê fica em registro.py (Supabase por pessoa, ou arquivo local em desenvolvimento).
# As datas de assinatura das rotas antigas de manutenção ficam em outro arquivo, para não se
# misturarem com os baixados (antes inflavam o contador).
ASSINATURAS_FILE = os.path.join(os.path.dirname(__file__), 'assinaturas.json')


def _load_assinaturas():
    try:
        if os.path.exists(ASSINATURAS_FILE):
            with open(ASSINATURAS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        logger.exception('Erro lendo assinaturas.json')
    return {}


def _save_assinaturas(data):
    try:
        with open(ASSINATURAS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception('Erro salvando assinaturas.json')


def get_downloaded_meta():
    """{uuid_doc: data do download} da pessoa logada."""
    try:
        return registro.baixados(auth.usuario_atual())
    except Exception:
        logger.exception('Erro lendo o registro de downloads')
        return {}


def get_downloaded_uuids():
    return set(get_downloaded_meta())


# Janela padrão do status de download; se o período filtrado começar antes, ela acompanha o filtro.
JANELA_BAIXADOS_DIAS = 60
FUSO_BRASILIA = timezone(timedelta(hours=-3))


def _chave_nome(nome):
    """Ordenação alfabética sem diferenciar maiúsculas nem acentos."""
    return unicodedata.normalize('NFKD', nome or '').encode('ascii', 'ignore').decode().lower()


def marcar_status_download(documentos, desde=None):
    """Põe em cada documento o status de download visto pela pessoa logada.

    status_download: 'mim' (eu baixei, mesmo que outros também), 'outro' (só outras pessoas) ou 'nao'.
    quem_baixou: texto da dica com quem baixou e quando, em ordem alfabética, um por linha.
    pessoas: quem baixou, em ordem alfabética ({'usuario_id', 'nome', 'email', 'eu'}), para a coluna "Baixado por".
    """
    try:
        por_doc = registro.baixados_por_documento(desde)
    except Exception:
        logger.exception('Erro lendo o registro de downloads')
        por_doc = {}
    usuario = auth.usuario_atual() or registro.USUARIO_COMPARTILHADO
    meu_id = str(usuario.get('id'))
    for d in documentos:
        pessoas = por_doc.get(d.get('uuidDoc')) or []
        eu = any(str(p['usuario_id']) == meu_id for p in pessoas)
        outros = [p for p in pessoas if str(p['usuario_id']) != meu_id]
        d['status_download'] = 'mim' if eu else ('outro' if outros else 'nao')
        d['outros_baixaram'] = bool(eu and outros)
        linhas = []
        d['pessoas'] = []
        for p in sorted(pessoas, key=lambda p: _chave_nome(p['nome'])):
            quando = p['baixado_em'].replace(tzinfo=timezone.utc).astimezone(FUSO_BRASILIA).strftime('%d/%m/%Y')
            eu_mesmo = str(p['usuario_id']) == meu_id
            linhas.append(f"{p['nome']}{' (você)' if eu_mesmo else ''} em {quando}")
            d['pessoas'].append({'usuario_id': str(p['usuario_id']), 'nome': p['nome'], 'email': p.get('email'), 'eu': eu_mesmo})
        d['quem_baixou'] = ('Baixado por:\n' + '\n'.join(linhas)) if linhas else ''
    return documentos


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


# Downloads em paralelo: quantos documentos baixar ao mesmo tempo da D4Sign
DOWNLOAD_WORKERS = max(1, int(os.environ.get('D4SIGN_DOWNLOAD_WORKERS', '6')))

def _criar_sessao():
    sessao = requests.Session()
    retry = Retry(total=3, backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(['GET', 'POST']), respect_retry_after_header=True)
    adapter = HTTPAdapter(pool_connections=DOWNLOAD_WORKERS * 2, pool_maxsize=DOWNLOAD_WORKERS * 2, max_retries=retry)
    sessao.mount('https://', adapter)
    sessao.mount('http://', adapter)
    return sessao

HTTP = _criar_sessao()

def baixar_documento(uuid_doc):
    url = f"{HOST_D4SIGN}/documents/{uuid_doc}/download?tokenAPI={TOKEN_API}&cryptKey={CRYPT_KEY}"
    try:
        r = HTTP.post(url, json={"type": "pdf", "language": "pt"}, timeout=30)
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
            resp = HTTP.get(result["url"], timeout=60)
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
    """Refresh the latest signature dates of the documents downloaded by the current user.
    Returns a mapping uuid -> formatted date or None.
    """
    try:
        uuids = sorted(get_downloaded_uuids())
        if not uuids:
            return jsonify({'ok': False, 'error': 'no downloads found', 'result': {}}), 200
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


@app.route('/baixados/remover', methods=['POST'])
def remover_baixado():
    """Tira a marcação "Baixado por mim" de um documento, só para a pessoa logada (a das outras pessoas fica)."""
    uuid_doc = (request.form.get('uuid') or '').strip()
    if not uuid_doc:
        return jsonify({'ok': False, 'error': 'documento não informado'}), 400
    try:
        registro.remover(auth.usuario_atual(), uuid_doc)
    except Exception:
        logger.exception('Erro removendo a marcação de baixado de %s', uuid_doc)
        return jsonify({'ok': False, 'error': 'não foi possível remover a marcação'}), 500
    return jsonify({'ok': True, 'total': len(get_downloaded_uuids())})


@app.route('/register-dates', methods=['POST'])
def register_dates():
    """Persist available ultimaAssinatura for currently listed documents into assinaturas.json.
    Returns mapping uuid -> formatted date for UI update.
    """
    try:
        # Load existing stored metadata first: prefer persisted signatures when present
        all_meta = _load_assinaturas() or {}
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
                _save_assinaturas(all_meta)
            except Exception:
                logger.exception('Error saving registered dates')

        return jsonify({'ok': True, 'result': results}), 200
    except Exception:
        logger.exception('register-dates error')
        return jsonify({'ok': False, 'error': 'internal error'}), 500


class _SaidaDoZip:
    """Destino onde o zipfile escreve: guarda só o pedaço do zip que ainda não foi enviado ao navegador.

    Sem seek(), o zipfile entende que a saída não volta atrás e grava o zip em sequência.
    """

    def __init__(self):
        self._pendente = bytearray()
        self._posicao = 0

    def write(self, dados):
        self._pendente += dados
        self._posicao += len(dados)
        return len(dados)

    def tell(self):
        return self._posicao

    def flush(self):
        pass

    def retirar(self):
        dados = bytes(self._pendente)
        self._pendente.clear()
        return dados


def _baixar_em_ordem(pool, uuids):
    """Baixa os documentos em paralelo e devolve (uuid, conteúdo) na ordem da seleção.

    Só pede à D4Sign alguns documentos à frente do que já foi enviado, para o servidor nunca
    segurar mais que esses poucos PDFs na memória, seja qual for o tamanho do lote.
    """
    restantes = iter(uuids)
    pendentes = deque((u, pool.submit(baixar_documento, u)) for u in itertools.islice(restantes, DOWNLOAD_WORKERS * 2))
    while pendentes:
        uuid_doc, futuro = pendentes.popleft()
        proximo = next(restantes, None)
        if proximo is not None:
            pendentes.append((proximo, pool.submit(baixar_documento, proximo)))
        yield uuid_doc, futuro.result()


# Resultado de cada lote ({id: {'arquivos', 'falhas'}}), lido uma vez pela página depois do download
LOTES = {}
MAX_LOTES_GUARDADOS = 50


def _gerar_zip_lote(lote_id, selecionados, nomes, usuario):
    """Gera o zip em pedaços: cada PDF vai para o navegador assim que chega da D4Sign."""
    inicio = time.time()
    saida = _SaidaDoZip()
    baixados = []
    falhas = 0
    used = set()
    counts = {}
    pool = ThreadPoolExecutor(max_workers=min(DOWNLOAD_WORKERS, len(selecionados)))
    try:
        with zipfile.ZipFile(saida, "w", zipfile.ZIP_STORED) as zf:
            for uuid_doc, content in _baixar_em_ordem(pool, selecionados):
                if not content:
                    falhas += 1
                    continue
                safe_name = re.sub(r'[<>:"/\\|?*]', "_", nomes[uuid_doc]).strip()
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
                baixados.append(uuid_doc)
                yield saida.retirar()
            # registra antes de fechar o zip: quando o navegador recebe o último pedaço, o resultado já está pronto.
            # Se a pessoa cancelar o download no meio, nada é registrado.
            try:
                registro.registrar(usuario, baixados)
            except Exception:
                logger.exception('Erro ao registrar downloads')
            while len(LOTES) >= MAX_LOTES_GUARDADOS:
                LOTES.pop(next(iter(LOTES)))
            LOTES[lote_id] = {'arquivos': len(baixados), 'falhas': falhas}
        yield saida.retirar()  # índice do zip, escrito ao fechar o arquivo
        logger.info(f'Lote: {len(baixados)} baixados, {falhas} falhas em {time.time() - inicio:.1f}s ({DOWNLOAD_WORKERS} em paralelo)')
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


@app.route('/favicon.ico')
def favicon():
    # alguns navegadores pedem /favicon.ico antes de ler o <link rel="icon"> da página
    return redirect('/static/logo.svg', code=301)


@app.route('/lote/<lote_id>')
def resultado_lote(lote_id):
    resultado = LOTES.pop(lote_id, None)
    if resultado is None:
        return jsonify({'ok': False, 'error': 'lote não encontrado'}), 404
    return jsonify({'ok': True, **resultado})


@app.route("/", methods=["GET", "POST"])
def index():
    t0 = time.time()
    cofres = listar_cofres()
    cofre_map = { (c.get("uuid") or c.get("uuid_safe") or c.get("uuid-safe")):
                  (c.get("name") or c.get("name_safe") or c.get("name-safe", "Sem Nome"))
                  for c in cofres }

    cofre_selecionado = request.form.get("cofre")
    documentos = listar_documentos(cofre_selecionado)

    # filtro de status de download (padrão: Não baixado, com Todas as pessoas). Valores antigos continuam valendo em links salvos.
    view_status = request.form.get('view_status') or request.args.get('view_status') or 'nao_baixado'
    view_status = {'finalizado': 'todos', 'baixado': 'baixado_mim'}.get(view_status, view_status)
    if view_status not in ('nao_baixado_mim', 'todos', 'baixado_mim', 'baixado_outro', 'nao_baixado'):
        view_status = 'nao_baixado'
    # filtro por pessoa que baixou (id da conta); vazio = todas as pessoas
    baixado_por = (request.form.get('baixado_por') or request.args.get('baixado_por') or '').strip()

    for d in documentos:
        d["cofre_nome"] = cofre_map.get(d.get("cofre_uuid"), "Desconhecido")
        # signature-enrichment removed (we no longer show ultimaAssinatura)

    busca_nome = (request.form.get("busca_nome") or "").strip().lower()
    if busca_nome:
        documentos = [d for d in documentos if busca_nome in (d.get("nomeLimpo") or "").lower()]

    # Date filtering logic
    data_periodo = (request.form.get('data_periodo') or '').strip()
    data_inicio = request.form.get("data_inicio")
    data_fim = request.form.get("data_fim")
    # ultima_* filters removed

    # Default date range: if user didn't apply a filter, default to last 60 days.
    date_filter_applied = any([data_periodo, data_inicio, data_fim])
    # Only auto-fill defaults on initial GET load. If the user POSTs a filter
    # we must not override their submitted values.
    if not date_filter_applied and request.method == 'GET':
        try:
            # Use UTC-aware current date for consistent behavior across environments
            today = datetime.utcnow().date()
            inicio_periodo = today - timedelta(days=60)
            data_inicio = inicio_periodo.strftime('%Y-%m-%d')
            data_fim = today.strftime('%Y-%m-%d')
        except Exception:
            # fallback to naive now if anything goes wrong
            today = datetime.now().date()
            inicio_periodo = today - timedelta(days=60)
            data_inicio = inicio_periodo.strftime('%Y-%m-%d')
            data_fim = today.strftime('%Y-%m-%d')

    # Filtro por campo "data" (dataAssinatura_dt)
    dt_inicio = None
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

    # Status de download: vale o que foi baixado nos últimos 60 dias; se o período filtrado começa
    # antes disso, a janela vai até o início do período. Sem período filtrado, vale tudo.
    if data_periodo or (data_inicio and data_fim):
        desde = datetime.utcnow() - timedelta(days=JANELA_BAIXADOS_DIAS)
        if dt_inicio and dt_inicio < desde:
            desde = dt_inicio
    else:
        desde = None
    marcar_status_download(documentos, desde)
    # contadores sobre a lista filtrada (cofre, nome, período), antes do filtro de status;
    # "por outros" = só outras pessoas baixaram (os que eu também baixei ficam de fora)
    total_mim = sum(1 for d in documentos if d['status_download'] == 'mim')
    total_outros = sum(1 for d in documentos if d['status_download'] == 'outro')
    # opções do filtro "Baixado por": quem baixou algum documento da lista (antes dos filtros de status e pessoa)
    pessoas_filtro = {}
    for d in documentos:
        for p in d['pessoas']:
            pessoas_filtro.setdefault(p['usuario_id'], p)
    pessoas_filtro = sorted(pessoas_filtro.values(), key=lambda p: _chave_nome(p['nome']))
    if baixado_por and not any(p['usuario_id'] == baixado_por for p in pessoas_filtro):
        # pessoa escolhida sem downloads no período: continua no filtro para dar para trocar
        pessoas_filtro.append({'usuario_id': baixado_por, 'nome': baixado_por, 'email': None, 'eu': False})

    # Apply view filter
    if view_status == 'nao_baixado_mim':
        # tudo o que eu ainda não baixei: só outras pessoas baixaram (amarelo) ou ninguém baixou
        documentos = [d for d in documentos if d['status_download'] != 'mim']
    elif view_status == 'baixado_mim':
        documentos = [d for d in documentos if d['status_download'] == 'mim']
    elif view_status == 'baixado_outro':
        documentos = [d for d in documentos if d['status_download'] == 'outro']
    elif view_status == 'nao_baixado':
        documentos = [d for d in documentos if d['status_download'] == 'nao']
    if baixado_por:
        documentos = [d for d in documentos if any(p['usuario_id'] == baixado_por for p in d['pessoas'])]

    # Limit rendering size
    MAX_RENDER = 2000
    if len(documentos) > MAX_RENDER:
        documentos = documentos[:MAX_RENDER]

    # Downloads
    if request.method == "POST" and "download" in request.form:
        selecionados = request.form.getlist("documentos")
        if selecionados:
            # o zip é montado e enviado aos poucos, sem guardar o lote no servidor; quantos arquivos
            # entraram e quantos falharam a página consulta em /lote/<id> depois que o download termina
            lote_id = uuid.uuid4().hex
            nomes = {u: request.form.get(f"doc_nomes[{u}]") or f"{u}.pdf" for u in selecionados}
            resp = Response(_gerar_zip_lote(lote_id, selecionados, nomes, auth.usuario_atual()), mimetype="application/zip")
            resp.headers['Content-Disposition'] = 'attachment; filename="documentos_assinados.zip"'
            resp.headers['X-Lote'] = lote_id
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
    enable_scroll = (len(documentos) >= 10)
    return render_template_string(TEMPLATE, documentos=documentos, cofres=cofres,
                                  cofre_selecionado=cofre_selecionado,
                                  busca_nome=request.form.get("busca_nome", ""), data_inicio=data_inicio,
                                  data_fim=data_fim, ordenar_por=ordenar_por, view_status=view_status,
                                  ICON_UP=ICON_UP_SVG, ICON_DOWN=ICON_DOWN_SVG,
                                  ICON_SUN=ICON_SUN_SVG, ICON_MOON=ICON_MOON_SVG,
                                  ICON_UP_JS=json.dumps(ICON_UP_SVG), ICON_DOWN_JS=json.dumps(ICON_DOWN_SVG),
                                  ICON_UP_WRAPPED_JS=json.dumps(ICON_UP_WRAPPED), ICON_DOWN_WRAPPED_JS=json.dumps(ICON_DOWN_WRAPPED),
                                  enable_scroll=enable_scroll,
                                  total_mim=total_mim, total_outros=total_outros,
                                  baixado_por=baixado_por, pessoas_filtro=pessoas_filtro,
                                  usuario=auth.usuario_atual(), login_ativo=auth.LOGIN_ATIVO,
                                  auto_refresh_last=auto_refresh_last, auto_refresh_next=auto_refresh_next)


if __name__ == "__main__":
    # Respect environment variables for deployment platforms
    port = int(os.environ.get('PORT', '5000'))
    host = os.environ.get('HOST', '0.0.0.0')
    debug = os.environ.get('FLASK_DEBUG', '0') in ('1', 'true', 'True')
    app.run(host=host, port=port, debug=debug)