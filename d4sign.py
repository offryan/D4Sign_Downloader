"""Minimal, cleaned and optimized D4Sign app.

Improvements made:
- In-memory TTL cache for external API calls (listar_cofres, listar_documentos)
- Pre-parse dates once and store datetime object for fast filtering
- Limit number of rendered documents to avoid huge HTML payloads
- Log timing for index page generation
"""

from flask import Flask, render_template, render_template_string, request, send_file, jsonify
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

app = Flask(__name__)

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
TEMPLATE = r"""<html lang="pt-BR">
<head>
    <meta charset="utf-8">
    <title>Documentos Assinados</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <link rel="stylesheet" type="text/css" href="style/style.css">
    <style>
        :root{
            --card-bg:#fff;
            /* approximate single row height used for scroll container sizing; tweak if needed */
            --row-height:56px;
            --page-bg:#f6f7f9;
            --muted:#6b7280;
            --accent:#111827;
            --border:#e6e9ee;
            --text-color:#111111;
            --btn-bg:#111111;
            --btn-text:#ffffff;
            --badge-bg:#111111;
            --spinner-border: rgba(0,0,0,0.12);
            --spinner-top: rgba(0,0,0,0.7);
            --success-bg: #10b981;
            --error-bg: #ef4444;
                --download-badge-bg: #e6f7ef;
                --download-badge-text: #065f46;
                    --counter-flash-bg: #0b74de;
                    --counter-flash-text: #ffffff;
            --modal-bg: #fff;
            --modal-text: var(--text-color);
            --row-hover-bg: #fbfdff;
        }
        /* Dark mode overrides: add class 'dark-mode' to <body> */
        .dark-mode {
            --card-bg:#0b1220;
            --page-bg:#071018;
            --muted:#9aa4b2;
            --accent:#ffffff;
            --border:#1f2937;
            --text-color:#ffffff;
            --btn-bg:#ffffff;
            --btn-text:#111111;
            --badge-bg:#ffffff;
            --spinner-border: rgba(255,255,255,0.12);
            --spinner-top: rgba(255,255,255,0.9);
            --success-bg: #059669;
            --error-bg: #ef4444;
                --download-badge-bg: #064e3b;
                --download-badge-text: #dff7ea;
                --counter-flash-bg: #60a5fa;
                --counter-flash-text: #05203a;
                --modal-bg: #071018;
                --modal-text: var(--text-color);
            --row-hover-bg: rgba(255,255,255,0.03);
        }
    html,body{height:100%;margin:0;background:var(--page-bg);font-family:Inter, Arial, Helvetica, sans-serif;color:var(--text-color);transition:background-color .25s ease,color .25s ease}
        .page{max-width:1100px;margin:28px auto;padding:18px}
    .card{background:var(--card-bg);border-radius:12px;padding:24px;box-shadow:0 6px 18px rgba(15,23,42,0.06);transition:background-color .25s ease,box-shadow .25s ease}
        .controls{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:18px}
        .controls .group{display:flex;flex-direction:column;gap:6px}
        label{font-size:13px;color:var(--muted)}
        select,input[type=text],input[type=date]{padding:10px 12px;border:1px solid var(--border);border-radius:8px;background:#fff}
    .title-row{display:flex;justify-content:space-between;align-items:center;margin:12px 0 18px}
        h1{margin:0;font-size:28px;letter-spacing:-0.02em}
        table{width:100%;border-collapse:collapse;margin-top:6px}
        thead th{font-weight:600;padding:12px 10px;border-bottom:1px solid var(--border);background:transparent;text-align:left}
    tbody td{padding:14px 10px;border-bottom:1px solid var(--border);color:var(--text-color)}
    tbody tr:hover td{background:var(--row-hover-bg)}
    /* In dark mode ensure hovered row text (including links) uses the page text color */
    .dark-mode tbody tr:hover td,
    .dark-mode tbody tr:hover td .doc-name,
    .dark-mode tbody tr:hover td a{
        color: var(--text-color);
    }
    .doc-name{font-weight:500}
    .date-col{text-align:left;color:var(--muted)}
    /* master checkbox / badge */
        .master-wrap{display:flex;align-items:center;gap:10px}
        #master-check{width:18px;height:18px;cursor:pointer;border:1px solid #cbd5e1;border-radius:6px;background:#fff}
    #selected-count{font-size:12px;color:var(--btn-text);background:var(--badge-bg);padding:6px 8px;border-radius:999px}
    th.sortable{cursor:pointer;user-select:none}
    th.sortable.active-sort{color:var(--accent);font-weight:700}
    /* icons sizing */
    #sort-arrow, #sort-arrow-data{display:inline-flex;align-items:center;margin-left:8px;color:var(--muted)}
    .sort-icon{width:10px;height:10px;display:inline-block;vertical-align:middle;line-height:10px}
    .sort-icon svg{width:10px;height:10px;display:block}
    /* Ensure inline SVG icons inherit color so toggles are visible in both modes */
    .sort-icon svg, #dark-mode-toggle svg{fill:currentColor;stroke:currentColor}
    /* Scrollbar styling to avoid white track in dark mode and ensure transparent background behind scroll areas */
    .table-container{background:transparent}
    .table-container::-webkit-scrollbar{width:10px}
    .table-container::-webkit-scrollbar-thumb{background:rgba(0,0,0,0.18);border-radius:8px}
    .table-container::-webkit-scrollbar-track{background:transparent}
    .dark-mode .table-container::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.12)}
    .dark-mode .table-container::-webkit-scrollbar-track{background:transparent}
    .table-container{scrollbar-width:auto;scrollbar-color:rgba(0,0,0,0.18) transparent}
    .dark-mode .table-container{scrollbar-color:rgba(255,255,255,0.12) transparent}
    /* fallback <i> sizing */
    .sort-icon i{font-style:normal;font-size:10px;line-height:10px;display:inline-block}
    /* In dark mode make sort arrow icons fully visible (use --accent which is white) */
    .dark-mode .sort-icon,
    .dark-mode #sort-arrow,
    .dark-mode #sort-arrow-data{
        color: var(--accent);
    }
    .dark-mode .sort-icon svg,
    .dark-mode .sort-icon path,
    .dark-mode .sort-icon g{
        fill: var(--accent) !important;
        stroke: var(--accent) !important;
    }
    .dark-mode .sort-icon i{color:var(--accent)}
    /* In dark mode make the sun toggle icon black for contrast against the white moon */
    .dark-mode .toggle-sun svg,
    .dark-mode .toggle-sun path,
    .dark-mode .toggle-sun g{
        fill: var(--btn-text) !important;
        stroke: var(--btn-text) !important;
        color: var(--btn-text) !important;
    }
    /* small spinner used while sorting */
    .spinner{width:10px;height:10px;border:1.5px solid var(--spinner-border);border-top-color:var(--spinner-top);border-radius:50%;display:inline-block;vertical-align:middle;box-sizing:border-box;animation:spin .8s linear infinite}
    @keyframes spin{from{transform:rotate(0)}to{transform:rotate(360deg)}}
        /* Download button */
    .download-btn{background:var(--btn-bg);color:var(--btn-text);padding:12px 18px;border-radius:10px;border:none;cursor:pointer;font-weight:600;transition:background-color .2s ease,color .2s ease}
        /* Dark mode toggle button styling */
        #dark-mode-toggle{border:1px solid var(--border);padding:6px 8px;border-radius:8px;cursor:pointer;background:transparent;color:var(--muted);display:inline-flex;align-items:center;gap:6px;transition:background-color .2s ease,color .2s ease,border-color .2s ease}
        #dark-mode-toggle .toggle-icon{display:inline-block;line-height:0;transition:opacity .25s ease,transform .25s ease;opacity:0;transform:scale(.9);width:18px;height:18px}
        #dark-mode-toggle .toggle-icon svg{width:18px;height:18px;display:block}
        #dark-mode-toggle .toggle-icon.visible{opacity:1;transform:scale(1)}
        .download-area{display:flex;justify-content:space-between;align-items:center;margin-top:18px}
        /* Small screen: make table rows stacked cards */
        @media (max-width:720px){
            .controls{flex-direction:column;align-items:stretch}
            .title-row{flex-direction:column;align-items:flex-start;gap:10px}
            table thead{display:none}
            table, tbody, tr, td{display:block;width:100%}
            tbody tr{margin:8px 0;padding:10px;border-radius:10px;background:#fff;box-shadow:0 1px 0 rgba(16,24,40,0.03)}
            tbody td{display:flex;justify-content:space-between;padding:12px}
            tbody td::before{content:attr(data-label);color:var(--muted);font-size:12px;margin-right:8px}
        }
        /* Scroll container that activates when many documents are shown. */
        .table-container.scroll-enabled{max-height:calc(var(--row-height) * 9);overflow-y:auto;scroll-behavior:smooth;-webkit-overflow-scrolling:touch;padding-right:6px}
        /* Keep the table header visible while scrolling on larger screens */
        @media (min-width:721px){
            .table-container.scroll-enabled thead th{position:sticky;top:0;background:var(--card-bg);z-index:3}
        }

    /* Estilo do modal */
    .modal {
  display: none;
  position: fixed;
  z-index: 2000;
  left: 0; top: 0;
  width: 100%; height: 100%;
  background: rgba(0,0,0,0.6);
}
.modal-content {
    position: relative;
    background: var(--modal-bg);
    margin: 12% auto;
    padding: 20px;
    border-radius: 12px;
    width: 420px;
    text-align: center;
    box-shadow: 0 8px 24px rgba(0,0,0,0.18);
    color: var(--modal-text);
}
.modal-content h2 {
    font-size: 18px;
    margin: 0 0 6px;
}
.modal-content .modal-body{display:flex;flex-direction:column;align-items:center;gap:10px;padding:6px}
.modal-spinner{width:72px;height:72px;border:8px solid var(--spinner-border);border-top-color:var(--spinner-top);border-radius:50%;box-sizing:border-box;animation:spin .8s linear infinite}
.modal-icon{width:64px;height:64px;border-radius:999px;display:flex;align-items:center;justify-content:center;font-size:28px}
.modal-icon.success{background:var(--success-bg);color:#fff}
.modal-icon.error{background:var(--error-bg);color:#fff}
.modal-content .msg{font-size:15px;color:var(--muted);max-width:360px}
.modal-actions{display:flex;gap:8px;margin-top:6px}
.modal .close {
  position: absolute;
  top: 10px; right: 15px;
  font-size: 22px;
  font-weight: bold;
  color: #aaa;
  cursor: not-allowed;
  pointer-events: none;
}
.modal .close.enabled {
  color: #333;
  cursor: pointer;
  pointer-events: auto;
}

/* Badge to indicate a downloaded document */
.baixado-badge{display:inline-block;background:var(--download-badge-bg);color:var(--download-badge-text);padding:3px 8px;border-radius:999px;font-size:12px;margin-left:8px}

/* Small counter shown near the header */
.downloaded-counter{font-size:13px;color:var(--muted);display:inline-flex;align-items:center;gap:6px}

/* micro-flash animation for counter */
.downloaded-counter .count-val{display:inline-block;padding:3px 6px;border-radius:6px;transition:transform .18s ease}
.downloaded-counter .count-val.flash{animation:counterFlash .45s ease both}
@keyframes counterFlash{
    0% { transform: scale(1); background: transparent; color: inherit }
    30% { transform: scale(1.18); background: var(--counter-flash-bg); color: var(--counter-flash-text) }
    70% { transform: scale(1.04); background: var(--counter-flash-bg); color: var(--counter-flash-text) }
    100% { transform: scale(1); background: transparent; color: inherit }
}
    </style>
</head>
<body>
    <div class="page">
        <div class="card">
            <form method="POST" id="filtro-form" class="controls">
                <div class="group">
                    <label>Lote por fundo</label>
                    <select name="cofre" onchange="this.form.submit()">
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
                    <label>Buscar por nome</label>
                    <input type="text" id="busca-nome" name="busca_nome" value="{{ busca_nome }}" placeholder="Pesquisar documento..." autocomplete="off">
                </div>

                <div class="group" style="position:relative">
                    <label>Período (assinatura)</label>
                    <div style="display:flex;gap:8px;align-items:center">
                        <input type="text" id="data-periodo" name="data_periodo" placeholder="YYYY-MM-DD - YYYY-MM-DD" value="{{ (data_inicio and data_fim) and (data_inicio ~ ' - ' ~ data_fim) or (data_periodo or '') }}" readonly style="cursor:pointer">
                        <button type="button" class="download-btn" id="btn-data-periodo" style="padding:6px 8px">📅</button>
                    </div>
                    <!-- hidden fields to submit legacy names -->
                    <input type="hidden" name="data_inicio" id="data_inicio_hidden" value="{{ data_inicio or '' }}">
                    <input type="hidden" name="data_fim" id="data_fim_hidden" value="{{ data_fim or '' }}">
                    <div id="data-periodo-popup" class="range-popup" style="display:none;position:absolute;z-index:80;background:#fff;padding:10px;border:1px solid var(--border);box-shadow:0 6px 18px rgba(15,23,42,0.06);border-radius:8px">
                        <div style="display:flex;gap:8px;align-items:center"><input type="date" id="data-periodo-start"> <span style="font-size:12px;color:var(--muted)">até</span> <input type="date" id="data-periodo-end"></div>
                        <div id="data-periodo-error" class="range-error" style="display:none;margin-top:8px;color:#b91c1c;font-size:12px"></div>
                        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:8px"><button type="button" class="download-btn" id="data-periodo-apply">Aplicar</button><button type="button" id="data-periodo-cancel" style="background:#eee;color:#111;padding:6px 10px;border-radius:8px;border:none">Cancelar</button></div>
                    </div>
                </div>

                <!-- ordering is now controlled by clicking the table header -->
                <input type="hidden" name="ordenar_por" id="ordenar_por" value="{{ ordenar_por or '' }}">
                <!-- filter for view status: default 'finalizado' or 'baixado' -->
                <input type="hidden" name="view_status" id="view_status_hidden" value="{{ request.form.get('view_status','') or request.args.get('view_status','') or 'finalizado' }}">
            </form>

            <div class="title-row">
                <h1>Documentos assinados</h1>
                <div style="display:flex;gap:12px;align-items:center">
                    <div style="color:var(--muted);font-size:13px"><div>Mostrando {{ documentos|length }} documentos</div></div>

                    <div class="downloaded-counter" id="downloaded-counter" title="Total de arquivos baixados">
                        <span style="font-weight:600">Baixados:</span>
                        <span id="downloaded-count" class="count-val">{{ total_downloaded or 0 }}</span>
                    </div>
                    <button id="dark-mode-toggle" title="Alternar modo noturno" aria-label="Alternar modo noturno">
                        <span class="toggle-icon toggle-sun" aria-hidden="true"></span>
                        <span class="toggle-icon toggle-moon" aria-hidden="true"></span>
                    </button>
                </div>
            </div>

            <form method="POST" id="download-form">
                <div class="table-container {% if documentos|length >= 10 %}scroll-enabled{% endif %}">
                    <table>
                    <thead>
                        <tr>
                            <th style="width:70px"><div class="master-wrap"><input type="checkbox" id="master-check" title="Selecionar todos"><span id="selected-count">0</span></div></th>
                            <th>Documento</th>
                            <th style="width:140px" class="sortable {% if ordenar_por in ['data_desc','data_asc'] %}active-sort{% endif %}" id="th-data">Data <span id="sort-arrow-data">{% if ordenar_por=='data_desc' %}<span class="sort-icon">{{ ICON_UP|safe }}</span>{% elif ordenar_por=='data_asc' %}<span class="sort-icon">{{ ICON_DOWN|safe }}</span>{% else %}<span class="sort-icon">{{ ICON_UP|safe }}</span>{% endif %}</span></th>

                            <th style="width:160px">Cofre</th>
                            <th style="width:120px">
                                                <div class="status-header" style="display:flex;align-items:center;gap:8px">
                                                    <span style="font-weight:600;font-size:13px">Status</span>
                                                    <select id="status-filter" name="status_filter" onchange="setViewStatusAndSubmit(this.value)" style="min-width:140px;padding:6px;border-radius:6px;border:1px solid var(--border)">
                                                        <option value="finalizado" {% if view_status == 'finalizado' %}selected{% endif %}>Finalizado</option>
                                                        <option value="baixado" {% if view_status == 'baixado' %}selected{% endif %}>Arquivos baixados</option>
                                                        <option value="nao_baixado" {% if view_status == 'nao_baixado' %}selected{% endif %}>Não baixados</option>
                                                    </select>
                                                </div>
                            </th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for doc in documentos %}
                        <tr>
                            <td>
                                <input type="checkbox" name="documentos" value="{{ doc['uuidDoc'] }}">
                                <input type="hidden" name="doc_nomes[{{ doc['uuidDoc'] }}]" value="{{ doc['nomeOriginal'] }}">
                            </td>
                            <td class="doc-name">{{ doc['nomeLimpo'] }}</td>
                            <td class="date-col" data-label="Data">{{ doc['dataAssinatura'] }}</td>

                            <td>{{ doc['cofre_nome'] }}</td>
                            <td>
                                {{ doc['statusName'] }}
                                {% if doc.get('baixado') %}
                                    <span class="baixado-badge">Baixado</span>
                                {% endif %}
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                    </table>
                </div>

            <div class="download-area">
                <div style="color:var(--muted);font-size:13px">Selecione documentos para baixar</div>
            <div style="display:flex;gap:8px">
                <button name="download" class="download-btn">Download Selecionados</button>
            </div>
            </div>
            </form>
        </div>
    <!-- icon templates (hidden) - wrapped so injected HTML includes .sort-icon -->
    <template id="icon-up-tpl"><span class="sort-icon">{{ ICON_UP|safe }}</span></template>
    <template id="icon-down-tpl"><span class="sort-icon">{{ ICON_DOWN|safe }}</span></template>
    <!-- theme icons -->
    <template id="icon-sun-tpl"><span class="sort-icon">{{ ICON_SUN|safe }}</span></template>
    <template id="icon-moon-tpl"><span class="sort-icon">{{ ICON_MOON|safe }}</span></template>
    </div>

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
                (function(){
            var master = document.getElementById('master-check');
            function setAll(checked){
                var inputs = document.querySelectorAll('input[name="documentos"]');
                for(var i=0;i<inputs.length;i++) inputs[i].checked = checked;
            }
            function updateCounter(){
                var inputs = document.querySelectorAll('input[name="documentos"]');
                var checked = Array.prototype.slice.call(inputs).filter(function(i){return i.checked;}).length;
                var countEl = document.getElementById('selected-count');
                if(countEl) countEl.textContent = checked;
                return {checked: checked, total: inputs.length};
            }
            if(master){
                master.addEventListener('change', function(){ setAll(master.checked); updateCounter(); master.indeterminate = false; });
            }
            document.addEventListener('change', function(e){
                if(!e.target || e.target.name !== 'documentos') return;
                var inputs = document.querySelectorAll('input[name="documentos"]');
                var all = true, any = false;
                for(var i=0;i<inputs.length;i++){
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
                            var newSummary = doc.querySelector('.title-row > div > div');
                            var oldSummary = document.querySelector('.title-row > div > div');
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
                            var newSummary = doc.querySelector('.title-row > div > div');
                            var oldSummary = document.querySelector('.title-row > div > div');
                            if(newSummary && oldSummary) oldSummary.textContent = newSummary.textContent;
                            // refresh selection counter and master checkbox state
                            updateCounter();
                        }catch(e){ console.error('live-parse error', e); }
                    }).catch(function(e){ console.error('live-search error', e); });
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
                        // Require both dates to be filled
                        if(!start.value || !end.value){
                            if(errorEl){ errorEl.textContent = 'Ambas as datas são obrigatórias.'; errorEl.style.display = 'block'; }
                            return;
                        }
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
                    if(saved === 'dark' || (saved !== 'light' && window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches)){
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

            // initialize downloaded counter and apply localStorage badges
            try{
                var downloadedCountEl = document.getElementById('downloaded-count');
                var downloadedCount = downloadedCountEl ? parseInt(downloadedCountEl.textContent, 10) || 0 : 0;

                // Load downloaded docs from localStorage and apply badges
                try{
                    var downloadedDocs = JSON.parse(localStorage.getItem('d4sign:downloaded_docs') || '{}');
                    var sixtyDaysAgo = Date.now() - (60 * 24 * 60 * 60 * 1000);
                    var recentCount = 0;

                    // Clean up old entries and count recent downloads
                    Object.keys(downloadedDocs).forEach(function(docId){
                        if(downloadedDocs[docId].timestamp < sixtyDaysAgo){
                            delete downloadedDocs[docId];
                        } else {
                            recentCount++;
                            // Find row with this document ID and add badge if not present
                            var checkbox = document.querySelector('input[name="documentos"][value="' + docId + '"]');
                            if(checkbox){
                                var row = checkbox.closest('tr');
                                if(row){
                                    var statusCell = row.querySelector('td:last-child');
                                    if(statusCell && !statusCell.querySelector('.baixado-badge')){
                                        var badge = document.createElement('span');
                                        badge.className = 'baixado-badge';
                                        badge.textContent = 'Baixado';
                                        statusCell.appendChild(document.createTextNode(' '));
                                        statusCell.appendChild(badge);
                                    }
                                }
                            }
                        }
                    });

                    // Save cleaned up data back
                    localStorage.setItem('d4sign:downloaded_docs', JSON.stringify(downloadedDocs));

                    // Update counter with localStorage data
                    if(downloadedCountEl){
                        downloadedCountEl.textContent = recentCount;
                        downloadedCount = recentCount;
                    }
                }catch(e){ console.error('Error loading from localStorage:', e); }
            }catch(e){ var downloadedCount = 0; }

            // ensure status-filter selection updates the hidden view_status before submitting
            var statusFilter = document.getElementById('status-filter');
            var viewStatusHidden = document.getElementById('view_status_hidden');
            // helper exposed for inline onchange to guarantee hidden value is set before submit
            window.setViewStatusAndSubmit = function(v){ try{ var vh = document.getElementById('view_status_hidden'); if(vh) vh.value = v; var f = document.getElementById('filtro-form'); if(f) f.submit(); }catch(e){} };
            if(statusFilter && viewStatusHidden){
                // initialize select based on hidden input
                try{ if(viewStatusHidden.value) statusFilter.value = (viewStatusHidden.value || 'finalizado'); }catch(e){}
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
                                    // capture zip-count header if present, then handle blob
                                    var zipCountHeader = resp.headers.get('X-Zip-Count');
                                    return resp.blob().then(function(blob){
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
                                                    modalMsgEl.textContent = (filesPart ? filesPart : '') + 'Tamanho total: ' + sizeStr + ' MB';
                                                }
                                            }catch(e){}
                                            // update modal to completed and enable close
                                            showSuccess('Download Concluído');
                                        // Save downloaded document IDs to localStorage for persistence
                                        try{
                                            var selectedIds = fd.getAll('documentos');
                                            var downloadedDocs = JSON.parse(localStorage.getItem('d4sign:downloaded_docs') || '{}');
                                            var now = new Date().toISOString();
                                            selectedIds.forEach(function(id){
                                                downloadedDocs[id] = {downloaded_at: now, timestamp: Date.now()};
                                            });
                                            // Clean up entries older than 60 days
                                            var sixtyDaysAgo = Date.now() - (60 * 24 * 60 * 60 * 1000);
                                            Object.keys(downloadedDocs).forEach(function(k){
                                                if(downloadedDocs[k].timestamp < sixtyDaysAgo) delete downloadedDocs[k];
                                            });
                                            localStorage.setItem('d4sign:downloaded_docs', JSON.stringify(downloadedDocs));
                                        }catch(e){ console.error('Error saving to localStorage:', e); }
                                        // increment downloaded counter visually with micro-flash
                                        try{
                                            downloadedCount = (downloadedCount || 0) + 1;
                                            if(downloadedCountEl) downloadedCountEl.textContent = downloadedCount;
                                            // add flash class to parent .count-val and remove after animation
                                            try{
                                                var cv = downloadedCountEl;
                                                if(cv){
                                                    cv.classList.remove('flash');
                                                    // force reflow to restart animation
                                                    void cv.offsetWidth;
                                                    cv.classList.add('flash');
                                                    var cleaned = false;
                                                    var onend = function(){ if(!cleaned){ cleaned = true; cv.classList.remove('flash'); cv.removeEventListener('animationend', onend); } };
                                                    cv.addEventListener('animationend', onend);
                                                    // fallback removal after 700ms
                                                    setTimeout(onend, 700);
                                                }
                                            }catch(e){}
                                        }catch(e){}
                                        // Reload page after short delay to show updated badges
                                        setTimeout(function(){ window.location.reload(); }, 2000);
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
</body>
</html>"""


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

    # Default date range: 60 days ago to today
    date_filter_applied = any([data_periodo, data_inicio, data_fim])
    # Only auto-fill defaults on initial GET load without filters
    if not date_filter_applied and request.method == 'GET':
        try:
            hoje = datetime.now()
            inicio_periodo = hoje - timedelta(days=60)
            data_inicio = inicio_periodo.strftime('%Y-%m-%d')
            data_fim = hoje.strftime('%Y-%m-%d')
        except Exception:
            hoje = datetime.now()
            inicio_periodo = hoje - timedelta(days=60)
            data_inicio = inicio_periodo.strftime('%Y-%m-%d')
            data_fim = hoje.strftime('%Y-%m-%d')

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
                    # Fix: Find the LAST occurrence of '.' for extension to avoid treating periods in filename as separators
                    if '.' in safe_name:
                        last_dot_idx = safe_name.rfind('.')
                        base = safe_name[:last_dot_idx]
                        ext = safe_name[last_dot_idx:]
                    else:
                        base = safe_name
                        ext = '.pdf'
                        safe_name += '.pdf'
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
    enable_scroll = (len(documentos) >= 10)
    return render_template_string(TEMPLATE, documentos=documentos, cofres=cofres,
                                  cofre_selecionado=cofre_selecionado,
                                  busca_nome=request.form.get("busca_nome", ""), data_inicio=data_inicio,
                                  data_fim=data_fim, ordenar_por=ordenar_por,
                                  ICON_UP=ICON_UP_SVG, ICON_DOWN=ICON_DOWN_SVG,
                                  ICON_SUN=ICON_SUN_SVG, ICON_MOON=ICON_MOON_SVG,
                                  ICON_UP_JS=json.dumps(ICON_UP_SVG), ICON_DOWN_JS=json.dumps(ICON_DOWN_SVG),
                                  ICON_UP_WRAPPED_JS=json.dumps(ICON_UP_WRAPPED), ICON_DOWN_WRAPPED_JS=json.dumps(ICON_DOWN_WRAPPED),
                                  enable_scroll=enable_scroll,
                                  total_downloaded=len(get_downloaded_uuids()),
                                  auto_refresh_last=auto_refresh_last, auto_refresh_next=auto_refresh_next)


if __name__ == "__main__":
    # Respect environment variables for deployment platforms
    port = int(os.environ.get('PORT', '5000'))
    host = os.environ.get('HOST', '0.0.0.0')
    debug = os.environ.get('FLASK_DEBUG', '0') in ('1', 'true', 'True')
    app.run(host=host, port=port, debug=debug)