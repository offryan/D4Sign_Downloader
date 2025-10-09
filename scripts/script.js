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

            // initialize downloaded counter from server-rendered value
            try{
                var downloadedCountEl = document.getElementById('downloaded-count');
                var downloadedCount = downloadedCountEl ? parseInt(downloadedCountEl.textContent, 10) || 0 : 0;
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