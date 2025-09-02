## Estrutura Visual do Front-End
1. Layout Geral
    Framework sugerido: React (com TailwindCSS ou Material UI para estilização rápida e moderna)
    Página principal: Dashboard com filtros e lista de documentos

2. Componentes Principais
    Filtro por Fundo: Dropdown com busca (autocomplete)
    Filtro por Período: Date picker com intervalo (últimos 30 dias até ontem)
    Botão "Buscar": Aciona chamada à API com os filtros aplicados
   
3. Lista de Documentos
    Cards ou Tabela responsiva com:
        Nome do documento (ex: Nome_2025-08-15_R$1500.pdf)
        Data da última assinatura
        Checkbox individual
    Ordenação: Por data ou nome

4. Seletor de Documentos + Download
    Botão "Download Selecionados"
        Cria ZIPs de até 100MB
        Mostra progresso e divide em múltiplos arquivos se necessário
        Pode usar uma barra de progresso ou spinner

5. Extras
    Feedback visual (ex: loading spinner, mensagens de erro/sucesso)
    Responsividade (mobile-first)
    Dark mode (opcional, mas agrada bastante)
    Paginação ou scroll infinito se houver muitos documentos
