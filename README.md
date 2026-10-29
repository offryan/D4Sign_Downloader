# D4Sign Downloader

App em Flask para a equipe da Sitawi Finanças do Bem encontrar e baixar os documentos assinados na D4Sign. Para cada documento, ele mostra quem da equipe já baixou, para ninguém baixar o mesmo arquivo duas vezes nem esquecer algum.

![D4Sign Downloader — tema claro](docs/preview.png)

| Download em lote | Tema escuro |
|---|---|
| ![Download em lote concluído](docs/preview-lote.png) | ![Tema escuro](docs/preview-escuro.png) |

| Celular | Login |
|---|---|
| ![Layout no celular](docs/preview-celular.png) | ![Tela de login](docs/preview-login.png) |

*Capturas de tela com documentos e pessoas fictícios.*

## Funcionalidades

### Encontrar documentos
- Lista os documentos finalizados (assinados) de todos os cofres da conta na D4Sign. Como só os finalizados são buscados, esse status não aparece na tela
- Todos os filtros ficam num cartão no topo, que também funciona no celular: cofre, nome do documento (a busca acontece enquanto você digita), período da assinatura, status de download e quem baixou
- A lista abre em **Não baixado** (ninguém baixou) com **Todas as pessoas**. As outras opções de status são *Não baixado por mim*, *Todos*, *Baixado por mim* e *Baixado por outro*
- Clique no título da coluna para ordenar pela data da assinatura

### Quem baixou o quê
- Cada pessoa tem o próprio histórico de downloads, salvo no PostgreSQL (Supabase). A coluna **Status** mostra esse histórico do ponto de vista de quem está logado:
  - **Baixado por mim** (verde): eu baixei (*Baixado por mim e outros* quando outras pessoas também baixaram). O × tira só a minha marcação; a linha fica amarela se outra pessoa também tiver baixado
  - **Baixado por outro** (amarelo): só outras pessoas baixaram
  - **Não baixado** (sem cor): ninguém baixou
  - Passar o mouse sobre uma etiqueta verde ou amarela mostra quem baixou e quando, em ordem alfabética
- A coluna **Baixado por** mostra todas as pessoas que baixaram cada documento, lado a lado e em ordem alfabética, com o meu nome em negrito. Passar o mouse sobre um nome mostra o e-mail da pessoa
- O filtro **Baixado por** mostra só os documentos que uma pessoa baixou e pode ser combinado com o filtro de status (escolher uma pessoa com o status em *Não baixado* muda o status para *Todos*)
- Cartões de resumo: documentos na lista, baixados por mim e baixados só por outros, conforme os filtros de cofre, nome e período
- Por padrão, contam os downloads dos últimos 60 dias. Se o período filtrado começar antes, a janela vai até o início do período

### Baixar
- Baixa um documento ou um lote de documentos selecionados num único `.zip`. Os documentos são buscados na D4Sign em paralelo (6 ao mesmo tempo, por padrão), com novas tentativas automáticas, e os que falharem são informados
- O zip é montado e enviado aos poucos: cada PDF vai para o navegador assim que chega da D4Sign. O servidor só guarda alguns documentos por vez na memória, então lotes grandes (centenas de documentos) não pesam no servidor, e nada é gravado em disco
- O registro de quem baixou é feito no banco no fim do lote. Se a pessoa cancelar o download no meio, nada é registrado
- Depois do download, a lista se atualiza sozinha: os documentos ficam verdes e os contadores mudam, sem recarregar a página

### Login da equipe
- Login com e-mail e senha, restrito a um domínio de e-mail (`@sitawi.net`, por padrão). As contas novas são ativadas por um link de confirmação enviado por e-mail
- O login com conta Microsoft (Entra ID) já está pronto em `auth.py`, comentado até ser ativado

### Visual
- Identidade visual da Sitawi (cores do Tema Sitawi, azul `#276DBD` como cor principal, fonte Segoe UI), também nas telas de login e no e-mail de confirmação
- Logo próprio do app (`static/logo.svg`): um documento com seta de download e selo verde de assinado. Aparece no topo, ao lado da marca da Sitawi, e como ícone da aba do navegador
- Abre no tema claro; o tema escuro fica a um clique, e o navegador lembra a escolha
- No celular, cada documento vira um cartão e o botão de download fica fixo na parte de baixo da tela

### Integrações
- Cache opcional no Redis para as datas de assinatura, com um processo de atualização em segundo plano
- Endpoint de webhook (`POST /webhook/d4sign`) para receber atualizações de assinatura da D4Sign

**Tecnologias:** Python, Flask, Requests, API REST da D4Sign, PostgreSQL / Supabase (psycopg), Brevo (e-mail transacional), Redis (opcional). Hospedado no Render.

## Como funciona o login

- **Cadastro:** nome, e-mail e senha (mínimo de 8 caracteres). Só são aceitos e-mails do `DOMINIO_PERMITIDO`, e o banco também recusa outros domínios. A senha é guardada só como hash scrypt.
- **Confirmação por e-mail:** a conta só funciona depois que a pessoa abre o link enviado por e-mail. O link vale por 24 horas e funciona uma vez só. Refazer o cadastro antes de confirmar invalida o link anterior, para ninguém "reservar" o e-mail de um colega.
- **Proteção:** 5 senhas erradas para o mesmo e-mail bloqueiam o acesso por 15 minutos, e o botão "reenviar link" é limitado a 1 vez por minuto e 5 por hora para cada e-mail.
- **Sem banco configurado:** o app roda sem login (útil para desenvolvimento local), e o histórico de downloads vai para um arquivo local, `downloads.json`.

## Como rodar localmente

1. Crie um ambiente virtual e instale as dependências:

```powershell
# Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

```bash
# Linux / macOS
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Crie um arquivo `.env` com estas variáveis (exemplo):

```
TOKEN_API=seu_token_aqui
CRYPT_KEY=sua_crypt_key_aqui
HOST_D4SIGN=https://sandbox.d4sign.com.br/api/v1
PORT=5000
# login e histórico por pessoa (deixe vazio para rodar sem login)
DATABASE_URL=postgresql://usuario:senha@host:5432/postgres
SECRET_KEY=um_texto_longo_e_aleatorio
# e-mails de confirmação (sem eles, o link aparece no log do servidor)
BREVO_API_KEY=xkeysib-...
EMAIL_REMETENTE=remetente@exemplo.com
# opcionais
DOMINIO_PERMITIDO=sitawi.net
D4SIGN_DOWNLOAD_WORKERS=6
REDIS_URL=redis://localhost:6379/0
D4SIGN_AUTO_REFRESH_INTERVAL=3600
```

3. Se for usar banco, crie as tabelas com o [`supabase.sql`](supabase.sql) (no Supabase: *SQL Editor*). O arquivo também traz, comentados, os comandos para criar um usuário de banco com acesso só a essas tabelas.

4. Rode o app:

```powershell
python d4sign.py
```

Depois abra `http://localhost:5000`.

## Publicar no Render

1. Faça um fork ou clone deste repositório na sua conta do GitHub.

2. No Render.com, crie um novo *Web Service*, conecte o repositório do GitHub e cadastre estas variáveis de ambiente no painel do Render:

- `TOKEN_API`: o token da D4Sign
- `CRYPT_KEY`: a crypt key da D4Sign
- `HOST_D4SIGN`: opcional (o padrão é o sandbox; em produção, use a URL de produção da D4Sign)
- `DATABASE_URL`: string de conexão do Supabase (use o *Session pooler*, porta 5432); liga o login e o histórico por pessoa
- `SECRET_KEY`: texto aleatório usado para assinar as sessões e os links de confirmação
- `BREVO_API_KEY` e `EMAIL_REMETENTE`: chave da API da Brevo e um remetente verificado na Brevo. O plano gratuito do Render bloqueia as portas SMTP, então os e-mails saem pela API HTTP da Brevo (gratuita até 300 e-mails por dia). Nos planos pagos, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USUARIO` e `SMTP_SENHA` também funcionam
- `DOMINIO_PERMITIDO`: opcional, domínio de e-mail que pode se cadastrar (padrão `sitawi.net`)
- `REDIS_URL`: opcional, liga o cache no Redis
- `PORT`: o Render preenche sozinho, mas mantenha `5000` como reserva

3. Comando de build: `pip install -r requirements.txt`
   Comando de início: `python d4sign.py`

## Estrutura do projeto

- `d4sign.py`: app Flask (chamadas à API da D4Sign, template da página, filtros e downloads)
- `auth.py`: login, cadastro, confirmação por e-mail (e o login Microsoft comentado)
- `registro.py`: contas e histórico de downloads por pessoa (PostgreSQL ou arquivo local)
- `email_envio.py`: envio de e-mails pela Brevo ou por SMTP
- `supabase.sql`: tabelas do banco, row level security e permissões
- `docs/`: capturas de tela usadas neste README

## Observações
- Os segredos ficam em variáveis de ambiente. Não envie `TOKEN_API`, `CRYPT_KEY`, `DATABASE_URL`, `SECRET_KEY` nem `BREVO_API_KEY` para o Git.
- A lista se ajusta à altura da janela e, quando há muitos resultados, rola dentro da tabela, mantendo o cabeçalho visível.

## 📄 Licença

© 2025-2026 Ryan G. Santos. Todos os direitos reservados. O código é público apenas para portfólio e avaliação: copiar, modificar, distribuir ou usar comercialmente sem autorização por escrito não é permitido. Veja o arquivo [LICENSE](LICENSE).
