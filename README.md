# D4Sign Downloader

App da equipe da Sitawi Finanças do Bem para baixar, em lote, os documentos assinados na D4Sign. Ele também mostra quem da equipe já baixou cada documento, para ninguém baixar o mesmo arquivo duas vezes nem esquecer algum.

![D4Sign Downloader](docs/preview.png)

| Download em lote | Tema escuro | Celular | Login |
|---|---|---|---|
| ![Download em lote concluído](docs/preview-lote.png) | ![Tema escuro](docs/preview-escuro.png) | ![Celular](docs/preview-celular.png) | ![Login](docs/preview-login.png) |

*Capturas com documentos e pessoas fictícios.*

## Como funciona

- **Lista:** mostra os documentos finalizados de todos os cofres da conta na D4Sign, com filtros por cofre, nome, período da assinatura, status e pessoa. Abre nos documentos que ninguém baixou ainda.
- **Status de cada documento**, do ponto de vista de quem está logado:
  - verde: **Baixado por mim** (o × desfaz a minha marcação)
  - amarelo: **Baixado por outro**
  - sem cor: **Não baixado**

  A coluna **Baixado por** mostra quem baixou.
- **Download:** baixa um documento ou vários num único `.zip`. O zip é enviado aos poucos, então lotes de centenas de documentos não pesam no servidor. Quem baixou o quê fica registrado no banco.
- **Login:** com a conta Microsoft da Sitawi quando o app está registrado no Entra ID. Sem esse registro, vale o login com e-mail e senha, só para e-mails `@sitawi.net`, com ativação por link enviado por e-mail.
- **Visual:** cores da Sitawi, tema claro e escuro e layout próprio para celular.

**Tecnologias:** Python e Flask, API da D4Sign, PostgreSQL no Supabase, e-mail pela Brevo. Hospedado no Render.

## Como rodar

1. Instale as dependências:

```bash
python -m venv .venv
.venv\Scripts\activate        # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

2. Crie um arquivo `.env`:

```
TOKEN_API=seu_token_da_d4sign
CRYPT_KEY=sua_crypt_key_da_d4sign
HOST_D4SIGN=https://sandbox.d4sign.com.br/api/v1
DATABASE_URL=postgresql://usuario:senha@host:5432/postgres
SECRET_KEY=um_texto_longo_e_aleatorio
BREVO_API_KEY=xkeysib-...
EMAIL_REMETENTE=remetente@exemplo.com
```

Para o login com conta Microsoft, inclua também `MS_CLIENT_ID`, `MS_TENANT_ID` e `MS_CLIENT_SECRET` do app registrado no Entra ID (single-tenant, redirect URI do tipo Web `<site>/auth/callback`). Com as três, o login com senha sai do ar e cada pessoa continua com o histórico da conta do mesmo e-mail.

Sem `DATABASE_URL`, o app roda sem login e guarda o histórico num arquivo local. Sem a Brevo, o link de confirmação aparece no log. As tabelas do banco estão em [`supabase.sql`](supabase.sql).

3. Rode `python d4sign.py` e abra `http://localhost:5000`.

**No Render:** crie um *Web Service* ligado a este repositório, com build `pip install -r requirements.txt`, início `python d4sign.py` e as mesmas variáveis do `.env`. Em produção, `HOST_D4SIGN` deve apontar para a API de produção da D4Sign.

## Estrutura

| Arquivo | Função |
|---|---|
| `d4sign.py` | App Flask: lista, filtros e downloads |
| `auth.py` | Login (conta Microsoft ou e-mail e senha), cadastro e confirmação por e-mail |
| `registro.py` | Contas e histórico de downloads |
| `email_envio.py` | Envio de e-mails |
| `supabase.sql` | Tabelas e permissões do banco |

Os segredos ficam só nas variáveis de ambiente, nunca no Git.

## Licença

© 2025-2026 Ryan G. Santos. Todos os direitos reservados. O código é público apenas para portfólio e avaliação. Veja o [LICENSE](LICENSE).
