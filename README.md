# D4Sign Downloader

Flask app for the Sitawi team to find and download signed documents from D4Sign. It shows, for each document, who on the team already downloaded it, so nobody downloads the same file twice or misses one.

![D4Sign Downloader — light mode](docs/preview.png)

| Dark mode | Phone | Login |
|---|---|---|
| ![Dark mode](docs/preview-escuro.png) | ![Phone layout](docs/preview-celular.png) | ![Login page](docs/preview-login.png) |

*Screenshots with fictitious documents and people.*

## Features

### Finding documents
- Lists the finished (signed) documents from every safe ("cofre") in the D4Sign account. Only finished documents are fetched, so that status is not shown on screen
- All filters in one card at the top (they also work on phones): safe, document name (live search), signature period, download status and who downloaded
- Opens on **Não baixado** (not downloaded by anyone) with **Todas as pessoas**; the other status options are *Não baixado por mim*, *Todos*, *Baixado por mim* and *Baixado por outro*
- Sort by signature date by clicking the column header

### Who downloaded what
- Each person has their own download history, saved in PostgreSQL (Supabase). The **Status** column shows it from the point of view of whoever is logged in:
  - **Baixado por mim** (green): I downloaded it (*Baixado por mim e outros* when other people did too). The × removes only my mark; the row turns yellow if someone else also downloaded it
  - **Baixado por outro** (yellow): only other people downloaded it
  - **Não baixado** (no color): nobody downloaded it
  - Hovering over a green or yellow tag lists who downloaded it and when, in alphabetical order
- The **Baixado por** column shows everyone who downloaded each document, side by side in alphabetical order, with my own name in bold; hovering over a name shows that person's e-mail
- The **Baixado por** filter shows only the documents a given person downloaded, and can be combined with the status filter (choosing a person while the status is *Não baixado* switches it to *Todos*)
- Summary cards: documents in the list, downloaded by me and downloaded only by others, for the current safe, name and period filters
- Downloads count within the last 60 days by default; if the period filter starts earlier, the window goes back to the start of the period

### Downloading
- Downloads one document or a batch of selected documents as a single `.zip`. Batches are downloaded in parallel (6 at a time by default) with automatic retries, and any document that fails is reported
- After a download the list updates on its own: the documents turn green and the counters change, without reloading the page

### Team login
- Login with e-mail and password, restricted to one e-mail domain (`@sitawi.net` by default). New accounts are activated through a confirmation link sent by e-mail
- Microsoft (Entra ID) login is ready in `auth.py`, commented out until it is enabled

### Design
- Sitawi visual identity (Tema Sitawi colors, blue `#276DBD` as the main color, Segoe UI), also on the login pages and the confirmation e-mail
- Opens in light mode; dark mode is one click away and the choice is remembered in the browser
- On phones each document becomes a card and the download button stays at the bottom of the screen

### Integrations
- Optional Redis cache for signature dates, plus a background refresh worker
- Webhook endpoint (`POST /webhook/d4sign`) to receive signature updates from D4Sign

**Stack:** Python, Flask, Requests, D4Sign REST API, PostgreSQL / Supabase (psycopg), Brevo (transactional e-mail), Redis (optional). Deployed on Vercel (Render also supported).

## How login works

- **Sign up:** name, e-mail and password (at least 8 characters). Only e-mails from `DOMINIO_PERMITIDO` are accepted, and the database rejects other domains too. Passwords are stored only as scrypt hashes.
- **E-mail confirmation:** the account only works after the link sent by e-mail is opened. The link is valid for 24 hours and works once. Signing up again before confirming invalidates the previous link, so nobody can "reserve" a colleague's e-mail.
- **Protection:** 5 wrong passwords for the same e-mail block it for 15 minutes, and the "resend link" button is limited to 1 per minute and 5 per hour per e-mail.
- **No database configured:** the app runs without login (useful for local development), and the download history goes to a local `downloads.json` file.

## Setup (local)

1. Create a virtual environment and install requirements:

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

2. Create a `.env` file with these variables (example):

```
TOKEN_API=your_token_here
CRYPT_KEY=your_crypt_key_here
HOST_D4SIGN=https://sandbox.d4sign.com.br/api/v1
PORT=5000
# login and per-person history (leave empty to run without login)
DATABASE_URL=postgresql://user:password@host:5432/postgres
SECRET_KEY=a_long_random_string
# confirmation e-mails (without them, the link is printed in the server log)
BREVO_API_KEY=xkeysib-...
EMAIL_REMETENTE=sender@example.com
# optional
DOMINIO_PERMITIDO=sitawi.net
D4SIGN_DOWNLOAD_WORKERS=6
REDIS_URL=redis://localhost:6379/0
D4SIGN_AUTO_REFRESH_INTERVAL=3600
```

3. If you use a database, create the tables with [`supabase.sql`](supabase.sql) (in Supabase: *SQL Editor*). The file also has the commented commands to create a database user that can only access these tables.

4. Run locally:

```powershell
python d4sign.py
```

Then open `http://localhost:5000`.

## Deploy to Vercel

1. On vercel.com, *Add New → Project* and import this GitHub repository. Vercel detects Flask and loads `app` from `d4sign.py` (set in `pyproject.toml`); `vercel.json` runs the function in São Paulo (`gru1`) with up to 300 s per request.
2. In *Settings → Environment Variables*, set the same variables listed for Render below, except `PORT`. Notes for Vercel:
   - `SECRET_KEY` is required: without it each function instance signs sessions with a different key and people get logged out.
   - `DATABASE_URL`: use Supabase's *Transaction pooler* (port 6543), the one meant for serverless functions.
   - `APP_URL` is optional: without it the confirmation e-mails use the production domain Vercel provides.
3. Deploy. Each push to `main` deploys to production; other branches get preview URLs.

On Vercel there are no background threads: the Redis refresh worker only runs on Render or locally. Batch downloads are sent as a streamed response, because a regular Vercel Function response is limited to 4.5 MB.

## Deploy to Render

1. Fork or clone this repository into your GitHub account.

2. On Render.com create a new Web Service, connect your GitHub repo and set the following environment variables in Render's dashboard:

- `TOKEN_API` — your D4Sign token
- `CRYPT_KEY` — your D4Sign crypt key
- `HOST_D4SIGN` — optional (defaults to sandbox)
- `DATABASE_URL` — Supabase connection string (use the *Session pooler*, port 5432); turns on login and the per-person history
- `SECRET_KEY` — random string used to sign sessions and confirmation links
- `BREVO_API_KEY` and `EMAIL_REMETENTE` — Brevo API key and a sender verified in Brevo. Render's free plan blocks SMTP ports, so e-mails go through Brevo's HTTP API (free up to 300 e-mails/day). On paid plans, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USUARIO` and `SMTP_SENHA` also work
- `DOMINIO_PERMITIDO` — optional, e-mail domain allowed to sign up (default `sitawi.net`)
- `REDIS_URL` — optional, enables the Redis cache
- `PORT` — Render sets this automatically, but keep `5000` as fallback

3. Build command: `pip install -r requirements.txt`
   Start command: `python d4sign.py`

## Getting the code

```bash
git clone https://github.com/offryan/D4Sign_Downloader.git
cd D4Sign_Downloader
```

## Project structure

- `d4sign.py` — Flask app: D4Sign API calls, page template, filters and downloads
- `auth.py` — login, sign-up, e-mail confirmation (and the commented Microsoft login)
- `registro.py` — accounts and per-person download history (PostgreSQL or local file)
- `email_envio.py` — sends e-mails through Brevo or SMTP
- `supabase.sql` — database tables, row level security and permissions
- `docs/` — screenshots used in this README

## Notes
- This repository uses environment variables for secrets. Do not commit `TOKEN_API`, `CRYPT_KEY`, `DATABASE_URL`, `SECRET_KEY` or `BREVO_API_KEY` to Git.
- The list adjusts to the window height and scrolls inside the table when there are many results, keeping the header visible.

## 📄 License

© 2025-2026 Ryan G. Santos. All rights reserved. The code is public for portfolio and evaluation purposes only: copying, modifying, distributing or using it commercially without written permission is not allowed. See the [LICENSE](LICENSE) file (in Portuguese).
