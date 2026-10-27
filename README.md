# D4Sign Downloader

Flask app to list and download signed documents from D4Sign, with team login and a per-person record of what was already downloaded.

![D4Sign Downloader](docs/preview.png)

## Features

- Lists finished (signed) documents from every safe ("cofre") in your D4Sign account
- Filters by safe, document name, signature period and status (all, downloaded, not downloaded)
- Downloads one document or a batch of selected documents as a single `.zip`. Batches are downloaded in parallel (6 at a time by default) with automatic retries, and any document that fails is reported
- Login with e-mail and password, restricted to one e-mail domain (`@sitawi.net` by default). New accounts are activated through a confirmation link sent by e-mail
- Each person has their own download history, saved in PostgreSQL (Supabase): downloaded documents get a "Baixado" tag, and the × on the tag removes it for that document
- Microsoft (Entra ID) login is ready in `auth.py`, commented out until it is enabled
- Optional Redis cache for signature dates, plus a background refresh worker
- Webhook endpoint (`POST /webhook/d4sign`) to receive signature updates from D4Sign
- Light and dark mode, with a card layout on phones

**Stack:** Python, Flask, Requests, D4Sign REST API, PostgreSQL / Supabase (psycopg), Brevo (transactional e-mail), Redis (optional). Deployed on Render.

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

## Notes
- This repository uses environment variables for secrets. Do not commit `TOKEN_API`, `CRYPT_KEY`, `DATABASE_URL`, `SECRET_KEY` or `BREVO_API_KEY` to Git.
- The list adjusts to the window height and scrolls inside the table when there are many results, with dark-mode scrollbar fixes.

## 📄 License

© 2025-2026 Ryan G. Santos. All rights reserved. The code is public for portfolio and evaluation purposes only: copying, modifying, distributing or using it commercially without written permission is not allowed. See the [LICENSE](LICENSE) file (in Portuguese).
