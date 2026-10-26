# D4Sign Minimal App

This is a minimal Flask app to list and download signed documents from D4Sign.

![D4Sign Downloader](docs/preview.png)

## Features

- Lists finished (signed) documents from every safe ("cofre") in your D4Sign account
- Filters by safe, document name and signature period
- Downloads one document or a batch of selected documents as a single `.zip`
- Tracks which documents were already downloaded
- Optional Redis cache for signature dates, plus a background refresh worker
- Webhook endpoint (`POST /webhook/d4sign`) to receive signature updates from D4Sign
- Light and dark mode

**Stack:** Python, Flask, Requests, D4Sign REST API, Redis (optional). Deployable on Render.

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
# optional
REDIS_URL=redis://localhost:6379/0
D4SIGN_AUTO_REFRESH_INTERVAL=3600
```

3. Run locally:

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
- `REDIS_URL` — optional, enables the Redis cache
- `PORT` — Render sets this automatically, but keep `5000` as fallback

3. Build command: `pip install -r requirements.txt`
   Start command: `python d4sign.py`

## Getting the code

```bash
git clone https://github.com/offryan/D4Sign_Downloader.git
cd D4Sign_Downloader
```

## Notes
- This repository uses environment variables for secrets. Do not commit `TOKEN_API` or `CRYPT_KEY` to Git.
- I added a few UI improvements (smooth scrolling when many results) and dark-mode scrollbar fixes.

## 📄 License

© 2025-2026 Ryan G. Santos. All rights reserved. The code is public for portfolio and evaluation purposes only: copying, modifying, distributing or using it commercially without written permission is not allowed. See the [LICENSE](LICENSE) file (in Portuguese).
