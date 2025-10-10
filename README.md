# D4Sign Minimal App

This is a minimal Flask app to list and download signed documents from D4Sign.

## Setup (local)

1. Create a virtual environment and install requirements:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Create a `.env` file with these variables (example):

```
TOKEN_API=your_token_here
CRYPT_KEY=your_crypt_key_here
HOST_D4SIGN=https://sandbox.d4sign.com.br/api/v1
PORT=5000
```

3. Run locally:

```powershell
python d4sign.py
```

## Deploy to Render

1. Create a new GitHub repository and push this project.

2. On Render.com create a new Web Service, connect your GitHub repo and set the following environment variables in Render's dashboard:

- `TOKEN_API` — your D4Sign token
- `CRYPT_KEY` — your D4Sign crypt key
- `HOST_D4SIGN` — optional (defaults to sandbox)
- `PORT` — Render sets this automatically, but keep `5000` as fallback

3. Build command: `pip install -r requirements.txt`
   Start command: `python d4sign.py`

## Creating the GitHub repo and pushing (local commands)

```powershell
# from repository root
git init
git add .
git commit -m "Initial commit: d4sign app"
# replace <your-repo-url> with the repo you create on GitHub
git remote add origin <your-repo-url>
git branch -M main
git push -u origin main
```

## Notes
- This repository uses environment variables for secrets. Do not commit `TOKEN_API` or `CRYPT_KEY` to Git.
- I added a few UI improvements (smooth scrolling when many results) and dark-mode scrollbar fixes.
