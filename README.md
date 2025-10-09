Run the Flask server (serves the existing `index.html` and static assets)

This repository separates server logic and UI. The server is implemented in
`scripts/script.py` and is configured to serve `index.html` and the
`scripts/` and `style/` directories from the project root so you don't need to
move files.

How to run (Windows PowerShell):

# Create a virtualenv and install dependencies
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install flask requests

# Run the app
python scripts\script.py

Open http://127.0.0.1:5000/ in your browser.

Notes:
- `index.html` uses Jinja variables (e.g. `ICON_UP`, `ICON_DOWN`) provided by
  the `index()` view in `scripts/script.py`. The Flask app is configured with
  `template_folder='.'` so the root `index.html` is rendered as a template.
- Static assets (JS/CSS) are served directly from the project directory since
  `static_folder='.'` and `static_url_path=''` were used. Requests for
  `/scripts/script.js` and `/style/style.css` will be served automatically.
- If you already have a different Flask app (e.g. `d4sign_app.py`), you can
  either import functions from `scripts/script.py` or point your webserver to
  the `index.html` and static folders similarly.
