from __future__ import annotations

import os
from threading import Thread

from flask import Flask

app = Flask(__name__)


@app.route("/")
def home():
    return "Bot is running"


def run():
    from bot.admin_api import register_admin_routes

    register_admin_routes(app)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


def keep_alive():
    Thread(target=run, daemon=True).start()
