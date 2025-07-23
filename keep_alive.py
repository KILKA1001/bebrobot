from flask import Flask, send_from_directory
from threading import Thread
import os

app = Flask(__name__)

@app.route("/")
def home():
    return send_from_directory('.', 'index.html')

def run():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

def keep_alive():
    t = Thread(target=run)
    t.start()
