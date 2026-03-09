#!/usr/bin/env python3
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from bot.data import db


if __name__ == "__main__":
    print(json.dumps(db.get_account_metrics_report(), ensure_ascii=False, indent=2, sort_keys=True))
