#!/usr/bin/env python3
"""
Simple HTTP server for the desktop widget.
Serves backfill dashboard and proxies Heroku API requests.
"""

import http.server
import json
import socketserver
import ssl
import sys
import time
import urllib.request

sys.path.insert(0, ".")
from sqlalchemy import create_engine, text
from src.core.config import settings

PORT = 8765
API_URL = "https://gmail-obsidian-sync-729716d2143d.herokuapp.com/dashboard/stats"

# Shared DB engine (created once at startup)
_engine = create_engine(settings.database_url, pool_pre_ping=True, pool_size=1, max_overflow=0)

# Rate tracking state
_prev_done: int | None = None
_prev_time: float | None = None
_prev_account_done: dict[str, int] = {}
_last_rates: dict[str, int] = {}       # cached per-account rates
_last_total_rate: int = 0               # cached total rate
_last_total_eta: float | None = None
_last_account_etas: dict[str, float | None] = {}


def _get_backfill_stats():
    """Query Supabase directly for backfill progress."""
    global _prev_done, _prev_time, _prev_account_done
    global _last_rates, _last_total_rate, _last_total_eta, _last_account_etas

    with _engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                ga.account_email,
                COUNT(*) FILTER (WHERE e.body IS NOT NULL AND e.body <> '__fetching__') as done,
                COUNT(*) FILTER (WHERE e.body = '__fetching__') as fetching,
                COUNT(*) FILTER (WHERE e.body IS NULL) as remaining,
                COUNT(*) as total
            FROM emails e
            JOIN gmail_accounts ga ON ga.id = e.account_id
            GROUP BY ga.account_email
            ORDER BY ga.account_email
        """)).fetchall()

    now = time.time()
    elapsed = (now - _prev_time) if _prev_time is not None else 0
    can_calc_rate = _prev_time is not None and elapsed >= 5  # need at least 5s between samples

    accounts = []
    total_done = 0
    total_fetching = 0
    total_remaining = 0
    total_all = 0

    for r in rows:
        email, done, fetching, remaining, total = r[0], r[1], r[2], r[3], r[4]
        pct = (done / total * 100) if total > 0 else 0

        # Per-account rate (only recalculate if enough time elapsed)
        if can_calc_rate and email in _prev_account_done:
            delta = done - _prev_account_done[email]
            acct_rate = max(0, round(delta / elapsed * 60))
            _last_rates[email] = acct_rate
            _last_account_etas[email] = round(remaining / acct_rate / 60, 1) if acct_rate > 0 else None

        acct_rate = _last_rates.get(email, 0)
        acct_eta = _last_account_etas.get(email)

        accounts.append({
            "email": email,
            "done": done,
            "fetching": fetching,
            "remaining": remaining,
            "total": total,
            "pct": round(pct, 1),
            "rate_per_min": acct_rate,
            "eta_hours": acct_eta,
        })
        total_done += done
        total_fetching += fetching
        total_remaining += remaining
        total_all += total

    # Update tracking state (only when we recalculated rates)
    if can_calc_rate:
        if _prev_done is not None:
            delta = total_done - _prev_done
            _last_total_rate = max(0, round(delta / elapsed * 60))
            _last_total_eta = round(total_remaining / _last_total_rate / 60, 1) if _last_total_rate > 0 else None

        # Update prev snapshots for next interval
        _prev_done = total_done
        _prev_time = now
        for a in accounts:
            _prev_account_done[a["email"]] = a["done"]
    elif _prev_time is None:
        # First call — just record baseline, no rates yet
        _prev_done = total_done
        _prev_time = now
        for a in accounts:
            _prev_account_done[a["email"]] = a["done"]

    return {
        "accounts": accounts,
        "total_done": total_done,
        "total_fetching": total_fetching,
        "total_remaining": total_remaining,
        "total": total_all,
        "pct": round(total_done / total_all * 100, 1) if total_all > 0 else 0,
        "rate_per_min": _last_total_rate,
        "eta_hours": _last_total_eta,
    }


class WidgetHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/api/stats':
            self._proxy_heroku()
        elif self.path == '/api/backfill':
            self._backfill_stats()
        elif self.path in ('/', '/widget'):
            self.path = '/desktop_widget_local.html'
            return super().do_GET()
        else:
            return super().do_GET()

    def _proxy_heroku(self):
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            response = urllib.request.urlopen(API_URL, timeout=5, context=ctx)
            data = response.read()
            self._json_response(200, data)
        except Exception as e:
            self._json_response(500, json.dumps({"error": str(e)}).encode())

    def _backfill_stats(self):
        try:
            stats = _get_backfill_stats()
            self._json_response(200, json.dumps(stats).encode())
        except Exception as e:
            self._json_response(500, json.dumps({"error": str(e)}).encode())

    def _json_response(self, code, data):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(data if isinstance(data, bytes) else data.encode())

    def log_message(self, format, *args):
        pass


def main():
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), WidgetHandler) as httpd:
        print(f"Widget Server Running on http://localhost:{PORT}/widget")
        print(f"Auto-refreshes every 10 seconds")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nWidget server stopped")


if __name__ == "__main__":
    main()
