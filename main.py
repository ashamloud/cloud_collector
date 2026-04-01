"""
===============================================================
  1xBet Crash - Cloud Collector (Render.com / Railway)
  Tourne 24/7 sur serveur gratuit
  Stocke dans SQLite + exporte en JSON
===============================================================
"""

import json
import os
import asyncio
import websockets
import sqlite3
import statistics
import signal
import sys
import math
from datetime import datetime
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# Config
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_FILE = os.path.join(DATA_DIR, "crash.db")
WS_URL = "wss://1xbet.com/games-frame/sockets/crash?ref=1&gr=285&whence=55&fcountry=165&appGuid=games-web-host-b2c-web-v3&lng=fr&v=1.5"
PORT = int(os.environ.get("PORT", 10000))


# ========== DATABASE ==========
class DB:
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS rounds (
                round_id INTEGER PRIMARY KEY,
                multiplier REAL NOT NULL,
                timestamp INTEGER DEFAULT 0,
                collected_at TEXT,
                cashout_count INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_mult ON rounds(multiplier);
        """)
        self.conn.commit()

    def insert(self, rid, mult, ts=0, cashouts=0):
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO rounds VALUES (?,?,?,?,?)",
                (rid, mult, ts, datetime.utcnow().isoformat(), cashouts)
            )
            self.conn.commit()
            return True
        except:
            return False

    def count(self):
        return self.conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0]

    def get_all(self):
        rows = self.conn.execute("SELECT multiplier FROM rounds ORDER BY round_id ASC").fetchall()
        return [r[0] for r in rows]

    def get_last(self, n):
        rows = self.conn.execute("SELECT multiplier FROM rounds ORDER BY round_id DESC LIMIT ?", (n,)).fetchall()
        return [r[0] for r in reversed(rows)]

    def get_recent_rounds(self, n=50):
        rows = self.conn.execute(
            "SELECT round_id, multiplier, timestamp, collected_at FROM rounds ORDER BY round_id DESC LIMIT ?", (n,)
        ).fetchall()
        return [{"round_id": r[0], "multiplier": r[1], "timestamp": r[2], "collected_at": r[3]} for r in reversed(rows)]


# ========== ANALYZER ==========
class Analyzer:
    def __init__(self, db):
        self.db = db

    def analyze(self):
        mults = self.db.get_all()
        n = len(mults)
        if n < 20:
            return {"status": "collecting", "count": n, "needed": 100}

        result = {
            "count": n,
            "mean": round(statistics.mean(mults), 2),
            "median": round(statistics.median(mults), 2),
            "min": round(min(mults), 2),
            "max": round(max(mults), 2),
            "stdev": round(statistics.stdev(mults), 2) if n > 1 else 0,
        }

        # Distribution
        ranges = [(1.0,1.2),(1.2,1.5),(1.5,2.0),(2.0,3.0),(3.0,5.0),(5.0,10.0),(10.0,50.0),(50.0,9999)]
        labels = ["instant","tres_bas","bas","moyen","haut","eleve","rare","mega"]
        dist = {}
        for (lo,hi),label in zip(ranges, labels):
            c = sum(1 for m in mults if lo <= m < hi)
            dist[label] = {"count": c, "pct": round(c/n*100, 1)}
        result["distribution"] = dist

        # Survival probabilities
        survival = {}
        for t in [1.5, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0]:
            c = sum(1 for m in mults if m >= t)
            survival[f">={t}x"] = round(c/n*100, 1)
        result["survival"] = survival

        # Sequential correlation
        if n >= 100:
            correlations = {}
            scenarios = [("<1.5x", lambda m: m < 1.5), ("<2x", lambda m: m < 2.0),
                        (">5x", lambda m: m > 5.0), (">10x", lambda m: m > 10.0)]
            for label, cond in scenarios:
                after = [mults[i] for i in range(1, n) if cond(mults[i-1])]
                if len(after) >= 5:
                    correlations[f"after_{label}"] = {
                        "count": len(after),
                        "mean": round(statistics.mean(after), 2),
                        "p_above_2x": round(sum(1 for m in after if m >= 2.0)/len(after)*100, 1),
                        "p_above_5x": round(sum(1 for m in after if m >= 5.0)/len(after)*100, 1),
                    }
            result["correlations"] = correlations

        # Streaks
        if n >= 100:
            streaks = {}
            for slen in [3, 4, 5, 6, 7, 8]:
                nexts = []
                for i in range(slen, n):
                    if all(mults[j] < 2.0 for j in range(i-slen, i)):
                        nexts.append(mults[i])
                if nexts and len(nexts) >= 3:
                    avg = statistics.mean(nexts)
                    global_avg = statistics.mean(mults)
                    diff = round(((avg/global_avg)-1)*100, 1)
                    streaks[f"after_{slen}_low"] = {
                        "count": len(nexts),
                        "mean": round(avg, 2),
                        "vs_global_pct": diff,
                        "p_above_2x": round(sum(1 for m in nexts if m >= 2.0)/len(nexts)*100, 1),
                        "interesting": diff > 20
                    }
            result["streaks"] = streaks

        # Autocorrelation
        if n >= 200:
            autocorr = {}
            for lag in range(1, 6):
                pairs = [(mults[i], mults[i-lag]) for i in range(lag, n)]
                mx = statistics.mean([p[0] for p in pairs])
                my = statistics.mean([p[1] for p in pairs])
                cov = sum((x-mx)*(y-my) for x,y in pairs)/len(pairs)
                sx = statistics.stdev([p[0] for p in pairs])
                sy = statistics.stdev([p[1] for p in pairs])
                r = cov/(sx*sy) if sx*sy > 0 else 0
                autocorr[f"lag_{lag}"] = {"r": round(r, 4), "significant": abs(r) > 0.1}
            result["autocorrelation"] = autocorr

        # Current streak
        last_10 = self.db.get_last(10)
        low_streak = 0
        for m in reversed(last_10):
            if m < 2.0:
                low_streak += 1
            else:
                break
        result["current_low_streak"] = low_streak
        result["last_10"] = [round(m, 2) for m in last_10]

        # Prediction
        result["prediction"] = {
            "tip": "ATTENDRE" if low_streak < 3 else "2x+ possible" if low_streak < 5 else "BON MOMENT pour 2x+",
            "streak": low_streak,
        }

        # Progress
        thresholds = {"basic": 100, "correlations": 500, "patterns": 1000, "predictions": 5000}
        progress = {}
        for name, t in thresholds.items():
            progress[name] = {"done": n >= t, "current": n, "target": t}
        result["progress"] = progress

        return result


# ========== HEALTH SERVER (Render needs a port) ==========
class HealthHandler(BaseHTTPRequestHandler):
    db = None
    analyzer = None
    collector = None

    def log_message(self, format, *args):
        pass  # Silence logs

    def do_GET(self):
        if self.path == "/":
            self._respond(200, {"status": "running", "rounds": self.db.count() if self.db else 0})
        elif self.path == "/stats":
            if self.analyzer:
                self._respond(200, self.analyzer.analyze())
            else:
                self._respond(503, {"error": "not ready"})
        elif self.path == "/recent":
            if self.db:
                self._respond(200, {"rounds": self.db.get_recent_rounds(50)})
            else:
                self._respond(503, {"error": "not ready"})
        elif self.path == "/live":
            if self.collector:
                self._respond(200, {
                    "total": self.collector.round_count,
                    "last_10": [round(m, 2) for m in self.collector.last_10],
                    "connected": self.collector.connected,
                    "uptime_min": int((datetime.utcnow() - self.collector.start_time).total_seconds() / 60)
                })
            else:
                self._respond(503, {"error": "not ready"})
        else:
            self._respond(404, {"error": "not found"})

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode())


# ========== COLLECTOR ==========
class CloudCollector:
    def __init__(self):
        self.db = DB()
        self.analyzer = Analyzer(self.db)
        self.running = True
        self.connected = False
        self.last_10 = deque(maxlen=10)
        self.current_cashouts = []
        self.round_count = self.db.count()
        self.start_time = datetime.utcnow()

        if self.round_count > 0:
            self.last_10.extend(self.db.get_last(10))
            self.log(f"Reprise: {self.round_count} resultats en base")

    def log(self, msg):
        ts = datetime.utcnow().strftime("%H:%M:%S")
        print(f"[{ts}] {msg}", flush=True)

    async def run(self):
        # Demarrer le serveur HTTP dans un thread
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        HealthHandler.db = self.db
        HealthHandler.analyzer = self.analyzer
        HealthHandler.collector = self
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.log(f"API demarree sur port {PORT}")
        self.log(f"  GET /       -> status")
        self.log(f"  GET /stats  -> analyse complete")
        self.log(f"  GET /recent -> 50 derniers resultats")
        self.log(f"  GET /live   -> donnees en direct")

        # Boucle de collecte
        while self.running:
            try:
                self.log("Connexion WebSocket...")
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Origin": "https://1xbet.com",
                }

                async with websockets.connect(
                    WS_URL, additional_headers=headers,
                    ping_interval=20, ping_timeout=15,
                    max_size=2**20, open_timeout=30, close_timeout=5,
                ) as ws:
                    self.connected = True
                    self.log("Connecte!")

                    await ws.send(json.dumps({"protocol": "json", "version": 1}) + "\x1e")
                    await ws.recv()
                    await ws.send(json.dumps({
                        "arguments": [{"activity": 30, "currency": 27}],
                        "invocationId": "0", "target": "Guest", "type": 1
                    }) + "\x1e")
                    self.log("Mode Guest OK")

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            self._process(msg)
                        except asyncio.TimeoutError:
                            await ws.send(json.dumps({"type": 6}) + "\x1e")
                        except websockets.exceptions.ConnectionClosed:
                            self.log("Deconnecte")
                            break

                self.connected = False
            except Exception as e:
                self.connected = False
                self.log(f"Erreur: {e}")

            if self.running:
                self.log("Reconnexion 5s...")
                await asyncio.sleep(5)

    def _process(self, raw):
        for part in raw.split("\x1e"):
            if not part.strip():
                continue
            try:
                msg = json.loads(part)
            except:
                continue

            target = msg.get("target", "")
            args = msg.get("arguments", [{}])
            arg = args[0] if args else {}

            if target == "OnRegistration":
                for h in reversed(arg.get("fs", [])):
                    m, rid = h.get("f", 0), h.get("l", 0)
                    if m > 0 and self.db.insert(rid, m):
                        self.last_10.append(m)
                        self.round_count += 1
                self.log(f"Historique charge. Total: {self.round_count}")

            elif target == "OnStart":
                self.current_cashouts = []

            elif target == "OnCashouts":
                self.current_cashouts.extend(arg.get("q", []))

            elif target == "OnCrash":
                mult = arg.get("f", 0)
                rid = arg.get("l", 0)
                ts = arg.get("ts", 0)

                if mult > 0 and self.db.insert(rid, mult, ts, len(self.current_cashouts)):
                    self.round_count += 1
                    self.last_10.append(mult)

                    recent = " ".join(f"{m:.1f}" for m in list(self.last_10)[-8:])
                    self.log(f"#{rid} = {mult:.2f}x [{recent}] total:{self.round_count}")

                    # Analyse auto
                    if self.round_count in [50, 100, 200, 500, 1000, 2000, 5000, 10000]:
                        analysis = self.analyzer.analyze()
                        self.log(f"=== ANALYSE A {self.round_count} ===")
                        if "correlations" in analysis:
                            for k, v in analysis["correlations"].items():
                                self.log(f"  {k}: moy={v['mean']}x, >=2x:{v['p_above_2x']}%")
                        if "streaks" in analysis:
                            for k, v in analysis["streaks"].items():
                                flag = " ** FAILLE? **" if v.get("interesting") else ""
                                self.log(f"  {k}: moy={v['mean']}x ({v['vs_global_pct']:+.0f}%){flag}")


async def main():
    print("""
    ====================================================
      1xBet CRASH Cloud Collector
      Tourne 24/7 - API accessible en HTTP
    ====================================================
    """, flush=True)

    collector = CloudCollector()

    def on_exit(sig, frame):
        collector.running = False
        collector.log("Arret...")
        sys.exit(0)

    signal.signal(signal.SIGINT, on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
