"""
===============================================================
  1xBet Crash - Cloud Collector v3 (Firebase)
  Tourne 24/7 - Stocke dans Firebase Realtime DB
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
import urllib.request
import urllib.error
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

# Firebase
FIREBASE_URL = os.environ.get(
    "FIREBASE_URL",
    "https://xbet-8a511-default-rtdb.europe-west1.firebasedatabase.app"
)


# ========== FIREBASE ==========
class Firebase:
    """Firebase Realtime Database via REST API"""

    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")
        self.batch = []
        self.batch_size = 10  # Envoyer par lots de 10

    def push_round(self, round_id, multiplier, timestamp=0, cashouts=0):
        """Envoie un resultat a Firebase"""
        data = {
            "m": multiplier,
            "ts": timestamp,
            "co": cashouts,
            "at": datetime.utcnow().isoformat() + "Z"
        }
        self.batch.append((round_id, data))

        if len(self.batch) >= self.batch_size:
            self._flush()

    def _flush(self):
        """Envoie le batch a Firebase"""
        if not self.batch:
            return

        # Preparer les donnees en batch
        batch_data = {}
        for rid, data in self.batch:
            batch_data[str(rid)] = data

        try:
            url = f"{self.base_url}/rounds.json"
            req_data = json.dumps(batch_data).encode("utf-8")
            req = urllib.request.Request(url, data=req_data, method="PATCH")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    pass  # OK
        except Exception as e:
            print(f"[Firebase] Erreur batch: {e}", flush=True)

        self.batch.clear()

    def update_stats(self, stats):
        """Met a jour les stats dans Firebase"""
        try:
            url = f"{self.base_url}/stats.json"
            req_data = json.dumps(stats).encode("utf-8")
            req = urllib.request.Request(url, data=req_data, method="PUT")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=10) as resp:
                pass
        except Exception as e:
            print(f"[Firebase] Erreur stats: {e}", flush=True)

    def update_live(self, live_data):
        """Met a jour les donnees live"""
        try:
            url = f"{self.base_url}/live.json"
            req_data = json.dumps(live_data).encode("utf-8")
            req = urllib.request.Request(url, data=req_data, method="PUT")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=10) as resp:
                pass
        except Exception as e:
            print(f"[Firebase] Erreur live: {e}", flush=True)

    def force_flush(self):
        self._flush()


# ========== LOCAL DB ==========
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
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }

        # Distribution
        ranges = [(1.0,1.2),(1.2,1.5),(1.5,2.0),(2.0,3.0),(3.0,5.0),(5.0,10.0),(10.0,50.0),(50.0,9999)]
        labels = ["instant","tres_bas","bas","moyen","haut","eleve","rare","mega"]
        dist = {}
        for (lo,hi),label in zip(ranges, labels):
            c = sum(1 for m in mults if lo <= m < hi)
            dist[label] = {"count": c, "pct": round(c/n*100, 1)}
        result["distribution"] = dist

        # Survival
        survival = {}
        for t in [1.5, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0]:
            c = sum(1 for m in mults if m >= t)
            survival[f"gte_{t}x"] = round(c/n*100, 1)
        result["survival"] = survival

        # Correlations (>= 100)
        if n >= 100:
            corr = {}
            scenarios = [("lt_1_5x", lambda m: m < 1.5), ("lt_2x", lambda m: m < 2.0),
                        ("gt_5x", lambda m: m > 5.0), ("gt_10x", lambda m: m > 10.0)]
            for label, cond in scenarios:
                after = [mults[i] for i in range(1, n) if cond(mults[i-1])]
                if len(after) >= 5:
                    corr[f"after_{label}"] = {
                        "cases": len(after),
                        "mean": round(statistics.mean(after), 2),
                        "p_2x": round(sum(1 for m in after if m >= 2.0)/len(after)*100, 1),
                        "p_5x": round(sum(1 for m in after if m >= 5.0)/len(after)*100, 1),
                    }
            result["correlations"] = corr

        # Streaks (>= 200)
        if n >= 200:
            streaks = {}
            global_avg = statistics.mean(mults)
            for slen in [3, 4, 5, 6, 7, 8]:
                nexts = []
                for i in range(slen, n):
                    if all(mults[j] < 2.0 for j in range(i-slen, i)):
                        nexts.append(mults[i])
                if nexts and len(nexts) >= 3:
                    avg = statistics.mean(nexts)
                    diff = round(((avg/global_avg)-1)*100, 1)
                    streaks[f"after_{slen}_low"] = {
                        "cases": len(nexts),
                        "mean": round(avg, 2),
                        "vs_global": diff,
                        "p_2x": round(sum(1 for m in nexts if m >= 2.0)/len(nexts)*100, 1),
                        "interesting": diff > 20
                    }
            result["streaks"] = streaks

        # Autocorrelation (>= 200)
        if n >= 200:
            ac = {}
            for lag in range(1, 6):
                pairs = [(mults[i], mults[i-lag]) for i in range(lag, n)]
                mx = statistics.mean([p[0] for p in pairs])
                my = statistics.mean([p[1] for p in pairs])
                cov = sum((x-mx)*(y-my) for x,y in pairs)/len(pairs)
                sx = statistics.stdev([p[0] for p in pairs])
                sy = statistics.stdev([p[1] for p in pairs])
                r = round(cov/(sx*sy), 4) if sx*sy > 0 else 0
                ac[f"lag_{lag}"] = {"r": r, "signal": abs(r) > 0.1}
            result["autocorrelation"] = ac

        # Current state
        last_10 = self.db.get_last(10)
        low_streak = 0
        for m in reversed(last_10):
            if m < 2.0:
                low_streak += 1
            else:
                break

        result["current"] = {
            "last_10": [round(m, 2) for m in last_10],
            "low_streak": low_streak,
            "tip": "ATTENDRE" if low_streak < 3 else "2x+ possible" if low_streak < 5 else "BON MOMENT",
        }

        # Progress
        thresholds = {"basic": 100, "correlations": 500, "patterns": 1000, "predictions": 5000}
        progress = {}
        for name, t in thresholds.items():
            progress[name] = {"done": n >= t, "current": n, "target": t}
        result["progress"] = progress

        return result


# ========== HTTP API ==========
class Handler(BaseHTTPRequestHandler):
    db = None
    analyzer = None
    collector = None

    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path == "/":
            self._json(200, {"status": "running", "rounds": self.db.count() if self.db else 0,
                             "firebase": FIREBASE_URL.split("/")[-1].split(".")[0]})
        elif self.path == "/stats":
            self._json(200, self.analyzer.analyze() if self.analyzer else {})
        elif self.path == "/recent":
            self._json(200, {"rounds": self.db.get_recent_rounds(50)} if self.db else {})
        elif self.path == "/live":
            if self.collector:
                self._json(200, {
                    "total": self.collector.round_count,
                    "last_10": [round(m, 2) for m in self.collector.last_10],
                    "connected": self.collector.connected,
                    "uptime_h": round((datetime.utcnow() - self.collector.start_time).total_seconds() / 3600, 1)
                })
            else:
                self._json(503, {})
        else:
            self._json(404, {"error": "not found"})

    def _json(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode())


# ========== COLLECTOR ==========
class Collector:
    def __init__(self):
        self.db = DB()
        self.firebase = Firebase(FIREBASE_URL)
        self.analyzer = Analyzer(self.db)
        self.running = True
        self.connected = False
        self.last_10 = deque(maxlen=10)
        self.current_cashouts = []
        self.round_count = self.db.count()
        self.start_time = datetime.utcnow()

        if self.round_count > 0:
            self.last_10.extend(self.db.get_last(10))
            self.log(f"Reprise: {self.round_count} en base")

    def log(self, msg):
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

    async def run(self):
        # HTTP server
        srv = HTTPServer(("0.0.0.0", PORT), Handler)
        Handler.db = self.db
        Handler.analyzer = self.analyzer
        Handler.collector = self
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.log(f"API sur :{PORT} | Firebase: {FIREBASE_URL.split('/')[-1][:30]}")

        while self.running:
            try:
                self.log("Connexion WS...")
                headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://1xbet.com"}

                async with websockets.connect(
                    WS_URL, additional_headers=headers,
                    ping_interval=20, ping_timeout=15,
                    max_size=2**20, open_timeout=30,
                ) as ws:
                    self.connected = True
                    self.log("Connecte!")

                    await ws.send(json.dumps({"protocol": "json", "version": 1}) + "\x1e")
                    await ws.recv()
                    await ws.send(json.dumps({
                        "arguments": [{"activity": 30, "currency": 27}],
                        "invocationId": "0", "target": "Guest", "type": 1
                    }) + "\x1e")
                    self.log("Guest OK")

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            self._process(msg)
                        except asyncio.TimeoutError:
                            await ws.send(json.dumps({"type": 6}) + "\x1e")
                        except websockets.exceptions.ConnectionClosed:
                            break

                self.connected = False
            except Exception as e:
                self.connected = False
                self.log(f"Erreur: {e}")

            if self.running:
                self.firebase.force_flush()
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
                        self.firebase.push_round(rid, m)
                self.firebase.force_flush()
                self.log(f"Historique OK. Total: {self.round_count}")

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

                    # Firebase
                    self.firebase.push_round(rid, mult, ts, len(self.current_cashouts))

                    recent = " ".join(f"{m:.1f}" for m in list(self.last_10)[-8:])
                    self.log(f"#{rid} {mult:.2f}x [{recent}] #{self.round_count}")

                    # Update Firebase live
                    if self.round_count % 5 == 0:
                        low_streak = 0
                        for m in reversed(list(self.last_10)):
                            if m < 2.0:
                                low_streak += 1
                            else:
                                break

                        self.firebase.update_live({
                            "total": self.round_count,
                            "last_10": [round(m, 2) for m in self.last_10],
                            "streak": low_streak,
                            "tip": "ATTENDRE" if low_streak < 3 else "2x+" if low_streak < 5 else "GO",
                            "updated": datetime.utcnow().isoformat() + "Z"
                        })

                    # Update Firebase stats periodiquement
                    if self.round_count in [50, 100, 200, 500, 1000, 2000, 5000, 10000] or self.round_count % 100 == 0:
                        stats = self.analyzer.analyze()
                        self.firebase.update_stats(stats)
                        self.log(f"=== Stats Firebase mises a jour ({self.round_count}) ===")


async def main():
    print("""
    ====================================================
      1xBet CRASH Collector v3 (Firebase)
      24/7 Cloud + Firebase Realtime DB
    ====================================================
    """, flush=True)

    collector = Collector()

    def on_exit(sig, frame):
        collector.running = False
        collector.firebase.force_flush()
        collector.log("Arret.")
        sys.exit(0)

    signal.signal(signal.SIGINT, on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    print(f"  Firebase: {FIREBASE_URL}", flush=True)
    print(f"  DB local: {DB_FILE}", flush=True)
    print(f"  En base:  {collector.round_count} resultats\n", flush=True)

    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
