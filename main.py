"""
===============================================================
  1xBet Crash - Smart Collector v5 (Firebase)
  Une salle configurable + auto-detection
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
import urllib.request
from datetime import datetime
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, "crash.db")
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
PORT = int(os.environ.get("PORT", 10000))
FIREBASE_URL = os.environ.get(
    "FIREBASE_URL",
    "https://xbet-8a511-default-rtdb.europe-west1.firebasedatabase.app"
)
WS_BASE = "wss://1xbet.com/games-frame/sockets/crash?ref=1&gr={}&whence=55&fcountry=165&appGuid=games-web-host-b2c-web-v3&lng=fr&v=1.5"
DEFAULT_ROOM = 3


def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            return json.load(f)
    return {"room": DEFAULT_ROOM}


def save_config(cfg):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f)


# ========== FIREBASE ==========
class Firebase:
    def __init__(self, url):
        self.url = url.rstrip("/")

    def _send(self, path, data, method="PUT"):
        try:
            req = urllib.request.Request(
                f"{self.url}/{path}.json",
                data=json.dumps(data).encode("utf-8"),
                method=method
            )
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=10):
                pass
        except Exception as e:
            print(f"[FB] {path}: {e}", flush=True)

    def update_live(self, data):
        self._send("live", data)

    def update_stats(self, data):
        self._send("stats", data)

    def push_rounds(self, data):
        self._send("rounds", data, "PATCH")


# ========== DB ==========
class DB:
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rounds (
                round_id INTEGER PRIMARY KEY,
                multiplier REAL NOT NULL,
                timestamp INTEGER DEFAULT 0,
                collected_at TEXT,
                cashout_count INTEGER DEFAULT 0
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_mult ON rounds(multiplier)")
        self.conn.commit()

    def insert(self, rid, mult, ts=0, cashouts=0):
        try:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO rounds (round_id, multiplier, timestamp, collected_at, cashout_count) VALUES (?,?,?,?,?)",
                (rid, mult, ts, datetime.utcnow().isoformat(), cashouts)
            )
            self.conn.commit()
            return cur.rowcount > 0
        except Exception as e:
            return False

    def count(self):
        return self.conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0]

    def get_all(self):
        return [r[0] for r in self.conn.execute("SELECT multiplier FROM rounds ORDER BY round_id ASC").fetchall()]

    def get_last(self, n):
        rows = self.conn.execute("SELECT multiplier FROM rounds ORDER BY round_id DESC LIMIT ?", (n,)).fetchall()
        return [r[0] for r in reversed(rows)]


# ========== ANALYZER ==========
class Analyzer:
    def __init__(self, db):
        self.db = db

    def analyze(self):
        mults = self.db.get_all()
        n = len(mults)
        if n < 20:
            return {"status": "collecting", "count": n}

        result = {
            "count": n, "mean": round(statistics.mean(mults), 2),
            "median": round(statistics.median(mults), 2),
            "min": round(min(mults), 2), "max": round(max(mults), 2),
            "stdev": round(statistics.stdev(mults), 2) if n > 1 else 0,
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }

        ranges = [(1.0,1.2),(1.2,1.5),(1.5,2.0),(2.0,3.0),(3.0,5.0),(5.0,10.0),(10.0,50.0),(50.0,9999)]
        labels = ["instant","tres_bas","bas","moyen","haut","eleve","rare","mega"]
        dist = {}
        for (lo,hi),label in zip(ranges, labels):
            c = sum(1 for m in mults if lo <= m < hi)
            dist[label] = {"count": c, "pct": round(c/n*100, 1)}
        result["distribution"] = dist

        survival = {}
        for t in [1.5, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0]:
            survival[f"gte_{t}x"] = round(sum(1 for m in mults if m >= t)/n*100, 1)
        result["survival"] = survival

        if n >= 100:
            corr = {}
            for label, cond in [("lt_1_5x", lambda m: m<1.5), ("lt_2x", lambda m: m<2.0),
                                ("gt_5x", lambda m: m>5.0), ("gt_10x", lambda m: m>10.0)]:
                after = [mults[i] for i in range(1, n) if cond(mults[i-1])]
                if len(after) >= 5:
                    corr[f"after_{label}"] = {
                        "cases": len(after), "mean": round(statistics.mean(after), 2),
                        "p_2x": round(sum(1 for m in after if m>=2.0)/len(after)*100, 1),
                        "p_5x": round(sum(1 for m in after if m>=5.0)/len(after)*100, 1),
                    }
            result["correlations"] = corr

        if n >= 200:
            streaks = {}
            ga = statistics.mean(mults)
            for slen in [3, 4, 5, 6, 7, 8]:
                nexts = [mults[i] for i in range(slen, n) if all(mults[j]<2.0 for j in range(i-slen, i))]
                if len(nexts) >= 3:
                    avg = statistics.mean(nexts)
                    streaks[f"after_{slen}_low"] = {
                        "cases": len(nexts), "mean": round(avg, 2),
                        "vs_global": round(((avg/ga)-1)*100, 1),
                        "p_2x": round(sum(1 for m in nexts if m>=2.0)/len(nexts)*100, 1),
                        "interesting": ((avg/ga)-1)*100 > 20
                    }
            result["streaks"] = streaks

        if n >= 200:
            ac = {}
            for lag in range(1, 6):
                pairs = [(mults[i], mults[i-lag]) for i in range(lag, n)]
                mx, my = statistics.mean([p[0] for p in pairs]), statistics.mean([p[1] for p in pairs])
                cov = sum((x-mx)*(y-my) for x,y in pairs)/len(pairs)
                sx, sy = statistics.stdev([p[0] for p in pairs]), statistics.stdev([p[1] for p in pairs])
                r = round(cov/(sx*sy), 4) if sx*sy > 0 else 0
                ac[f"lag_{lag}"] = {"r": r, "signal": abs(r) > 0.1}
            result["autocorrelation"] = ac

        thresholds = {"basic": 100, "correlations": 500, "patterns": 1000, "predictions": 5000}
        result["progress"] = {n: {"done": len(mults)>=t, "current": len(mults), "target": t} for n,t in thresholds.items()}
        return result


# ========== ROOM SCANNER ==========
async def scan_for_room(target_results):
    """Scanne les salles pour trouver celle qui correspond aux resultats donnes"""
    test_ids = list(range(1, 50)) + list(range(100, 120)) + list(range(200, 310))

    async def check_room(gr):
        try:
            url = WS_BASE.format(gr)
            headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://1xbet.com"}
            async with websockets.connect(url, additional_headers=headers, open_timeout=10) as ws:
                await ws.send(json.dumps({"protocol": "json", "version": 1}) + "\x1e")
                await ws.recv()
                await ws.send(json.dumps({
                    "arguments": [{"activity": 30, "currency": 27}],
                    "invocationId": "0", "target": "Guest", "type": 1
                }) + "\x1e")
                for _ in range(10):
                    msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    for part in msg.split("\x1e"):
                        if not part.strip():
                            continue
                        try:
                            data = json.loads(part)
                            if data.get("target") == "OnRegistration":
                                history = [h.get("f", 0) for h in data["arguments"][0].get("fs", [])][:5]
                                # Comparer avec les resultats cibles
                                for i in range(len(history) - len(target_results) + 1):
                                    if history[i:i+len(target_results)] == target_results:
                                        return gr, history
                                return None, None
                        except:
                            pass
        except:
            pass
        return None, None

    # Tester par lots
    for i in range(0, len(test_ids), 10):
        batch = test_ids[i:i+10]
        tasks = [check_room(gr) for gr in batch]
        results = await asyncio.gather(*tasks)
        for gr, hist in results:
            if gr is not None:
                return gr, hist
    return None, None


# ========== HTTP ==========
class Handler(BaseHTTPRequestHandler):
    collector = None
    dashboard_html = None

    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/dashboard"):
            self._html()
        elif self.path == "/api":
            c = self.collector
            self._json(200, {"status": "running", "room": c.room_id, "total": c.round_count} if c else {})
        elif self.path == "/stats":
            self._json(200, self.collector.analyzer.analyze() if self.collector else {})
        elif self.path == "/live":
            self._json(200, self.collector.get_live() if self.collector else {})
        elif self.path.startswith("/find?"):
            # /find?r=1.5,2.3,1.1
            qs = self.path.split("?", 1)[1] if "?" in self.path else ""
            r_str = ""
            for p in qs.split("&"):
                if p.startswith("r="):
                    r_str = p[2:]
            if r_str:
                targets = [float(x) for x in r_str.split(",")]
                # Run scan in background
                loop = asyncio.new_event_loop()
                gr, hist = loop.run_until_complete(scan_for_room(targets))
                loop.close()
                if gr:
                    # Save and switch
                    self.collector.switch_room(gr)
                    self._json(200, {"found": True, "room": gr, "history": hist})
                else:
                    self._json(200, {"found": False})
            else:
                self._json(400, {"error": "usage: /find?r=1.5,2.3,1.1"})
        elif self.path.startswith("/setroom?"):
            qs = self.path.split("?", 1)[1]
            for p in qs.split("&"):
                if p.startswith("gr="):
                    gr = int(p[3:])
                    self.collector.switch_room(gr)
                    self._json(200, {"room": gr, "status": "switching"})
                    return
            self._json(400, {"error": "usage: /setroom?gr=285"})
        else:
            self._json(404, {})

    def _html(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if not Handler.dashboard_html:
            p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
            if os.path.exists(p):
                with open(p, "rb") as f:
                    Handler.dashboard_html = f.read()
        self.wfile.write(Handler.dashboard_html or b"<h1>No dashboard</h1>")

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
        self.loop = asyncio.get_event_loop()
        self._switch_event = asyncio.Event()

        config = load_config()
        self.room_id = config.get("room", DEFAULT_ROOM)

        if self.round_count > 0:
            self.last_10.extend(self.db.get_last(10))
            self.log(f"Reprise: {self.round_count} en base | Salle: gr={self.room_id}")

    def log(self, msg):
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

    def switch_room(self, new_gr):
        self.log(f"Changement de salle: gr={self.room_id} -> gr={new_gr}")
        self.room_id = new_gr
        self.last_10.clear() # Clear specific room history
        save_config({"room": new_gr})
        self.loop.call_soon_threadsafe(self._switch_event.set)

    def get_live(self):
        last_100 = self.db.get_last(min(100, self.round_count))
        p2 = sum(1 for m in last_100 if m >= 2.0) / len(last_100) * 100 if last_100 else 50
        p5 = sum(1 for m in last_100 if m >= 5.0) / len(last_100) * 100 if last_100 else 20
        p10 = sum(1 for m in last_100 if m >= 10.0) / len(last_100) * 100 if last_100 else 10

        low_streak = 0
        for m in reversed(list(self.last_10)):
            if m < 2.0:
                low_streak += 1
            else:
                break

        if self.round_count < 100: conf = "FAIBLE"
        elif self.round_count < 1000: conf = "MOYENNE"
        elif self.round_count < 5000: conf = "BONNE"
        else: conf = "FORTE"

        if low_streak >= 5: tip, sig = "GO 2x+", 3
        elif low_streak >= 3: tip, sig = "PROBABLE 2x+", 2
        elif low_streak >= 2: tip, sig = "SURVEILLER", 1
        else: tip, sig = "ATTENDRE", 0

        return {
            "room": self.room_id,
            "total": self.round_count,
            "last_10": [round(m, 2) for m in self.last_10],
            "last_20": [round(m, 2) for m in self.db.get_last(20)],
            "streak": low_streak,
            "tip": tip, "signal": sig,
            "prediction": {
                "p2x": round(min(p2 + low_streak*3, 95), 1),
                "p5x": round(min(p5 + low_streak*1.8, 70), 1),
                "p10x": round(min(p10 + low_streak*0.9, 40), 1),
                "confidence": conf,
                "avg_100": round(statistics.mean(last_100), 2) if last_100 else 0,
            },
            "connected": self.connected,
            "updated": datetime.utcnow().isoformat() + "Z"
        }

    async def run(self):
        srv = HTTPServer(("0.0.0.0", PORT), Handler)
        Handler.collector = self
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.log(f"API sur :{PORT} | Dashboard: http://localhost:{PORT}/")

        while self.running:
            ws_url = WS_BASE.format(self.room_id)
            self._switch_event.clear()
            try:
                self.log(f"Connexion WS salle gr={self.room_id}...")
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Origin": "https://1xbet.com"}

                async with websockets.connect(
                    ws_url, additional_headers=headers,
                    ping_interval=20, ping_timeout=15,
                    max_size=2**20, open_timeout=30,
                ) as ws:
                    self.connected = True
                    self.log(f"Connecte a gr={self.room_id}!")

                    await ws.send(json.dumps({"protocol": "json", "version": 1}) + "\x1e")
                    await ws.recv()
                    await ws.send(json.dumps({
                        "arguments": [{"activity": 30, "currency": 27}],
                        "invocationId": "0", "target": "Guest", "type": 1
                    }) + "\x1e")
                    self.log("Guest OK")

                    async def ping_task():
                        while self.running and not self._switch_event.is_set() and self.connected:
                            await asyncio.sleep(10)
                            try:
                                await ws.send(json.dumps({"type": 6}) + "\x1e")
                            except:
                                break
                    
                    p_task = asyncio.create_task(ping_task())

                    while self.running and not self._switch_event.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=35)
                            self._process(msg)
                        except asyncio.TimeoutError:
                            self.log("Timeout serveur sans data, tentative de maintien...")
                        except websockets.exceptions.ConnectionClosed:
                            self.log("Deconnecte")
                            break
                    p_task.cancel()

                self.connected = False
                if self._switch_event.is_set():
                    self.log("Changement de salle detecte...")
            except Exception as e:
                self.connected = False
                self.log(f"Erreur: {e}")

            if self.running:
                self.log("Reconnexion 3s...")
                await asyncio.sleep(3)

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
                count = 0
                for h in reversed(arg.get("fs", [])):
                    m, rid = h.get("f", 0), h.get("l", 0)
                    if m > 0 and self.db.insert(rid, m):
                        self.last_10.append(m)
                        self.round_count += 1
                        count += 1
                self.log(f"Historique: +{count} (total:{self.round_count})")

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
                    self.log(f"#{rid} {mult:.2f}x [{recent}] #{self.round_count}")

                    # Firebase live a chaque resultat
                    live = self.get_live()
                    self.firebase.update_live(live)

                    # Firebase rounds batch
                    self.firebase.push_rounds({str(rid): {"m": mult, "ts": ts}})

                    # Stats toutes les 50
                    if self.round_count % 50 == 0:
                        self.firebase.update_stats(self.analyzer.analyze())
                        self.log(f"=== Stats MAJ ({self.round_count}) ===")


async def main():
    print("""
    ====================================================
      1xBet CRASH Smart Collector v5
      Salle configurable + auto-detection
    ====================================================
    """, flush=True)

    collector = Collector()

    def on_exit(sig, frame):
        collector.running = False
        collector.log("Arret.")
        sys.exit(0)

    signal.signal(signal.SIGINT, on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    print(f"  Firebase:  {FIREBASE_URL}", flush=True)
    print(f"  Salle:     gr={collector.room_id}", flush=True)
    print(f"  En base:   {collector.round_count} resultats", flush=True)
    print(f"  Dashboard: http://localhost:{PORT}/\n", flush=True)

    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
