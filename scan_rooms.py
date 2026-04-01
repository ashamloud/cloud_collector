"""
Scanner de salles Crash 1xBet
Teste differents IDs de salle (gr=) pour trouver toutes les salles actives
"""
import asyncio
import json
import websockets

async def test_room(gr_id):
    """Teste si une salle Crash existe avec l'ID donne"""
    url = f"wss://1xbet.com/games-frame/sockets/crash?ref=1&gr={gr_id}&whence=55&fcountry=165&appGuid=games-web-host-b2c-web-v3&lng=fr&v=1.5"
    headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://1xbet.com"}
    
    try:
        async with websockets.connect(url, additional_headers=headers, open_timeout=10, close_timeout=3) as ws:
            # Handshake SignalR
            await ws.send(json.dumps({"protocol": "json", "version": 1}) + "\x1e")
            await ws.recv()
            
            # Guest mode
            await ws.send(json.dumps({
                "arguments": [{"activity": 30, "currency": 27}],
                "invocationId": "0", "target": "Guest", "type": 1
            }) + "\x1e")
            
            # Attendre OnRegistration
            for _ in range(10):
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                for part in msg.split("\x1e"):
                    if not part.strip():
                        continue
                    try:
                        data = json.loads(part)
                        if data.get("target") == "OnRegistration":
                            arg = data.get("arguments", [{}])[0]
                            history = arg.get("fs", [])
                            game_name = arg.get("gn", "unknown")
                            players = arg.get("pc", 0)
                            last_results = [h.get("f", 0) for h in history[:5]]
                            
                            print(f"  [OK] gr={gr_id} | Jeu: {game_name} | Joueurs: {players} | Derniers: {last_results}")
                            return {
                                "gr": gr_id,
                                "name": game_name,
                                "players": players,
                                "history": last_results,
                                "url": url
                            }
                    except:
                        pass
            
            print(f"  [?] gr={gr_id} - connecte mais pas de OnRegistration")
            return None
            
    except Exception as e:
        err = str(e)[:50]
        if "404" not in err and "403" not in err:
            print(f"  [X] gr={gr_id} - {err}")
        return None

async def main():
    print("=== Scanner de salles Crash 1xBet ===\n")
    
    # Tester une large plage d'IDs
    # On sait que gr=285 existe. Testons autour et les IDs courants
    test_ids = list(range(1, 50)) + list(range(100, 120)) + list(range(200, 310)) + list(range(400, 420)) + list(range(500, 520))
    
    rooms = []
    
    # Tester par lots de 10 pour ne pas surcharger
    for i in range(0, len(test_ids), 10):
        batch = test_ids[i:i+10]
        print(f"\nTest IDs {batch[0]}-{batch[-1]}...")
        tasks = [test_room(gid) for gid in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, dict) and r:
                rooms.append(r)
    
    print(f"\n{'='*50}")
    print(f"SALLES TROUVEES: {len(rooms)}")
    print(f"{'='*50}")
    for r in rooms:
        print(f"  gr={r['gr']:4d} | {r['name']:20s} | {r['players']} joueurs | {r['history']}")
    
    # Sauvegarder
    with open("discovered_rooms.json", "w") as f:
        json.dump(rooms, f, indent=2)
    print(f"\nSauvegarde dans discovered_rooms.json")

if __name__ == "__main__":
    asyncio.run(main())
