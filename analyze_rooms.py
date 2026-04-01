import json

with open("discovered_rooms.json") as f:
    rooms = json.load(f)

unique = {}
for r in rooms:
    key = str(r["history"][:3])
    if key not in unique:
        unique[key] = {"ids": [], "h": r["history"]}
    unique[key]["ids"].append(r["gr"])

print(f"{len(unique)} salles UNIQUES:\n")
for i, (k, v) in enumerate(unique.items()):
    ids = v["ids"]
    ids_str = str(ids[:5])
    extra = len(ids) - 5
    if extra > 0:
        ids_str += f" +{extra} autres"
    print(f"  Salle {i+1}: gr={ids_str} | Resultats: {v['h']}")
