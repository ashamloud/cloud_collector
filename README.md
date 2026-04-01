# 1xBet Crash Collector - Cloud Edition

Collecteur de resultats Crash 1xBet qui tourne 24/7 sur serveur gratuit.

## API Endpoints
- `GET /` → Status du collecteur
- `GET /stats` → Analyse complete (correlations, streaks, predictions)
- `GET /recent` → 50 derniers resultats
- `GET /live` → Donnees en direct

## Deploy on Render.com (gratuit)
1. Push ce dossier sur GitHub
2. Va sur https://render.com
3. "New" → "Background Worker"
4. Connecte ton repo GitHub
5. Runtime: Python, Start command: `python main.py`
6. Deploy!
