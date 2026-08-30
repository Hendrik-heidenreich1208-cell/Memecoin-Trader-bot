# Memecoin Telegram Wallet Alert Bot

Ein Starter-Bot für Solana, der beobachtete Trader-Wallets überwacht und bei erkannten Token-Käufen Telegram-Alarme sendet.

## Wichtig zur "30 Minuten vorher"-Idee

Aus öffentlichen On-Chain-Daten lässt sich ein Kauf normalerweise erst erkennen, wenn die Transaktion gesendet bzw. verarbeitet wird.
Ein zuverlässiger Alarm 30 Minuten **vor** einem fremden Kauf ist daher nicht möglich, solange der Trader seine Absicht nicht vorher öffentlich signalisiert.

Was möglich ist:
- Alarm sehr kurz nach der Transaktion
- Wallet-Tracking mehrerer Trader
- Mindest-Kaufwert
- Trader-Score
- Token-/Liquidity-/Market-Cap-Filter
- später: automatische Ranking-Logik nach Winrate/ROI
- später: Birdeye/Jupiter/DEX-Enrichment

## Einrichtung

1. Erstelle bei Telegram über BotFather einen Bot und kopiere den Bot-Token.
2. Starte einen Chat mit deinem Bot.
3. Ermittle deine Telegram Chat-ID.
4. Kopiere `.env.example` nach `.env` und trage die Werte ein.
5. Öffne `app.py` und trage unter `TRACKED_WALLETS` die Solana-Adressen ein, die überwacht werden sollen.

Beispiel:

```python
TRACKED_WALLETS = {
    "SOLANA_WALLET_1": {"name": "Trader Alpha", "score": 94},
    "SOLANA_WALLET_2": {"name": "Trader Beta", "score": 88},
}
```

6. Installation:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

Unter Windows:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

7. Deploye den Server öffentlich, z.B. auf Railway, Render, Fly.io oder einem VPS.
8. Erstelle bei Helius einen Webhook auf:

`https://DEINE-DOMAIN/helius`

und lasse die Wallet-Adressen überwachen.

Falls du `WEBHOOK_SECRET` benutzt, muss dein Proxy/Webhook-Setup den Header
`x-webhook-secret` mit demselben Wert mitsenden.

## Produktions-Ausbau

Die mitgelieferte Erkennung ist absichtlich ein Starter. Für einen belastbaren Bot sollte die nächste Version:

- echte DEX-Swaps statt bloßer Transfers klassifizieren,
- Tokenpreise und Kaufwert in USD anreichern,
- Market Cap und Liquidität prüfen,
- Honeypot-/Scam-Risiken filtern,
- Wallets anhand historischer Ergebnisse bewerten,
- Gewinne nach 5m / 30m / 2h / 24h tracken,
- dieselbe Wallet nicht mehrfach für denselben Trade melden,
- mehrere Chains optional unterstützen.

## Sicherheits-Hinweis

Der Bot sollte nur öffentliche Blockchain-Daten verwenden. Private Keys oder Seed Phrases gehören niemals in den Bot.
