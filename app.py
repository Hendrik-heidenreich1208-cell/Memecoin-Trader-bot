import os
import time
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, Request, HTTPException
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Memecoin Wallet Alert Bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
MIN_USD = float(os.getenv("MIN_USD", "250"))
SOLANA_EXPLORER = "https://solscan.io/tx/"

# Wallets you want to monitor.
# Format:
# {
#   "wallet_address": {"name": "Trader A", "score": 92}
# }
TRACKED_WALLETS: Dict[str, Dict[str, Any]] = {
    "9jyqFiLnruggwNn4EQwBNFXwpbLM9hrA4hV59ytyAVVz": {
        "name": "Trader 1",
        "score": 1
    }
    # "PASTE_SOLANA_WALLET_HERE": {"name": "Trader 1", "score": 90},
}

# Common quote / base assets that should generally NOT be reported as the
# "memecoin bought". You can extend this list.
IGNORE_SYMBOLS = {
    "SOL", "WSOL", "USDC", "USDT", "JUP", "JITOSOL", "MSOL", "BSOL"
}

# Primitive duplicate protection for server restarts.
_seen = {}
SEEN_TTL_SECONDS = 3600


def cleanup_seen() -> None:
    now = time.time()
    for k, t in list(_seen.items()):
        if now - t > SEEN_TTL_SECONDS:
            _seen.pop(k, None)


async def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Telegram credentials are missing.")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()


def get_account_keys(tx: Dict[str, Any]) -> List[str]:
    keys = []

    # Enhanced Helius payloads commonly expose accountData.
    for item in tx.get("accountData", []) or []:
        account = item.get("account")
        if account:
            keys.append(account)

    # Fallback fields sometimes seen in parsed payloads.
    fee_payer = tx.get("feePayer")
    if fee_payer:
        keys.append(fee_payer)

    return list(dict.fromkeys(keys))


def identify_trader(tx: Dict[str, Any]) -> tuple[str | None, Dict[str, Any] | None]:
    addresses = set(get_account_keys(tx))

    # Some payloads can include nativeTransfers/tokenTransfers with owner fields.
    for tr in tx.get("tokenTransfers", []) or []:
        for k in ("fromUserAccount", "toUserAccount"):
            if tr.get(k):
                addresses.add(tr[k])

    for tr in tx.get("nativeTransfers", []) or []:
        for k in ("fromUserAccount", "toUserAccount"):
            if tr.get(k):
                addresses.add(tr[k])

    for wallet, meta in TRACKED_WALLETS.items():
        if wallet in addresses:
            return wallet, meta

    return None, None


def infer_purchase(tx: Dict[str, Any], trader_wallet: str) -> Dict[str, Any] | None:
    """
    Generic heuristic:
    find the largest positive token transfer INTO the tracked wallet
    for a non-ignored token. This is intentionally conservative and should
    be upgraded with DEX-specific parsing / Birdeye enrichment for production.
    """
    candidates = []

    for tr in tx.get("tokenTransfers", []) or []:
        to_user = tr.get("toUserAccount")
        if to_user != trader_wallet:
            continue

        mint = tr.get("mint", "unknown")
        amount = tr.get("tokenAmount")
        symbol = tr.get("symbol") or "UNKNOWN"

        if symbol.upper() in IGNORE_SYMBOLS:
            continue
        if amount is None:
            continue

        candidates.append({
            "mint": mint,
            "symbol": symbol,
            "amount": amount,
        })

    if not candidates:
        return None

    # Without price enrichment, tokenAmount isn't directly comparable across tokens.
    # Usually one bought token dominates the incoming legs.
    return candidates[0]


def estimate_spend_usd(tx: Dict[str, Any], trader_wallet: str) -> float | None:
    """
    Attempts to estimate spend when Helius/enrichment includes USD-like metadata.
    Production recommendation: query Birdeye price/token overview for the mint
    and calculate transaction USD value precisely.
    """
    # Custom enrichers can put volumeUSD in the payload.
    for key in ("volumeUSD", "usdValue", "valueUsd"):
        v = tx.get(key)
        if isinstance(v, (int, float)):
            return float(v)
    return None


@app.get("/")
async def health():
    return {"ok": True, "tracked_wallets": len(TRACKED_WALLETS)}


@app.post("/helius")
async def helius_webhook(request: Request):
    if WEBHOOK_SECRET:
        supplied = request.headers.get("x-webhook-secret", "")
        if supplied != WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Bad webhook secret")

    payload = await request.json()
    txs = payload if isinstance(payload, list) else [payload]

    cleanup_seen()
    alerts = 0

    for tx in txs:
        signature = tx.get("signature") or tx.get("transactionSignature")
        if signature and signature in _seen:
            continue

        trader_wallet, meta = identify_trader(tx)
        if not trader_wallet:
            continue

        purchase = infer_purchase(tx, trader_wallet)
        if not purchase:
            continue

        usd = estimate_spend_usd(tx, trader_wallet)
        if usd is not None and usd < MIN_USD:
            continue

        name = meta.get("name", "Tracked trader")
        score = meta.get("score", "?")
        symbol = purchase["symbol"]
        mint = purchase["mint"]
        amount = purchase["amount"]

        usd_text = f"${usd:,.0f}" if usd is not None else "noch nicht berechnet"
        tx_link = f"{SOLANA_EXPLORER}{signature}" if signature else "keine Signatur"

        message = (
            f"🚨 MEMECOIN BUY ALERT\n\n"
            f"Trader: {name}\n"
            f"Trader-Score: {score}/100\n"
            f"Token: {symbol}\n"
            f"Mint: {mint}\n"
            f"Menge: {amount}\n"
            f"Kaufwert: {usd_text}\n\n"
            f"Transaktion: {tx_link}\n\n"
            f"⚠️ Nur Wallet-Tracking, keine Anlageberatung."
        )

        await send_telegram(message)
        alerts += 1

        if signature:
            _seen[signature] = time.time()

    return {"ok": True, "alerts_sent": alerts}


@app.get("/test-telegram")
async def test_telegram():
    await send_telegram("✅ Telegram-Test erfolgreich! Dein Memecoin-Bot funktioniert.")
    return {"ok": True}
