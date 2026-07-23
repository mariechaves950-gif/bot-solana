import os
import time
import requests

# --- CONFIGURATION ---
# Le token ne doit JAMAIS être écrit en dur ici.
# Sur Railway : Settings > Variables > ajoute TELEGRAM_TOKEN et CHAT_ID
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    raise RuntimeError("TELEGRAM_TOKEN ou CHAT_ID manquant dans les variables d'environnement")

active_tokens = {}
seen_mints = set()  # évite de re-alerter le même token à chaque cycle


def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=5)
        if r.status_code != 200:
            print(f"Erreur Telegram ({r.status_code}) : {r.text}")
    except Exception as e:
        print(f"Erreur d'envoi Telegram : {e}")


def check_new_solana_tokens():
    """
    Récupère les derniers profils de tokens soumis sur DexScreener
    et filtre sur la chaîne Solana. C'est le bon endpoint : l'ancien
    /latest/dex/tokens/solana n'existe pas (il attend une adresse de
    token, pas un nom de chaîne), d'où l'absence d'alertes.
    """
    try:
        url = "https://api.dexscreener.com/token-profiles/latest/v1"
        response = requests.get(url, timeout=10)
        print(f"[debug] status={response.status_code}")

        if response.status_code != 200:
            return

        data = response.json()
        # La réponse peut être une liste directe ou un objet {"data": [...]}
        profiles = data if isinstance(data, list) else data.get("data", [])
        print(f"[debug] {len(profiles)} profils reçus")

        for profile in profiles:
            if not profile or profile.get("chainId") != "solana":
                continue

            mint = profile.get("tokenAddress")
            if not mint or mint in seen_mints:
                continue

            seen_mints.add(mint)

            # On va chercher les infos de marché (prix, liquidité) via l'endpoint pairs
            pair_url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
            pair_res = requests.get(pair_url, timeout=5)
            pairs = pair_res.json().get("pairs") if pair_res.status_code == 200 else None
            pair = pairs[0] if pairs else None

            name = symbol = "Inconnu"
            market_cap = 0
            liquidity_usd = 0
            dex_name = "DEX inconnu"
            pair_url_link = profile.get("url", "https://dexscreener.com/solana")

            if pair:
                base_token = pair.get("baseToken") or {}
                name = base_token.get("name", name)
                symbol = base_token.get("symbol", symbol)
                market_cap = pair.get("marketCap", 0) or pair.get("fdv", 0) or 0
                liquidity = pair.get("liquidity") or {}
                liquidity_usd = liquidity.get("usd", 0)
                dex_name = pair.get("dexId", dex_name)
                pair_url_link = pair.get("url", pair_url_link)

            msg_ok = (
                f"✅ *Nouveau Token Solana Détecté !*\n\n"
                f"🪙 Nom : {name} ({symbol})\n"
                f"🏦 DEX : {dex_name}\n"
                f"📊 Market Cap / FDV : ${market_cap:,.0f}\n"
                f"💧 Liquidité USD : ${liquidity_usd:,.0f}\n"
                f"🔗 [Voir sur DexScreener]({pair_url_link})"
            )
            send_telegram_message(msg_ok)
            print(f"Alerte envoyée pour : {symbol} ({mint})")

            if pair:
                active_tokens[mint] = {
                    "symbol": symbol,
                    "max_price": market_cap or 1.0,
                    "start_time": time.time(),
                }

    except Exception as e:
        print(f"Erreur lors de la vérification DexScreener : {e}")


def monitor_ath():
    current_time = time.time()
    tokens_to_remove = []

    for mint, data in list(active_tokens.items()):
        elapsed = current_time - data["start_time"]

        if elapsed > 1800:
            tokens_to_remove.append(mint)
            continue

        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
            res = requests.get(url, timeout=5)
            if res.status_code == 200:
                token_data = res.json()
                pairs = token_data.get("pairs") if token_data else None
                if pairs and pairs[0]:
                    current_mc = pairs[0].get("marketCap", 0) or pairs[0].get("fdv", 0)

                    if current_mc and current_mc > data["max_price"]:
                        active_tokens[mint]["max_price"] = current_mc
                        msg_ath = (
                            f"🚀 *Nouvel ATH (30 min) !*\n\n"
                            f"🪙 Token : {data['symbol']}\n"
                            f"📈 Nouveau Max : ${current_mc:,.0f}\n"
                            f"⏱️ Temps écoulé : {int(elapsed // 60)} min"
                        )
                        send_telegram_message(msg_ath)
                        print(f"Alerte ATH envoyée pour : {data['symbol']}")
        except Exception as e:
            print(f"Erreur monitor_ath pour {mint} : {e}")

    for mint in tokens_to_remove:
        active_tokens.pop(mint, None)


if __name__ == "__main__":
    print("Bot de surveillance démarré...")
    while True:
        check_new_solana_tokens()
        monitor_ath()
        time.sleep(10)
