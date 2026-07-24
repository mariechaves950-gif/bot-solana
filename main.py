import os
import csv
import time
import requests

# --- CONFIGURATION ---
# Le token ne doit JAMAIS être écrit en dur ici.
# Sur Railway : Settings > Variables > ajoute TELEGRAM_TOKEN et CHAT_ID
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    raise RuntimeError("TELEGRAM_TOKEN ou CHAT_ID manquant dans les variables d'environnement")

active_tokens = {}      # tokens alertés, en cours de suivi ATH
seen_mints = set()      # mints déjà ALERTÉS (pour ne jamais ré-alerter le même token)
pending_mints = {}      # mints vus mais PAS ENCORE migrés : mint -> premier vu (timestamp)

# Un mint en attente de migration est abandonné après ce délai (il ne migrera
# probablement plus, inutile de continuer à interroger l'API pour lui).
PENDING_MAX_AGE = 24 * 3600  # 24h

# --- FILTRES DE QUALITÉ ---
# Ces seuils réduisent le bruit (tokens sans intérêt / scams grossiers)
# mais ne garantissent JAMAIS qu'un token est fiable. Aucun filtre
# automatique ne protège d'un rug pull.
MIN_LIQUIDITY_USD = 5000      # liquidité minimum sur le pool
MIN_MARKET_CAP = 20000        # market cap / FDV minimum
MIN_LIQUIDITY_RATIO = 0.03    # liquidité doit représenter au moins 3% du market cap

# Un token pump.fun reste sur "pumpfun" (bonding curve) tant qu'il n'a pas
# gradué. Une fois la bonding curve terminée (migration), il apparaît sur
# PumpSwap ou Raydium. On ne veut alerter QUE sur les tokens déjà migrés.
DEX_MIGRES = {"pumpswap", "raydium"}

# --- LOG POUR ANALYSE STATISTIQUE ---
# Chaque token, une fois son suivi de 30 min terminé, est ajouté comme une
# ligne dans ce CSV : toutes les données qu'on avait AU MOMENT DE L'ALERTE
# (donc disponibles à l'avance) + le résultat final (multiplicateur). Ça
# permet de comparer après coup ce qui différencie les tokens qui font x2+
# de ceux qui ne font rien.
# Attention : sur Railway, le système de fichiers est éphémère lors d'un
# redéploiement. Pour un historique qui survit aux redéploiements, il
# faudrait plutôt écrire vers un Google Sheet, une base externe (ex.
# Supabase/Postgres), ou un volume persistant Railway.
LOG_FILE = "token_log.csv"
LOG_FIELDS = [
    "timestamp", "mint", "symbol", "dex",
    "initial_mc", "max_mc", "multiplicateur",
    "liquidity_usd", "liquidity_ratio",
    "rugcheck_score", "rugcheck_flags",
    "txns_buys_m5", "txns_sells_m5", "volume_m5",
    "txns_buys_h1", "txns_sells_h1", "volume_h1",
]


def log_resultat_csv(row):
    """Ajoute une ligne au CSV de log, en créant l'en-tête si besoin."""
    try:
        file_existe = os.path.isfile(LOG_FILE)
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LOG_FIELDS)
            if not file_existe:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[log_resultat_csv] erreur : {e}")


def _to_float(value):
    """Conversion défensive en float (le priceUsd de DexScreener est une string)."""
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def passe_les_filtres(market_cap, liquidity_usd):
    if not market_cap or not liquidity_usd:
        return False
    if liquidity_usd < MIN_LIQUIDITY_USD:
        return False
    if market_cap < MIN_MARKET_CAP:
        return False
    if (liquidity_usd / market_cap) < MIN_LIQUIDITY_RATIO:
        return False
    return True


# Score de risque RugCheck max toléré (0 = aucun risque, 100 = risque max)
RUGCHECK_MAX_SCORE = 20


def rugcheck_verdict(mint):
    """
    Interroge RugCheck.xyz (API publique, sans clé) pour un score de risque.
    Retourne (ok: bool, score: int|None, flags: list[str]).
    En cas d'erreur ou de timeout, on considère le token comme "non vérifiable"
    et on ne l'envoie pas (mieux vaut rater une alerte qu'en envoyer une dangereuse).
    """
    try:
        url = f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
        res = requests.get(url, timeout=8)
        if res.status_code != 200:
            print(f"[rugcheck] status={res.status_code} pour {mint}")
            return False, None, []

        data = res.json()
        score = data.get("score_normalised")
        risks = data.get("risks") or []
        flags = [r.get("name", "?") for r in risks if r.get("level") in ("warn", "danger")]
        lp_locked = data.get("lpLockedPct", 0)

        if score is None:
            return False, None, flags

        ok = score <= RUGCHECK_MAX_SCORE and lp_locked and lp_locked > 0
        return ok, score, flags

    except Exception as e:
        print(f"[rugcheck] erreur pour {mint} : {e}")
        return False, None, []


def get_true_ath_mc(pool_address, initial_mc, initial_price, start_time):
    """
    Récupère les vraies bougies OHLCV via GeckoTerminal (API publique,
    gratuite, sans clé) pour calculer le véritable plus haut (ATH) atteint
    depuis la migration — sans avoir besoin de sonder l'API toutes les X
    secondes. Le "high" de chaque bougie capture les pics même s'ils ne
    durent que quelques secondes entre deux vérifications classiques.

    Retourne le market cap ATH estimé (converti à partir du ratio de prix),
    ou None si l'appel échoue ou si on n'a pas de prix initial pour convertir
    — dans ce cas, on retombe sur le suivi par polling classique.
    """
    if not pool_address or not initial_price:
        return None
    try:
        elapsed_minutes = max(int((time.time() - start_time) / 60) + 5, 10)
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/minute"
        params = {
            "aggregate": 1,
            "limit": min(elapsed_minutes, 1000),
            "currency": "usd",
            "token": "base",
        }
        res = requests.get(url, params=params, timeout=8)
        if res.status_code != 200:
            print(f"[geckoterminal] status={res.status_code} pour {pool_address}")
            return None

        ohlcv_list = res.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        if not ohlcv_list:
            return None

        # Chaque bougie : [timestamp, open, high, low, close, volume]
        max_high = max(candle[2] for candle in ohlcv_list if len(candle) >= 3)
        if not max_high:
            return None

        return initial_mc * (max_high / initial_price)

    except Exception as e:
        print(f"[geckoterminal] erreur pour {pool_address} : {e}")
        return None


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


def fetch_pair_data(mint):
    """Récupère la 1ère pair DexScreener connue pour un mint, ou None."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        res = requests.get(url, timeout=5)
        if res.status_code != 200:
            return None
        pairs = res.json().get("pairs")
        return pairs[0] if pairs else None
    except Exception as e:
        print(f"[fetch_pair_data] erreur pour {mint} : {e}")
        return None


def essayer_alerter(mint, pair, source_url):
    """
    Vérifie si le pair migré passe les filtres + RugCheck, et alerte si oui.

    C'EST ICI qu'on capture le market cap "initial" utilisé pour le
    multiplicateur du rapport 30 min. Cette fonction est appelée soit :
    - dès la découverte du mint si le pool est déjà migré à ce moment-là,
    - soit à chaque cycle (10s) tant qu'il est en attente dans pending_mints,
    de façon à capter le market cap au tout premier instant où le pool
    migré (Raydium/PumpSwap) est visible — et non au moment (potentiellement
    tardif) où le bot a simplement fini par scanner le token.

    Retourne True si le token a été alerté (à retirer de pending_mints),
    False sinon.
    """
    base_token = pair.get("baseToken") or {}
    name = base_token.get("name", "Inconnu")
    symbol = base_token.get("symbol", "Inconnu")
    market_cap = pair.get("marketCap", 0) or pair.get("fdv", 0) or 0
    liquidity_usd = (pair.get("liquidity") or {}).get("usd", 0)
    dex_name = pair.get("dexId", "DEX inconnu")
    pair_url_link = pair.get("url", source_url)

    if pair.get("dexId") not in DEX_MIGRES:
        print(f"[non migré] {symbol} ({mint}) — dex={pair.get('dexId')}")
        return False

    if not passe_les_filtres(market_cap, liquidity_usd):
        print(f"[filtré] {symbol} ({mint}) — MC=${market_cap:,.0f} Liq=${liquidity_usd:,.0f}")
        return False

    rug_ok, rug_score, rug_flags = rugcheck_verdict(mint)
    if not rug_ok:
        print(f"[rugcheck] {symbol} ({mint}) rejeté — score={rug_score} flags={rug_flags}")
        return False

    # Données supplémentaires, non utilisées comme filtre, juste pour le log
    # et la comparaison a posteriori (voir LOG_FIELDS).
    txns = pair.get("txns") or {}
    volume = pair.get("volume") or {}
    txns_m5 = txns.get("m5") or {}
    txns_h1 = txns.get("h1") or {}
    entry_stats = {
        "liquidity_ratio": round(liquidity_usd / market_cap, 4) if market_cap else None,
        "txns_buys_m5": txns_m5.get("buys"),
        "txns_sells_m5": txns_m5.get("sells"),
        "volume_m5": volume.get("m5"),
        "txns_buys_h1": txns_h1.get("buys"),
        "txns_sells_h1": txns_h1.get("sells"),
        "volume_h1": volume.get("h1"),
    }

    flags_txt = ", ".join(rug_flags) if rug_flags else "Aucun"
    msg_ok = (
        f"✅ *Nouveau Token Solana Détecté !*\n\n"
        f"🪙 Nom : {name} ({symbol})\n"
        f"🏦 DEX : {dex_name}\n"
        f"📊 Market Cap / FDV : ${market_cap:,.0f}\n"
        f"💧 Liquidité USD : ${liquidity_usd:,.0f}\n"
        f"🛡️ RugCheck Score : {rug_score}/100\n"
        f"🚩 Flags : {flags_txt}\n"
        f"🔗 [Voir sur DexScreener]({pair_url_link})\n"
        f"⚡ [Trader sur Axiom](https://axiom.trade/meme/{mint})\n"
        f"🔍 [Voir sur RugCheck](https://rugcheck.xyz/tokens/{mint})"
    )
    send_telegram_message(msg_ok)
    print(f"Alerte envoyée pour : {symbol} ({mint}) — RugCheck score={rug_score}")

    active_tokens[mint] = {
        "symbol": symbol,
        "dex": dex_name,
        "initial_mc": market_cap or 1.0,
        "max_price": market_cap or 1.0,
        "liquidity_usd": liquidity_usd,
        "rugcheck_score": rug_score,
        "rugcheck_flags": flags_txt,
        "start_time": time.time(),
        "dex_url": pair_url_link,  # <-- pour le lien dans le rapport 30 min
        "entry_stats": entry_stats,  # <-- pour le log CSV à la fin du suivi
        "pool_address": pair.get("pairAddress"),  # <-- pour interroger GeckoTerminal
        "initial_price": _to_float(pair.get("priceUsd")),
    }
    seen_mints.add(mint)
    return True


def check_new_solana_tokens():
    """
    Découvre de nouveaux mints Solana via le flux de profils DexScreener.
    Un mint déjà alerté (seen_mints) ou déjà suivi (pending_mints /
    active_tokens) n'est pas retraité ici. S'il n'est pas encore migré,
    on le place en attente : check_pending_tokens() le re-vérifiera à
    chaque cycle pour capter le tout premier instant de migration.
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
            if not mint:
                continue
            if mint in seen_mints or mint in pending_mints or mint in active_tokens:
                continue

            pair = fetch_pair_data(mint)
            source_url = profile.get("url", "https://dexscreener.com/solana")

            if not pair or pair.get("dexId") not in DEX_MIGRES:
                # Pas encore de pool, ou encore sur pump.fun (bonding curve) :
                # on met en attente de migration.
                pending_mints[mint] = time.time()
                continue

            # Déjà migré au moment où on le découvre : 1er point de donnée
            # disponible, on l'utilise comme market cap initial.
            essayer_alerter(mint, pair, source_url)

    except Exception as e:
        print(f"Erreur lors de la vérification DexScreener : {e}")


# Un mint en attente est re-testé au maximum toutes les PENDING_CHECK_INTERVAL
# secondes (et non à chaque boucle de 10s) pour éviter de saturer l'API
# DexScreener si beaucoup de mints sont en attente de migration en même temps.
# 10s reste la meilleure précision possible sur le moment de la migration ;
# on augmente cet intervalle seulement si le nombre de pending_mints devient
# trop grand pour l'API.
PENDING_CHECK_INTERVAL = 30
_last_pending_check = 0


def check_pending_tokens():
    """
    Reprend chaque mint en attente et vérifie si son pool est passé sur
    Raydium/PumpSwap depuis le dernier passage. Ne s'exécute réellement que
    toutes les PENDING_CHECK_INTERVAL secondes (throttling), donc on capture
    le market cap au premier cycle de vérification où la migration devient
    visible : c'est ça, le vrai "market cap initial".
    """
    global _last_pending_check
    now = time.time()

    if (now - _last_pending_check) < PENDING_CHECK_INTERVAL:
        return
    _last_pending_check = now

    to_remove = []

    for mint, first_seen in list(pending_mints.items()):
        if now - first_seen > PENDING_MAX_AGE:
            to_remove.append(mint)
            continue

        pair = fetch_pair_data(mint)
        if not pair or pair.get("dexId") not in DEX_MIGRES:
            continue  # toujours pas migré, on réessaiera au prochain cycle

        essayer_alerter(mint, pair, f"https://dexscreener.com/solana/{mint}")
        # Migré ou non retenu (filtres/rugcheck), inutile de le re-tester :
        # un pool ne "migre" qu'une fois.
        to_remove.append(mint)

    for mint in to_remove:
        pending_mints.pop(mint, None)


PRICE_CHECK_INTERVAL = 120  # ne va chercher le prix que toutes les 2 minutes
_last_price_check = 0


def monitor_ath():
    """
    La détection de nouveaux tokens reste rapide (toutes les 10s, dans la
    boucle principale). Ici, on ne va chercher le prix sur DexScreener que
    toutes les PRICE_CHECK_INTERVAL secondes, pour économiser des requêtes.
    Un seul rapport est envoyé sur Telegram, 30 minutes après la détection
    initiale, résumant le multiplicateur max atteint.
    """
    global _last_price_check
    current_time = time.time()
    tokens_to_remove = []

    do_price_check = (current_time - _last_price_check) >= PRICE_CHECK_INTERVAL
    if do_price_check:
        _last_price_check = current_time

    for mint, data in list(active_tokens.items()):
        elapsed = current_time - data["start_time"]

        if do_price_check:
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
            except Exception as e:
                print(f"Erreur monitor_ath pour {mint} : {e}")

        if elapsed >= 1800:
            initial_mc = data["initial_mc"] or 1.0
            max_mc = active_tokens[mint]["max_price"]
            multiplicateur = max_mc / initial_mc
            dex_url = data.get("dex_url", f"https://dexscreener.com/solana/{mint}")

            msg_rapport = (
                f"📋 *Rapport 30 min*\n\n"
                f"🪙 Token : {data['symbol']}\n"
                f"💰 Market Cap initial (à la migration) : ${initial_mc:,.0f}\n"
                f"🏆 Market Cap max atteint : ${max_mc:,.0f}\n"
                f"✖️ Multiplicateur : x{multiplicateur:,.2f}\n"
                f"🔗 [Voir sur DexScreener]({dex_url})\n"
                f"⚡ [Trader sur Axiom](https://axiom.trade/meme/{mint})"
            )
            send_telegram_message(msg_rapport)
            print(f"Rapport 30 min envoyé pour : {data['symbol']} — x{multiplicateur:,.2f}")

            entry_stats = data.get("entry_stats", {})
            log_resultat_csv({
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "mint": mint,
                "symbol": data["symbol"],
                "dex": data.get("dex"),
                "initial_mc": initial_mc,
                "max_mc": max_mc,
                "multiplicateur": round(multiplicateur, 3),
                "liquidity_usd": data.get("liquidity_usd"),
                "liquidity_ratio": entry_stats.get("liquidity_ratio"),
                "rugcheck_score": data.get("rugcheck_score"),
                "rugcheck_flags": data.get("rugcheck_flags"),
                "txns_buys_m5": entry_stats.get("txns_buys_m5"),
                "txns_sells_m5": entry_stats.get("txns_sells_m5"),
                "volume_m5": entry_stats.get("volume_m5"),
                "txns_buys_h1": entry_stats.get("txns_buys_h1"),
                "txns_sells_h1": entry_stats.get("txns_sells_h1"),
                "volume_h1": entry_stats.get("volume_h1"),
            })

            tokens_to_remove.append(mint)

    for mint in tokens_to_remove:
        active_tokens.pop(mint, None)


if __name__ == "__main__":
    print("Bot de surveillance démarré...")
    while True:
        check_new_solana_tokens()
        check_pending_tokens()
        monitor_ath()
        time.sleep(10)
