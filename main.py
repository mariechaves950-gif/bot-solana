import os
import csv
import time
import threading
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

# --- CONCENTRATION DES HOLDERS (topHolders / risks, via RugCheck) ---
# RugCheck renvoie déjà topHolders + risks dans le même appel que le score
# de risque : pas besoin d'une API supplémentaire. On se contente d'aller
# lire ces champs dans la réponse JSON déjà reçue.
#
# REJECT_TOP10_PCT : si défini (ex: 50.0), rejette automatiquement tout
# token dont les 10 premiers wallets détiennent plus de ce % de l'offre.
# Laisse à None pour juste LOGUER la valeur sans filtrer (recommandé au
# début, le temps de voir ce que ça donne dans le CSV).
REJECT_TOP10_PCT = None

# REJECT_IF_INSIDERS : si True, rejette tout token où RugCheck détecte au
# moins une alerte de concentration/insiders dans sa liste "risks"
# (single_holder, high_concentration).
REJECT_IF_INSIDERS = False

# --- ANALYSE DES 20 PREMIÈRES SECONDES ---
# Juste après l'alerte, on sonde le prix (et les txns m5) à haute fréquence
# (toutes les ANALYSE_20S_SAMPLE_INTERVAL secondes) pendant
# ANALYSE_20S_DURATION secondes, pour savoir à quelle seconde précise le
# token atteint son premier creux, et pour estimer l'activité achats/ventes
# réelle sur cette fenêtre (par delta, voir plus bas). Tourne dans un thread
# séparé pour ne jamais bloquer la boucle principale (qui, elle, ne tourne
# que toutes les 10s).
ANALYSE_20S_SAMPLE_INTERVAL = 2   # secondes entre 2 mesures
ANALYSE_20S_DURATION = 20         # durée totale observée

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
    "horodatage", "mint", "symbole", "dex",
    "mc_initial", "mc_max", "multiplicateur",
    "liquidite_usd", "ratio_liquidite",
    "score_rugcheck", "alertes_rugcheck",
    "pct_top10_holders", "insiders_detectes",
    "nombre_holders", "bundle_detecte",
    "pool_age_seconds", "lp_locked_pct",
    "achats_m5", "ventes_m5", "volume_m5",
    "achats_h1", "ventes_h1", "volume_h1",
    "seconde_prix_plus_bas_20s", "multiplicateur_plus_bas_20s",
    "buy_ratio_10s", "buy_ratio_20s", "achats_bruts_2s",
    "price_change_m5", "tx_accel", "buy_ratio_2s",
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


def _extraire_concentration_holders(data):
    """
    Extrait la concentration des 10 premiers wallets depuis "topHolders",
    et compte le nombre d'alertes de concentration/insiders présentes dans
    la liste "risks" de RugCheck (ex: "single_holder", "high_concentration")
    — aucun appel réseau supplémentaire, tout vient de la réponse déjà reçue.
    Retourne (top10_pct: float|None, insiders_detected: int).
    """
    top_holders = data.get("topHolders") or []
    top10_pct = None
    if top_holders:
        try:
            # Chaque entrée a généralement un champ "pct" (part détenue en %).
            top10_pct = round(sum(h.get("pct", 0) or 0 for h in top_holders[:10]), 2)
        except (TypeError, ValueError):
            top10_pct = None

    risks = data.get("risks") or []
    concentration_keywords = ("single_holder", "high_concentration")
    insiders_detected = sum(
        1 for r in risks
        if any(kw in (r.get("name", "") or "").lower() for kw in concentration_keywords)
    )

    return top10_pct, insiders_detected


def _extraire_holders_et_bundle(data):
    """
    Extrait le nombre total de holders et détecte un éventuel "bundle"
    (achats groupés au lancement, signature de bots coordonnés / insiders),
    toujours depuis la même réponse RugCheck déjà reçue.

    ATTENTION : la doc publique de l'endpoint report/summary ne précise pas
    formellement le nom du champ "nombre de holders" ni un champ dédié
    "bundle" — on tente donc plusieurs clés plausibles, et on détecte le
    bundle par mot-clé dans "risks" (RugCheck y ajoute généralement une
    entrée explicite quand un bundle est détecté). Si total_holders reste
    systématiquement None une fois en prod, inspecte une réponse brute
    (print(data)) pour repérer le bon nom de champ et ajuste la liste
    ci-dessous.
    """
    total_holders = data.get("totalHolders")
    if total_holders is None:
        total_holders = data.get("holders")
    if total_holders is None:
        holder_analysis = data.get("holderAnalysis") or {}
        total_holders = holder_analysis.get("totalHolders") or holder_analysis.get("count")

    risks = data.get("risks") or []
    bundle_detected = any("bundle" in (r.get("name", "") or "").lower() for r in risks)

    return total_holders, bundle_detected


def rugcheck_verdict(mint):
    """
    Interroge RugCheck.xyz (API publique, sans clé) pour un score de risque.
    Retourne (ok: bool, score: int|None, flags: list[str], top10_pct: float|None,
    insiders_detected: int, lp_locked_pct: float|None, total_holders: int|None,
    bundle_detected: bool).
    En cas d'erreur ou de timeout, on considère le token comme "non vérifiable"
    et on ne l'envoie pas (mieux vaut rater une alerte qu'en envoyer une dangereuse).
    """
    try:
        url = f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
        res = requests.get(url, timeout=8)
        if res.status_code != 200:
            print(f"[rugcheck] status={res.status_code} pour {mint}")
            return False, None, [], None, 0, None, None, False

        data = res.json()
        score = data.get("score_normalised")
        risks = data.get("risks") or []
        flags = [r.get("name", "?") for r in risks if r.get("level") in ("warn", "danger")]
        lp_locked = data.get("lpLockedPct", 0)

        top10_pct, insiders_detected = _extraire_concentration_holders(data)
        total_holders, bundle_detected = _extraire_holders_et_bundle(data)

        if score is None:
            return False, None, flags, top10_pct, insiders_detected, lp_locked, total_holders, bundle_detected

        ok = score <= RUGCHECK_MAX_SCORE and lp_locked and lp_locked > 0

        if ok and REJECT_TOP10_PCT is not None and top10_pct is not None and top10_pct > REJECT_TOP10_PCT:
            print(f"[rugcheck] {mint} rejeté — concentration top10={top10_pct}% > {REJECT_TOP10_PCT}%")
            ok = False

        if ok and REJECT_IF_INSIDERS and insiders_detected > 0:
            print(f"[rugcheck] {mint} rejeté — {insiders_detected} alerte(s) de concentration/insiders")
            ok = False

        return ok, score, flags, top10_pct, insiders_detected, lp_locked, total_holders, bundle_detected

    except Exception as e:
        print(f"[rugcheck] erreur pour {mint} : {e}")
        return False, None, [], None, 0, None, None, False


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


def send_telegram_document(filepath, caption=None):
    """
    Envoie un fichier en pièce jointe Telegram (utilisé pour le CSV de log).
    Si le fichier n'existe pas encore (aucun token n'a terminé son suivi
    de 30 min), on prévient l'utilisateur au lieu de planter.
    """
    if not os.path.isfile(filepath):
        send_telegram_message(f"⚠️ Aucun fichier `{filepath}` pour le moment (aucun token n'a encore terminé son suivi de 30 min).")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    try:
        with open(filepath, "rb") as f:
            files = {"document": (os.path.basename(filepath), f)}
            payload = {"chat_id": CHAT_ID}
            if caption:
                payload["caption"] = caption
            r = requests.post(url, data=payload, files=files, timeout=30)
            if r.status_code != 200:
                print(f"Erreur envoi document Telegram ({r.status_code}) : {r.text}")
    except Exception as e:
        print(f"Erreur d'envoi du document Telegram : {e}")


_telegram_update_offset = 0


def check_telegram_commands():
    """
    Vérifie les nouveaux messages reçus par le bot (getUpdates) et répond
    aux commandes reconnues. Appelée à chaque tour de la boucle principale
    (~toutes les 10s), donc une commande peut mettre jusqu'à ~10s à être
    traitée. On ignore tout message qui ne vient pas du CHAT_ID autorisé.

    Commandes reconnues : /csv, /log, /download -> envoie le CSV complet.
    """
    global _telegram_update_offset
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    params = {"offset": _telegram_update_offset + 1, "timeout": 0}
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code != 200:
            print(f"[check_telegram_commands] status={res.status_code}")
            return
        updates = res.json().get("result", [])
    except Exception as e:
        print(f"[check_telegram_commands] erreur : {e}")
        return

    for update in updates:
        _telegram_update_offset = max(_telegram_update_offset, update.get("update_id", 0))
        message = update.get("message") or {}
        chat_id = str((message.get("chat") or {}).get("id", ""))
        text = (message.get("text") or "").strip().lower()

        if chat_id != str(CHAT_ID):
            continue

        if text in ("/csv", "/log", "/download"):
            send_telegram_document(LOG_FILE, caption="📊 Historique complet des tokens suivis")
        elif text == "/help":
            send_telegram_message(
                "Commandes disponibles :\n"
                "/csv — télécharger le fichier de log complet"
            )


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


def analyser_20_premieres_secondes(mint):
    """
    Tourne dans un THREAD SÉPARÉ, lancé juste après l'alerte d'un token.
    Sonde le market cap ET les txns m5 (achats/ventes) toutes les
    ANALYSE_20S_SAMPLE_INTERVAL secondes pendant ANALYSE_20S_DURATION
    secondes.

    Deux types de résultats sont calculés :
    1) Le point le plus bas atteint (comme avant) : utile pour savoir
       statistiquement à quel moment il vaut mieux entrer.
    2) L'activité achats/ventes réelle sur la fenêtre, calculée PAR DELTA
       entre deux polls consécutifs. Comme DexScreener ne fournit que des
       compteurs sur une fenêtre glissante de 5 minutes (m5), on ne peut
       pas connaître l'activité exacte des 20 dernières secondes
       directement — on l'approxime en regardant de combien le compteur
       m5 a augmenté entre deux mesures rapprochées (2s d'écart) :
         - achats_bruts_2s : achats nets (achats - ventes) survenus entre
           le tout premier poll (t=0s) et le second (t=2s), pour mesurer
           combien de bots/snipers rentrent instantanément.
         - buy_ratio_10s / buy_ratio_20s : part des achats dans le total
           achats+ventes cumulés sur, respectivement, les 10 et 20
           premières secondes.

    Le résultat est stocké dans active_tokens[mint] pour être repris dans
    le rapport CSV final. Aucun message Telegram n'est envoyé ici : le
    résultat n'apparaît que dans le CSV.
    """
    data = active_tokens.get(mint)
    if not data:
        return
    initial_mc = data.get("initial_mc") or 1.0
    start = data["start_time"]
    samples = []  # liste de (seconde_ecoulee, market_cap, achats_m5, ventes_m5)

    nb_mesures = max(int(ANALYSE_20S_DURATION / ANALYSE_20S_SAMPLE_INTERVAL), 1)
    for i in range(nb_mesures + 1):
        elapsed = round(time.time() - start)
        pair = fetch_pair_data(mint)
        mc = None
        buys_m5 = sells_m5 = None
        if pair:
            mc = pair.get("marketCap", 0) or pair.get("fdv", 0)
            txns_m5 = (pair.get("txns") or {}).get("m5") or {}
            buys_m5 = txns_m5.get("buys")
            sells_m5 = txns_m5.get("sells")
        samples.append((elapsed, mc, buys_m5, sells_m5))
        if i < nb_mesures:
            time.sleep(ANALYSE_20S_SAMPLE_INTERVAL)

    # --- Point le plus bas (comme avant) ---
    valides = [(s, mc) for s, mc, _, _ in samples if mc]
    if valides:
        low_second, low_mc = min(valides, key=lambda x: x[1])
        low_mult = low_mc / initial_mc
        if mint in active_tokens:
            active_tokens[mint]["low_second_20s"] = low_second
            active_tokens[mint]["low_mult_20s"] = round(low_mult, 3)
        print(f"[analyse_20s] {data['symbol']} ({mint}) — point le plus bas à {low_second}s (x{low_mult:,.2f})")
    else:
        print(f"[analyse_20s] aucune donnée de market cap exploitable pour {mint}")

    # --- Deltas achats/ventes entre chaque poll consécutif ---
    deltas = []  # (elapsed_debut, elapsed_fin, delta_achats, delta_ventes)
    for (e_prev, _, b_prev, s_prev), (e_next, _, b_next, s_next) in zip(samples, samples[1:]):
        if None in (b_prev, s_prev, b_next, s_next):
            continue
        # Le compteur m5 est cumulatif sur une fenêtre glissante ; en théorie
        # il ne devrait pas décroître sur 2s, mais on protège quand même
        # contre un léger recalcul côté DexScreener.
        delta_achats = max(b_next - b_prev, 0)
        delta_ventes = max(s_next - s_prev, 0)
        deltas.append((e_prev, e_next, delta_achats, delta_ventes))

    achats_bruts_2s = None
    buy_ratio_2s = None
    if deltas:
        _, _, premier_delta_achats, premier_delta_ventes = deltas[0]
        achats_bruts_2s = premier_delta_achats - premier_delta_ventes
        total_2s = premier_delta_achats + premier_delta_ventes
        buy_ratio_2s = round(premier_delta_achats / total_2s, 3) if total_2s > 0 else None

    def _buy_ratio(fenetre_max_s):
        achats_cumules = sum(da for _, e_fin, da, _ in deltas if e_fin <= fenetre_max_s)
        ventes_cumulees = sum(dv for _, e_fin, _, dv in deltas if e_fin <= fenetre_max_s)
        total = achats_cumules + ventes_cumulees
        return round(achats_cumules / total, 3) if total else None

    buy_ratio_10s = _buy_ratio(10)
    buy_ratio_20s = _buy_ratio(ANALYSE_20S_DURATION)

    if mint in active_tokens:
        active_tokens[mint]["buy_ratio_10s"] = buy_ratio_10s
        active_tokens[mint]["buy_ratio_20s"] = buy_ratio_20s
        active_tokens[mint]["achats_bruts_2s"] = achats_bruts_2s
        active_tokens[mint]["buy_ratio_2s"] = buy_ratio_2s

    print(
        f"[analyse_20s] {data['symbol']} ({mint}) — "
        f"buy_ratio_2s={buy_ratio_2s} buy_ratio_10s={buy_ratio_10s} buy_ratio_20s={buy_ratio_20s} achats_bruts_2s={achats_bruts_2s}"
    )


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

    rug_ok, rug_score, rug_flags, top10_pct, insiders_detected, lp_locked_pct, total_holders, bundle_detected = rugcheck_verdict(mint)
    if not rug_ok:
        print(f"[rugcheck] {symbol} ({mint}) rejeté — score={rug_score} flags={rug_flags} top10={top10_pct}")
        return False

    # Ancienneté du pool au moment de l'alerte, déduite de pairCreatedAt
    # (timestamp en millisecondes fourni par DexScreener).
    pair_created_at = pair.get("pairCreatedAt")
    pool_age_seconds = None
    if pair_created_at:
        try:
            pool_age_seconds = round(time.time() - (float(pair_created_at) / 1000))
        except (TypeError, ValueError):
            pool_age_seconds = None

    # Données supplémentaires, non utilisées comme filtre (sauf si activé
    # via REJECT_TOP10_PCT / REJECT_IF_INSIDERS ci-dessus), juste pour le
    # log et la comparaison a posteriori (voir LOG_FIELDS).
    txns = pair.get("txns") or {}
    volume = pair.get("volume") or {}
    txns_m5 = txns.get("m5") or {}
    txns_h1 = txns.get("h1") or {}

    # Variation de prix sur 5 min (déjà fournie par DexScreener).
    price_change_m5 = (pair.get("priceChange") or {}).get("m5")

    # Accélération relative de l'activité : rythme des 5 dernières minutes
    # extrapolé sur 1h (x12), comparé au volume de transactions réellement
    # observé sur la dernière heure. ATTENTION : h1 inclut déjà les 5
    # dernières minutes (m5 n'est pas une période indépendante de h1), donc
    # à interpréter comme "rythme récent vs moyenne horaire incluant ce
    # rythme récent", pas comme deux fenêtres strictement disjointes.
    tot_m5 = (txns_m5.get("buys") or 0) + (txns_m5.get("sells") or 0)
    tot_h1 = (txns_h1.get("buys") or 0) + (txns_h1.get("sells") or 0)
    tx_accel = round((tot_m5 * 12) / tot_h1, 3) if tot_h1 > 0 else None

    entry_stats = {
        "liquidity_ratio": round(liquidity_usd / market_cap, 4) if market_cap else None,
        "txns_buys_m5": txns_m5.get("buys"),
        "txns_sells_m5": txns_m5.get("sells"),
        "volume_m5": volume.get("m5"),
        "txns_buys_h1": txns_h1.get("buys"),
        "txns_sells_h1": txns_h1.get("sells"),
        "volume_h1": volume.get("h1"),
        "price_change_m5": price_change_m5,
        "tx_accel": tx_accel,
    }

    flags_txt = ", ".join(rug_flags) if rug_flags else "Aucun"
    top10_txt = f"{top10_pct}%" if top10_pct is not None else "N/A"
    holders_txt = f"{total_holders}" if total_holders is not None else "N/A"
    bundle_txt = "⚠️ Oui" if bundle_detected else "Non"
    msg_ok = (
        f"✅ *Nouveau Token Solana Détecté !*\n\n"
        f"🪙 Nom : {name} ({symbol})\n"
        f"🏦 DEX : {dex_name}\n"
        f"📊 Market Cap / FDV : ${market_cap:,.0f}\n"
        f"💧 Liquidité USD : ${liquidity_usd:,.0f}\n"
        f"🛡️ RugCheck Score : {rug_score}/100\n"
        f"🚩 Flags : {flags_txt}\n"
        f"👥 Top 10 holders : {top10_txt}\n"
        f"👤 Nombre de holders : {holders_txt}\n"
        f"📦 Bundle détecté : {bundle_txt}\n"
        f"🕵️ Insiders détectés : {insiders_detected}\n"
        f"🔗 [Voir sur DexScreener]({pair_url_link})\n"
        f"⚡ [Trader sur Axiom](https://axiom.trade/meme/{mint})\n"
        f"🔍 [Voir sur RugCheck](https://rugcheck.xyz/tokens/{mint})"
    )
    send_telegram_message(msg_ok)
    print(f"Alerte envoyée pour : {symbol} ({mint}) — RugCheck score={rug_score} top10={top10_pct}%")

    active_tokens[mint] = {
        "symbol": symbol,
        "dex": dex_name,
        "initial_mc": market_cap or 1.0,
        "max_price": market_cap or 1.0,
        "liquidity_usd": liquidity_usd,
        "rugcheck_score": rug_score,
        "rugcheck_flags": flags_txt,
        "top10_pct": top10_pct,
        "insiders_detected": insiders_detected,
        "lp_locked_pct": lp_locked_pct,
        "total_holders": total_holders,
        "bundle_detected": bundle_detected,
        "pool_age_seconds": pool_age_seconds,
        "start_time": time.time(),
        "dex_url": pair_url_link,  # <-- pour le lien dans le rapport 30 min
        "entry_stats": entry_stats,  # <-- pour le log CSV à la fin du suivi
        "pool_address": pair.get("pairAddress"),  # <-- pour interroger GeckoTerminal
        "initial_price": _to_float(pair.get("priceUsd")),
    }
    seen_mints.add(mint)

    # Lance l'analyse des 20 premières secondes dans un thread séparé pour
    # ne pas bloquer la boucle principale (qui doit continuer à tourner
    # toutes les 10s pendant ce temps-là).
    threading.Thread(target=analyser_20_premieres_secondes, args=(mint,), daemon=True).start()

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
                        print(f"[monitor_ath] {data['symbol']} ({mint}) MC actuel=${current_mc:,.0f} (max enregistré=${data['max_price']:,.0f})")
                        if current_mc and current_mc > data["max_price"]:
                            active_tokens[mint]["max_price"] = current_mc
                    else:
                        print(f"[monitor_ath] {data['symbol']} ({mint}) aucune pair retournée par DexScreener")
                else:
                    print(f"[monitor_ath] {data['symbol']} ({mint}) status={res.status_code} — {res.text[:200]}")
            except Exception as e:
                print(f"Erreur monitor_ath pour {mint} : {e}")

        if elapsed >= 1800:
            initial_mc = data["initial_mc"] or 1.0

            # Vrai ATH via les bougies OHLCV GeckoTerminal (capture les pics
            # même s'ils sont passés entre deux polling classiques). On garde
            # le max avec le suivi par polling, au cas où GeckoTerminal
            # échoue ou ne connaît pas encore ce pool.
            true_ath_mc = get_true_ath_mc(
                data.get("pool_address"),
                initial_mc,
                data.get("initial_price"),
                data["start_time"],
            )
            print(f"[monitor_ath] {data['symbol']} ({mint}) GeckoTerminal ATH mc={true_ath_mc}")
            max_mc = max(active_tokens[mint]["max_price"], true_ath_mc or 0)

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
                "horodatage": time.strftime("%Y-%m-%d %H:%M:%S"),
                "mint": mint,
                "symbole": data["symbol"],
                "dex": data.get("dex"),
                "mc_initial": initial_mc,
                "mc_max": max_mc,
                "multiplicateur": round(multiplicateur, 3),
                "liquidite_usd": data.get("liquidity_usd"),
                "ratio_liquidite": entry_stats.get("liquidity_ratio"),
                "score_rugcheck": data.get("rugcheck_score"),
                "alertes_rugcheck": data.get("rugcheck_flags"),
                "pct_top10_holders": data.get("top10_pct"),
                "insiders_detectes": data.get("insiders_detected"),
                "nombre_holders": data.get("total_holders"),
                "bundle_detecte": data.get("bundle_detected"),
                "pool_age_seconds": data.get("pool_age_seconds"),
                "lp_locked_pct": data.get("lp_locked_pct"),
                "achats_m5": entry_stats.get("txns_buys_m5"),
                "ventes_m5": entry_stats.get("txns_sells_m5"),
                "volume_m5": entry_stats.get("volume_m5"),
                "achats_h1": entry_stats.get("txns_buys_h1"),
                "ventes_h1": entry_stats.get("txns_sells_h1"),
                "volume_h1": entry_stats.get("volume_h1"),
                "seconde_prix_plus_bas_20s": data.get("low_second_20s"),
                "multiplicateur_plus_bas_20s": data.get("low_mult_20s"),
                "buy_ratio_10s": data.get("buy_ratio_10s"),
                "buy_ratio_20s": data.get("buy_ratio_20s"),
                "achats_bruts_2s": data.get("achats_bruts_2s"),
                "price_change_m5": entry_stats.get("price_change_m5"),
                "tx_accel": entry_stats.get("tx_accel"),
                "buy_ratio_2s": data.get("buy_ratio_2s"),
            })

            tokens_to_remove.append(mint)

    for mint in tokens_to_remove:
        active_tokens.pop(mint, None)


if __name__ == "__main__":
    print("Bot de surveillance démarré...")
    while True:
        check_telegram_commands()
        check_new_solana_tokens()
        check_pending_tokens()
        monitor_ath()
        time.sleep(10)
