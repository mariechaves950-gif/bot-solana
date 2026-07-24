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
REJECT_TOP10_PCT = None
REJECT_IF_INSIDERS = False

# --- ANALYSE DES 20 PREMIÈRES SECONDES ---
ANALYSE_20S_SAMPLE_INTERVAL = 2   # secondes entre 2 mesures
ANALYSE_20S_DURATION = 20         # durée totale observée

# ============================================================
# --- MODULE DEV CLUSTER : tracking de l'arbre de wallets ---
# ============================================================
#
# LIMITATION IMPORTANTE À CONNAÎTRE AVANT D'UTILISER CE MODULE :
# Ce module utilise le RPC PUBLIC Solana (gratuit, sans clé), pas Helius.
# Conséquences concrètes :
#   - Rate limit bas et partagé avec tout le monde (~40 req/s en théorie,
#     bien moins en pratique) -> des erreurs/timeouts sont NORMAUX, le code
#     ci-dessous les tolère (retourne des résultats partiels plutôt que de
#     planter).
#   - Pas d'API "enhanced" : on doit parser nous-mêmes les instructions
#     brutes de chaque transaction pour repérer les transferts SOL et les
#     créations de tokens Pump.fun.
#   - La détection des "tokens créés par ce wallet" est un BEST-EFFORT basé
#     sur l'historique de signatures du wallet (limité à
#     CLUSTER_MAX_TX_HISTORIQUE transactions par wallet) : un dev très actif
#     avec un historique plus long que ça peut avoir des tokens plus anciens
#     non détectés.
#   - L'identification du wallet "créateur" (dev) d'un mint utilise l'API
#     publique de Pump.fun (frontend-api.pump.fun). Cet endpoint n'est PAS
#     officiellement documenté et peut changer sans préavis — à vérifier/
#     ajuster si ça casse en prod (teste avec un mint connu avant de
#     déployer).

SOLANA_RPC_URL = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

# Programmes système / DEX connus et fiables à exclure de l'exploration
# (ce ne sont pas des "wallets" du dev, juste des comptes de programme).
# Cette liste ne contient QUE des adresses de programmes vérifiables ; elle
# ne contient PAS d'adresses de CEX (je n'ai pas de liste à jour fiable à
# te fournir de mémoire — complète-la toi-même via des labels Solscan si tu
# veux aussi couper l'exploration sur les hot wallets d'exchanges).
BLACKLIST_ADDRESSES = {
    "11111111111111111111111111111111",           # System Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token Program
    "ComputeBudget111111111111111111111111111111",  # Compute Budget Program
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  # Pump.fun Program
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM V4
}

MAX_DEPTH = 2                     # profondeur d'exploration (aval ET amont)
CLUSTER_MIN_SOL = 0.02            # ignore les transferts SOL en dessous (poussière/frais)
CLUSTER_MAX_TX_HISTORIQUE = 200   # nb max de tx analysées par wallet (coût RPC)
CLUSTER_MAX_WALLETS = 25          # garde-fou anti-explosion combinatoire
CLUSTER_MAX_FANOUT = 15           # au-delà, un wallet est probablement un hub/CEX -> on ignore ses destinataires
MAX_CLUSTER_THREADS_PARALLELES = 2  # limite le nb d'explorations de cluster en même temps

_cluster_semaphore = threading.Semaphore(MAX_CLUSTER_THREADS_PARALLELES)

_rpc_id_counter = 0
_rpc_id_lock = threading.Lock()


def _next_rpc_id():
    global _rpc_id_counter
    with _rpc_id_lock:
        _rpc_id_counter += 1
        return _rpc_id_counter


def rpc_call(method, params, retries=2, timeout=15):
    """
    Appel générique au RPC Solana public, avec un léger retry (le RPC public
    renvoie souvent des erreurs 429/timeout sous charge). Retourne le champ
    "result" de la réponse, ou None en cas d'échec après tous les retries.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": _next_rpc_id(),
        "method": method,
        "params": params,
    }
    for tentative in range(retries + 1):
        try:
            res = requests.post(SOLANA_RPC_URL, json=payload, timeout=timeout)
            if res.status_code == 200:
                data = res.json()
                if "error" in data:
                    print(f"[rpc] {method} erreur RPC : {data['error']}")
                    return None
                return data.get("result")
            if res.status_code == 429:
                time.sleep(1.5 * (tentative + 1))  # backoff sur rate limit
                continue
            print(f"[rpc] {method} status={res.status_code}")
        except Exception as e:
            print(f"[rpc] {method} exception : {e}")
        time.sleep(0.5 * (tentative + 1))
    return None


def get_signatures_for_address(address, limit=CLUSTER_MAX_TX_HISTORIQUE):
    result = rpc_call(
        "getSignaturesForAddress",
        [address, {"limit": limit}],
    )
    return result or []


def get_transaction(signature):
    return rpc_call(
        "getTransaction",
        [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
    )


def extraire_transferts_sol(tx):
    """
    Parcourt les instructions "parsed" d'une transaction et retourne la
    liste des transferts SOL natifs (System Program "transfer"), sous la
    forme [(source, destination, montant_sol), ...]. Ignore les transferts
    de tokens SPL (pas pertinents pour retracer le financement en gas).
    """
    transferts = []
    if not tx:
        return transferts
    try:
        message = (tx.get("transaction") or {}).get("message") or {}
        instructions = message.get("instructions") or []
        for ix in instructions:
            parsed = ix.get("parsed")
            if not parsed or ix.get("program") != "system":
                continue
            if parsed.get("type") != "transfer":
                continue
            info = parsed.get("info") or {}
            source = info.get("source")
            destination = info.get("destination")
            lamports = info.get("lamports", 0)
            if source and destination and lamports:
                transferts.append((source, destination, lamports / 1_000_000_000))
    except Exception as e:
        print(f"[extraire_transferts_sol] erreur : {e}")
    return transferts


def trouver_wallet_financeur(wallet):
    """
    Remonte l'historique COMPLET disponible (jusqu'à CLUSTER_MAX_TX_HISTORIQUE
    tx) du wallet pour trouver le tout premier transfert SOL ENTRANT reçu
    -> c'est le "funding origin" (le wallet qui a payé le gas initial).
    getSignaturesForAddress renvoie du plus récent au plus ancien : on part
    donc de la fin de la liste pour retomber sur les transactions les plus
    anciennes en premier.
    Retourne (adresse_financeur, montant_sol, signature, block_time) ou
    (None, None, None, None) si rien trouvé.
    """
    sigs = get_signatures_for_address(wallet)
    if not sigs:
        return None, None, None, None

    for sig_info in reversed(sigs):  # du plus ancien au plus récent
        tx = get_transaction(sig_info.get("signature"))
        for source, destination, montant in extraire_transferts_sol(tx):
            if destination == wallet and source != wallet and montant >= CLUSTER_MIN_SOL:
                return source, montant, sig_info.get("signature"), sig_info.get("blockTime")
    return None, None, None, None


def trouver_sous_wallets(wallet):
    """
    Parcourt l'historique du wallet et agrège tous les transferts SOL
    SORTANTS par destinataire (montant total envoyé), en excluant la
    blacklist et les montants sous CLUSTER_MIN_SOL.
    Retourne une liste de (destinataire, montant_total_sol), triée par
    montant décroissant. Si le wallet a envoyé à plus de CLUSTER_MAX_FANOUT
    adresses différentes, on considère que c'est probablement un hub/CEX et
    on retourne une liste vide (pour ne pas exploser l'arbre).
    """
    sigs = get_signatures_for_address(wallet)
    totaux = {}
    for sig_info in sigs:
        tx = get_transaction(sig_info.get("signature"))
        for source, destination, montant in extraire_transferts_sol(tx):
            if source != wallet or destination == wallet:
                continue
            if destination in BLACKLIST_ADDRESSES or montant < CLUSTER_MIN_SOL:
                continue
            totaux[destination] = totaux.get(destination, 0) + montant

    if len(totaux) > CLUSTER_MAX_FANOUT:
        print(f"[trouver_sous_wallets] {wallet} a >{CLUSTER_MAX_FANOUT} destinataires -> probable hub, ignoré")
        return []

    return sorted(totaux.items(), key=lambda x: x[1], reverse=True)


def explorer_arbre_aval(wallet, profondeur_restante, wallets_deja_vus):
    """
    Descend récursivement l'arbre de distribution (dev -> sous-wallets ->
    sous-sous-wallets...) jusqu'à max_depth, avec un garde-fou sur le
    nombre total de wallets explorés (CLUSTER_MAX_WALLETS).
    Retourne une liste de dicts {"wallet":..., "montant_recu":..., "niveau":...}.
    """
    resultat = []
    if profondeur_restante < 0 or len(wallets_deja_vus) >= CLUSTER_MAX_WALLETS:
        return resultat

    sous_wallets = trouver_sous_wallets(wallet)
    for dest, montant in sous_wallets:
        if len(wallets_deja_vus) >= CLUSTER_MAX_WALLETS:
            break
        if dest in wallets_deja_vus:
            continue
        wallets_deja_vus.add(dest)
        niveau = MAX_DEPTH - profondeur_restante + 1
        resultat.append({"wallet": dest, "montant_recu": round(montant, 3), "niveau": niveau})
        if profondeur_restante > 0:
            resultat.extend(explorer_arbre_aval(dest, profondeur_restante - 1, wallets_deja_vus))

    return resultat


def get_pump_fun_creator(mint):
    """
    Récupère l'adresse du wallet créateur (dev) d'un token via l'API
    publique de Pump.fun. ATTENTION : endpoint non officiel, à vérifier
    avant mise en prod (teste avec un mint connu). Retourne None en cas
    d'échec, plutôt que de faire planter le reste du pipeline.
    """
    try:
        url = f"https://frontend-api-v3.pump.fun/coins-v2/{mint}"
        res = requests.get(url, timeout=8, headers={"Accept": "application/json"})
        if res.status_code != 200:
            print(f"[pump_fun_creator] status={res.status_code} pour {mint}")
            return None
        data = res.json()
        return data.get("creator")
    except Exception as e:
        print(f"[pump_fun_creator] erreur pour {mint} : {e}")
        return None


def trouver_tokens_crees_par_wallet(wallet):
    """
    BEST-EFFORT : parcourt l'historique de signatures du wallet et détecte
    les transactions qui incluent une instruction "create" du programme
    Pump.fun, pour en extraire les mints créés par ce wallet. Limité aux
    CLUSTER_MAX_TX_HISTORIQUE dernières transactions du wallet.
    Retourne une liste de mints (str).
    """
    mints_crees = []
    sigs = get_signatures_for_address(wallet)
    for sig_info in sigs:
        tx = get_transaction(sig_info.get("signature"))
        if not tx:
            continue
        try:
            message = (tx.get("transaction") or {}).get("message") or {}
            account_keys = message.get("accountKeys") or []
            programmes_impliques = {
                (ak.get("pubkey") if isinstance(ak, dict) else ak) for ak in account_keys
            }
            if "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" not in programmes_impliques:
                continue
            # Le mint créé est généralement le premier "post token balance"
            # nouvellement apparu dans cette transaction.
            meta = tx.get("meta") or {}
            post_balances = meta.get("postTokenBalances") or []
            pre_mints = {b.get("mint") for b in (meta.get("preTokenBalances") or [])}
            for b in post_balances:
                mint = b.get("mint")
                if mint and mint not in pre_mints:
                    mints_crees.append(mint)
        except Exception as e:
            print(f"[trouver_tokens_crees_par_wallet] erreur parsing tx : {e}")
    return list(dict.fromkeys(mints_crees))  # dédoublonne en gardant l'ordre


def calculer_multiplicateur_token(mint):
    """
    Calcule un multiplicateur (mc_max / mc_initial) best-effort pour un
    ancien token, via DexScreener (pool + prix courant) + GeckoTerminal
    (bougies OHLCV depuis la création, pour retrouver le prix initial et le
    plus haut réel). Retourne None si les données sont insuffisantes.
    """
    pair = fetch_pair_data(mint)
    if not pair:
        return None
    pool_address = pair.get("pairAddress")
    price_actuel = _to_float(pair.get("priceUsd"))
    if not pool_address or not price_actuel:
        return None

    try:
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/hour"
        params = {"aggregate": 1, "limit": 1000, "currency": "usd", "token": "base"}
        res = requests.get(url, params=params, timeout=8)
        if res.status_code != 200:
            return None
        ohlcv_list = res.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        if not ohlcv_list:
            return None
        # ohlcv_list est du plus récent au plus ancien selon l'API GeckoTerminal
        prix_initial = ohlcv_list[-1][1]  # "open" de la bougie la plus ancienne dispo
        prix_max = max(candle[2] for candle in ohlcv_list if len(candle) >= 3)
        if not prix_initial:
            return None
        return round(prix_max / prix_initial, 3)
    except Exception as e:
        print(f"[calculer_multiplicateur_token] erreur pour {mint} : {e}")
        return None


def analyser_dev_cluster(mint, dev_wallet):
    """
    Tourne dans un THREAD SÉPARÉ (limité à MAX_CLUSTER_THREADS_PARALLELES en
    parallèle via _cluster_semaphore, pour ne pas cramer le RPC public si
    plusieurs tokens sont alertés proches dans le temps).

    1. Remonte le financeur du wallet dev (amont, 1 niveau)
    2. Descend l'arbre de distribution du wallet dev (aval, jusqu'à MAX_DEPTH)
    3. Pour le wallet dev + chaque sous-wallet trouvé, cherche les tokens
       déjà créés (best-effort) et calcule leur multiplicateur
    4. Envoie un message Telegram récapitulatif
    5. Stocke les stats agrégées dans active_tokens[mint] pour qu'elles
       soient reprises dans le CSV au moment du rapport 30 min
    """
    with _cluster_semaphore:
        try:
            print(f"[cluster] début exploration pour dev={dev_wallet} (token {mint})")

            financeur, montant_financeur, _, _ = trouver_wallet_financeur(dev_wallet)

            wallets_vus = {dev_wallet}
            arbre_aval = explorer_arbre_aval(dev_wallet, MAX_DEPTH - 1, wallets_vus)
            tous_les_wallets = [dev_wallet] + [w["wallet"] for w in arbre_aval]

            tous_les_mints = []
            for w in tous_les_wallets:
                tous_les_mints.extend(trouver_tokens_crees_par_wallet(w))
            tous_les_mints = list(dict.fromkeys(tous_les_mints))
            tous_les_mints = [m for m in tous_les_mints if m != mint]  # exclut le token qui vient d'être alerté

            multiplicateurs = []
            for m in tous_les_mints:
                mult = calculer_multiplicateur_token(m)
                if mult is not None:
                    multiplicateurs.append(mult)

            mult_moyen = round(sum(multiplicateurs) / len(multiplicateurs), 2) if multiplicateurs else None
            mult_max = round(max(multiplicateurs), 2) if multiplicateurs else None
            mult_min = round(min(multiplicateurs), 2) if multiplicateurs else None

            # Stocke pour le CSV (repris dans monitor_ath au moment du rapport 30 min)
            if mint in active_tokens:
                active_tokens[mint]["cluster_wallet_count"] = len(tous_les_wallets)
                active_tokens[mint]["cluster_dev_wallet"] = dev_wallet
                active_tokens[mint]["cluster_funding_wallet"] = financeur
                active_tokens[mint]["cluster_tokens_historiques"] = len(tous_les_mints)
                active_tokens[mint]["cluster_mult_moyen"] = mult_moyen
                active_tokens[mint]["cluster_mult_max"] = mult_max
                active_tokens[mint]["cluster_mult_min"] = mult_min

            # --- Message Telegram récapitulatif ---
            financeur_txt = f"`{financeur[:4]}...{financeur[-4:]}`" if financeur else "Non trouvé"
            financeur_montant_txt = f" ({montant_financeur:.2f} SOL)" if montant_financeur else ""

            sous_wallets_txt = ""
            for w in arbre_aval[:8]:  # limite l'affichage pour rester lisible sur mobile
                addr = w["wallet"]
                sous_wallets_txt += f"   • `{addr[:4]}...{addr[-4:]}` → {w['montant_recu']} SOL (niv. {w['niveau']})\n"
            if len(arbre_aval) > 8:
                sous_wallets_txt += f"   … et {len(arbre_aval) - 8} autre(s)\n"
            if not sous_wallets_txt:
                sous_wallets_txt = "   Aucun détecté\n"

            stats_txt = "Aucun token historique détecté (best-effort, historique limité)"
            if multiplicateurs:
                stats_txt = (
                    f"{len(tous_les_mints)} token(s) détecté(s) — "
                    f"moyen x{mult_moyen} / max x{mult_max} / min x{mult_min}"
                )

            msg = (
                f"🕵️ *Analyse Dev Cluster*\n\n"
                f"👤 Wallet dev : `{dev_wallet[:4]}...{dev_wallet[-4:]}`\n"
                f"💰 Financé par : {financeur_txt}{financeur_montant_txt}\n\n"
                f"📊 Sous-wallets détectés : {len(arbre_aval)}\n"
                f"{sous_wallets_txt}\n"
                f"📈 Historique du cluster : {stats_txt}\n\n"
                f"🔗 [Voir le wallet dev sur Solscan](https://solscan.io/account/{dev_wallet})"
            )
            send_telegram_message(msg)
            print(f"[cluster] terminé pour {mint} — {len(tous_les_wallets)} wallets, {len(tous_les_mints)} tokens historiques")

        except Exception as e:
            print(f"[analyser_dev_cluster] erreur pour {mint} : {e}")


def lancer_analyse_cluster_si_possible(mint):
    """
    Point d'entrée appelé depuis essayer_alerter(). Récupère d'abord le
    wallet dev (via l'API Pump.fun) de façon synchrone rapide, puis lance
    l'exploration complète (potentiellement longue) dans un thread séparé
    pour ne jamais bloquer la boucle principale.
    """
    def _job():
        dev_wallet = get_pump_fun_creator(mint)
        if not dev_wallet:
            print(f"[cluster] impossible de trouver le wallet dev pour {mint}, analyse annulée")
            return
        analyser_dev_cluster(mint, dev_wallet)

    threading.Thread(target=_job, daemon=True).start()


# ============================================================
# --- FIN MODULE DEV CLUSTER ---
# ============================================================


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
    # --- colonnes dev cluster ---
    "cluster_dev_wallet", "cluster_funding_wallet", "cluster_wallet_count",
    "cluster_tokens_historiques", "cluster_mult_moyen", "cluster_mult_max", "cluster_mult_min",
    # --- colonnes boost DexScreener ---
    "boost_detecte", "nombre_boosts_actifs",
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
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def extraire_infos_boost(pair):
    """
    Extrait les infos de boost DexScreener d'une pair. DexScreener renvoie
    un champ "boosts": {"active": N} quand le token a des boosts payants
    actifs (mise en avant sur leur plateforme). Retourne (bool, int).
    """
    boosts_info = pair.get("boosts") or {}
    nombre_boosts_actifs = boosts_info.get("active", 0) or 0
    boost_detecte = nombre_boosts_actifs > 0
    return boost_detecte, nombre_boosts_actifs


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


RUGCHECK_MAX_SCORE = 20


def _extraire_concentration_holders(data):
    top_holders = data.get("topHolders") or []
    top10_pct = None
    if top_holders:
        try:
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
    if not pool_address or not initial_price:
        return None
    try:
        elapsed_minutes = max(int((time.time() - start_time) / 60) + 5, 10)
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/minute"
        params = {"aggregate": 1, "limit": min(elapsed_minutes, 1000), "currency": "usd", "token": "base"}
        res = requests.get(url, params=params, timeout=8)
        if res.status_code != 200:
            print(f"[geckoterminal] status={res.status_code} pour {pool_address}")
            return None
        ohlcv_list = res.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        if not ohlcv_list:
            return None
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
            send_telegram_message("Commandes disponibles :\n/csv — télécharger le fichier de log complet")


def fetch_pair_data(mint):
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
    data = active_tokens.get(mint)
    if not data:
        return
    initial_mc = data.get("initial_mc") or 1.0
    start = data["start_time"]
    samples = []

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

    deltas = []
    for (e_prev, _, b_prev, s_prev), (e_next, _, b_next, s_next) in zip(samples, samples[1:]):
        if None in (b_prev, s_prev, b_next, s_next):
            continue
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

    pair_created_at = pair.get("pairCreatedAt")
    pool_age_seconds = None
    if pair_created_at:
        try:
            pool_age_seconds = round(time.time() - (float(pair_created_at) / 1000))
        except (TypeError, ValueError):
            pool_age_seconds = None

    txns = pair.get("txns") or {}
    volume = pair.get("volume") or {}
    txns_m5 = txns.get("m5") or {}
    txns_h1 = txns.get("h1") or {}
    price_change_m5 = (pair.get("priceChange") or {}).get("m5")

    tot_m5 = (txns_m5.get("buys") or 0) + (txns_m5.get("sells") or 0)
    tot_h1 = (txns_h1.get("buys") or 0) + (txns_h1.get("sells") or 0)
    tx_accel = round((tot_m5 * 12) / tot_h1, 3) if tot_h1 > 0 else None

    # --- Détection boost DexScreener ---
    boost_detecte, nombre_boosts_actifs = extraire_infos_boost(pair)

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
    boost_txt = f"⚡ Oui ({nombre_boosts_actifs})" if boost_detecte else "Non"
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
        f"🚀 Boosté DexScreener : {boost_txt}\n"
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
        "dex_url": pair_url_link,
        "entry_stats": entry_stats,
        "pool_address": pair.get("pairAddress"),
        "initial_price": _to_float(pair.get("priceUsd")),
        # champs boost DexScreener (mis à jour aussi dans monitor_ath si le
        # boost apparaît après coup, pas seulement à l'alerte initiale)
        "boost_detecte": boost_detecte,
        "nombre_boosts_actifs": nombre_boosts_actifs,
        # champs cluster, remplis plus tard en arrière-plan (peuvent rester
        # à None si l'analyse cluster n'a pas terminé avant le rapport 30 min)
        "cluster_dev_wallet": None,
        "cluster_funding_wallet": None,
        "cluster_wallet_count": None,
        "cluster_tokens_historiques": None,
        "cluster_mult_moyen": None,
        "cluster_mult_max": None,
        "cluster_mult_min": None,
    }
    seen_mints.add(mint)

    threading.Thread(target=analyser_20_premieres_secondes, args=(mint,), daemon=True).start()

    # Lance l'analyse du dev cluster en arrière-plan (thread séparé, limité
    # par _cluster_semaphore) -> ne bloque jamais la boucle principale.
    lancer_analyse_cluster_si_possible(mint)

    return True


def check_new_solana_tokens():
    try:
        url = "https://api.dexscreener.com/token-profiles/latest/v1"
        response = requests.get(url, timeout=10)
        print(f"[debug] status={response.status_code}")

        if response.status_code != 200:
            return

        data = response.json()
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
                pending_mints[mint] = time.time()
                continue

            essayer_alerter(mint, pair, source_url)

    except Exception as e:
        print(f"Erreur lors de la vérification DexScreener : {e}")


PENDING_CHECK_INTERVAL = 30
_last_pending_check = 0


def check_pending_tokens():
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
            continue

        essayer_alerter(mint, pair, f"https://dexscreener.com/solana/{mint}")
        to_remove.append(mint)

    for mint in to_remove:
        pending_mints.pop(mint, None)


PRICE_CHECK_INTERVAL = 120
_last_price_check = 0


def monitor_ath():
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

                        # Le boost peut apparaître après l'alerte initiale :
                        # on met à jour le statut à chaque vérification prix
                        # si un boost devient actif (on ne le "désactive"
                        # jamais automatiquement, un boost déjà vu compte).
                        boost_detecte_maj, nb_boosts_maj = extraire_infos_boost(pairs[0])
                        if boost_detecte_maj:
                            active_tokens[mint]["boost_detecte"] = True
                            active_tokens[mint]["nombre_boosts_actifs"] = max(
                                nb_boosts_maj, active_tokens[mint].get("nombre_boosts_actifs", 0) or 0
                            )
                    else:
                        print(f"[monitor_ath] {data['symbol']} ({mint}) aucune pair retournée par DexScreener")
                else:
                    print(f"[monitor_ath] {data['symbol']} ({mint}) status={res.status_code} — {res.text[:200]}")
            except Exception as e:
                print(f"Erreur monitor_ath pour {mint} : {e}")

        if elapsed >= 1800:
            initial_mc = data["initial_mc"] or 1.0

            true_ath_mc = get_true_ath_mc(
                data.get("pool_address"), initial_mc, data.get("initial_price"), data["start_time"],
            )
            print(f"[monitor_ath] {data['symbol']} ({mint}) GeckoTerminal ATH mc={true_ath_mc}")
            max_mc = max(active_tokens[mint]["max_price"], true_ath_mc or 0)

            multiplicateur = max_mc / initial_mc
            dex_url = data.get("dex_url", f"https://dexscreener.com/solana/{mint}")

            boost_txt_rapport = (
                f"Oui ({data.get('nombre_boosts_actifs', 0)})"
                if data.get("boost_detecte") else "Non"
            )

            msg_rapport = (
                f"📋 *Rapport 30 min*\n\n"
                f"🪙 Token : {data['symbol']}\n"
                f"💰 Market Cap initial (à la migration) : ${initial_mc:,.0f}\n"
                f"🏆 Market Cap max atteint : ${max_mc:,.0f}\n"
                f"✖️ Multiplicateur : x{multiplicateur:,.2f}\n"
                f"🚀 Boosté : {boost_txt_rapport}\n"
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
                # --- stats dev cluster (peuvent être None si l'analyse
                # arrière-plan n'a pas terminé avant les 30 min) ---
                "cluster_dev_wallet": data.get("cluster_dev_wallet"),
                "cluster_funding_wallet": data.get("cluster_funding_wallet"),
                "cluster_wallet_count": data.get("cluster_wallet_count"),
                "cluster_tokens_historiques": data.get("cluster_tokens_historiques"),
                "cluster_mult_moyen": data.get("cluster_mult_moyen"),
                "cluster_mult_max": data.get("cluster_mult_max"),
                "cluster_mult_min": data.get("cluster_mult_min"),
                # --- stats boost DexScreener ---
                "boost_detecte": data.get("boost_detecte", False),
                "nombre_boosts_actifs": data.get("nombre_boosts_actifs", 0),
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
