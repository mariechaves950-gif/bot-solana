import os
import re
import csv
import html
import time
import threading
import requests
from datetime import datetime

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
MIN_LIQUIDITY_RATIO = 0.03    # liquidité doit représenter au moins 3% du market cap (si market cap connu)
# MIN_MARKET_CAP a été retiré : le bot ne filtre plus par Market Cap, tous
# les tokens sont traités sans exception, quel que soit leur Market Cap.

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
# --- FILTRES DE TRIGGER (signal d'entrée optimisé) ---
# ============================================================
# Ces critères s'ajoutent AUX filtres de qualité ci-dessus (liquidité,
# RugCheck...). Ils ne les remplacent pas.
#
# IMPORTANT : mc_initial / price_change_m5 / tx_accel sont disponibles AU
# MOMENT DE L'ALERTE (donnée DexScreener au moment de la détection), donc
# ils filtrent AVANT l'envoi de l'alerte Telegram.
#
# buy_ratio_20s, en revanche, n'existe QU'APRÈS 20 secondes de suivi
# post-alerte (calculé dans analyser_20_premieres_secondes). Il ne peut
# donc PAS bloquer l'alerte elle-même — il sert de condition de
# confirmation pour ouvrir une SIMULATION de position (SL/TP), une fois les
# 20 premières secondes de trading écoulées.
# TRIGGER_MC_MIN / TRIGGER_MC_MAX ont été retirés : plus aucun filtre de
# Market Cap n'est appliqué, ni en filtre de qualité ni en filtre de trigger.
TRIGGER_PRICE_CHANGE_M5_MIN = 10
TRIGGER_PRICE_CHANGE_M5_MAX = 50
TRIGGER_TX_ACCEL_MIN = 1.2
TRIGGER_BUY_RATIO_20S_MIN = 0.55


def passe_filtres_triggers(market_cap, price_change_m5, tx_accel):
    """
    Filtre de "signal d'entrée" additionnel, appliqué avant l'alerte, en
    complément des filtres de qualité existants (liquidité/RugCheck).
    NOTE : le filtre de Market Cap (TRIGGER_MC_MIN/TRIGGER_MC_MAX) a été
    retiré à la demande de l'utilisateur — tous les tokens sont traités
    sans exception, quel que soit leur Market Cap.
    """
    if price_change_m5 is None or not (TRIGGER_PRICE_CHANGE_M5_MIN < price_change_m5 < TRIGGER_PRICE_CHANGE_M5_MAX):
        return False
    if tx_accel is None or tx_accel <= TRIGGER_TX_ACCEL_MIN:
        return False
    return True


# ============================================================
# --- SIMULATION SL/TP (AUCUN ORDRE RÉEL — TRACKING UNIQUEMENT) ---
# ============================================================
# Le bot n'a ni wallet ni intégration de swap : il n'achète ni ne vend
# jamais réellement. Ce qui suit simule ce qui SE SERAIT PASSÉ si une
# position avait été prise au prix observé à la fin des 20 premières
# secondes (uniquement si le signal est validé, cf. TRIGGER_BUY_RATIO_20S_MIN
# ci-dessus). Utile pour évaluer la stratégie a posteriori via le CSV.
SIMULATION_SL_PCT = -0.25              # stop-loss initial : -25% du prix d'entrée simulé
SIMULATION_TP1_MULT = 2.0              # take-profit 1 : x2 -> vend 50% (simulé), SL remonté au breakeven
SIMULATION_TP2_MULT = 3.0              # take-profit 2 : x3 -> active un trailing stop sur le solde
SIMULATION_TRAILING_APRES_TP2_PCT = -0.20  # trailing stop -20% sous le plus haut, sur le solde après TP2

# --- LIMITE DE TEMPS CONDITIONNELLE (Time-based Exit) ---
# Si une position simulée reste "ouverte" (aucun SL, aucun TP1 déclenché)
# plus de MAX_HOLD_TIME_MINUTES ET que le prix reste dans une fourchette
# neutre par rapport au prix d'entrée, on clôture automatiquement 100% de
# la position au marché (vente simulée, aucun ordre réel à annuler puisque
# le bot ne passe jamais d'ordre réel).
MAX_HOLD_TIME_MINUTES = 60             # durée max avant d'envisager une sortie sur temps
TIME_EXIT_RATIO_MIN = 1 + (-0.10)      # borne basse de la fourchette neutre : -10% du prix d'entrée
TIME_EXIT_RATIO_MAX = 1 + 0.20         # borne haute de la fourchette neutre : +20% du prix d'entrée

# ============================================================
# --- PARAMÈTRES D'OPTIMISATION (analyse token_log4 à token_log9) ---
# ============================================================
# Statut par point :
#   1. TP1 partiel + Trailing Stop dès +20%  -> NON câblé (constantes prêtes)
#   2. Fallback NaN sur buy_ratio_20s        -> CÂBLÉ (voir analyser_20_premieres_secondes)
#   3. Fenêtre d'âge de pool idéale          -> NON câblé (constantes prêtes)
#   4. Bonus crédibilité (profil + boost)    -> NON câblé (constantes prêtes)

# --- 1. TP1 partiel + Trailing Stop dès +20% (à câbler dans
#     gerer_simulation_position, en remplacement/complément de
#     SIMULATION_TP1_MULT et SIMULATION_TRAILING_APRES_TP2_PCT) ---
TP1_PARTIAL_GAIN_PCT = 0.35            # gain déclenchant le TP1 partiel (35% — à arbitrer entre 0.35 et 0.50)
TP1_PARTIAL_SELL_RATIO = 0.5           # part de la position vendue au TP1 (50%)
TRAILING_ACTIVATION_GAIN_PCT = 0.20    # gain à partir duquel le SL est remonté au breakeven (+20%)

# --- 2. Fallback NaN sur buy_ratio_20s (câblé dans
#     analyser_20_premieres_secondes, avant le calcul de signal_valide) ---
FALLBACK_BUY_RATIO_ENABLED = True      # active le fallback si buy_ratio_20s est None
FALLBACK_BUY_RATIO_MIN = 0.55          # seuil appliqué au ratio de repli (achats_m5 / (achats_m5+ventes_m5))
# NOTE : ce ratio de repli est mesuré sur une fenêtre différente (activité
# de marché AVANT/à l'alerte, via DexScreener) de buy_ratio_20s (mesuré
# APRÈS l'alerte, par polling direct) — ce n'est qu'un proxy, pas une
# donnée strictement équivalente.

# --- 3. Fenêtre d'âge de pool idéale (à câbler dans essayer_alerter ou
#     passe_filtres_triggers, en filtre strict ou en scoring pondéré) ---
POOL_AGE_IDEAL_MAX_SECONDS = 600       # fenêtre d'opportunité "pool jeune" (< 600s)
POOL_AGE_STRICT_FILTER = False         # False = scoring pondéré (recommandé), True = rejet strict au-delà du seuil

# --- 4. Bonus de score pour profil DexScreener + boost combinés (à câbler
#     dans essayer_alerter, en assouplissement des seuils micro-structure
#     quand ce bonus est actif) ---
CREDIBILITY_BONUS_ENABLED = True       # active le bonus profil + boost
CREDIBILITY_BONUS_REQUIRES_BOTH = True # bonus décisif seulement si profil ET boost sont vrais simultanément


def gerer_simulation_position(mint, current_mc, elapsed_seconds):
    """
    Fait évoluer l'état d'une position SIMULÉE (aucun ordre réel) en
    fonction du market cap courant. Appelée à chaque vérification de prix
    (monitor_ath) pour les tokens dont le signal a été validé.
    """
    data = active_tokens.get(mint)
    if not data or not current_mc:
        return

    statut = data.get("position_statut")
    if statut not in ("ouverte", "tp1", "trailing"):
        return  # pas de position simulée active (en attente, non validée, ou déjà clôturée)

    entree = data.get("prix_entree_simule")
    if not entree:
        return
    ratio = current_mc / entree

    if statut == "ouverte":
        if current_mc <= data["sl_prix_simule"]:
            data["position_statut"] = "stop_loss"
            data["resultat_pct_simule"] = round((data["sl_prix_simule"] / entree - 1) * 100, 2)
            print(f"[simulation] {data['symbol']} — SL touché, position simulée clôturée ({SIMULATION_SL_PCT*100:.0f}%)")
            return
        if ratio >= SIMULATION_TP1_MULT:
            data["tp1_atteint"] = True
            data["time_to_2x"] = round(elapsed_seconds)
            data["sl_prix_simule"] = entree  # SL remonté au breakeven sur le solde
            data["position_statut"] = "tp1"
            print(f"[simulation] {data['symbol']} — TP1 (x2) touché à {elapsed_seconds:.0f}s, 50% vendus (simulé), SL -> breakeven")
            return

        # --- Time-based Exit : ni SL ni TP1 déclenché à ce stade ---
        entry_time = data.get("entry_time")
        if entry_time and MAX_HOLD_TIME_MINUTES:
            hold_minutes = (time.time() - entry_time) / 60
            if hold_minutes >= MAX_HOLD_TIME_MINUTES and TIME_EXIT_RATIO_MIN <= ratio <= TIME_EXIT_RATIO_MAX:
                data["position_statut"] = "time_exit"
                data["resultat_pct_simule"] = round((ratio - 1) * 100, 2)
                print(
                    f"[simulation] {data['symbol']} — Time-based Exit après {hold_minutes:.0f} min "
                    f"(prix neutre, {(ratio - 1) * 100:+.1f}%), 100% vendus (simulé), ordres associés annulés"
                )
                return

    elif statut == "tp1":
        if current_mc <= data["sl_prix_simule"]:
            data["position_statut"] = "breakeven"
            data["resultat_pct_simule"] = 0.0
            print(f"[simulation] {data['symbol']} — Breakeven touché après TP1, solde clôturé (simulé)")
            return
        if ratio >= SIMULATION_TP2_MULT:
            data["tp2_atteint"] = True
            data["time_to_3x"] = round(elapsed_seconds)
            data["position_statut"] = "trailing"
            data["max_price_apres_tp2"] = current_mc
            print(f"[simulation] {data['symbol']} — TP2 (x3) touché à {elapsed_seconds:.0f}s, trailing -20% activé (simulé)")

    elif statut == "trailing":
        if current_mc > data.get("max_price_apres_tp2", current_mc):
            data["max_price_apres_tp2"] = current_mc
        seuil_trailing = data["max_price_apres_tp2"] * (1 + SIMULATION_TRAILING_APRES_TP2_PCT)
        if current_mc <= seuil_trailing:
            data["position_statut"] = "trailing_stop"
            data["resultat_pct_simule"] = round((current_mc / entree - 1) * 100, 2)
            print(f"[simulation] {data['symbol']} — Trailing stop touché après TP2, solde clôturé (simulé)")


# ============================================================
# --- PRIX SOL/USD (pour convertir des volumes USD en équivalent SOL) ---
# ============================================================
_sol_price_cache = {"prix": None, "ts": 0}
SOL_PRICE_CACHE_TTL = 300  # 5 minutes


def get_sol_usd_price():
    now = time.time()
    if _sol_price_cache["prix"] and (now - _sol_price_cache["ts"]) < SOL_PRICE_CACHE_TTL:
        return _sol_price_cache["prix"]
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {"ids": "solana", "vs_currencies": "usd"}
        res = requests.get(url, params=params, timeout=8)
        if res.status_code == 200:
            prix = res.json().get("solana", {}).get("usd")
            if prix:
                _sol_price_cache["prix"] = prix
                _sol_price_cache["ts"] = now
                return prix
    except Exception as e:
        print(f"[get_sol_usd_price] erreur : {e}")
    return _sol_price_cache["prix"]  # dernier prix connu (ou None si jamais récupéré)


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
#     publique de Pump.fun (frontend-api-v3.pump.fun). Cet endpoint n'est
#     PAS officiellement documenté et peut changer sans préavis — à
#     vérifier/ajuster si ça casse en prod (teste avec un mint connu avant
#     de déployer).

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


def explorer_chaine_amont(wallet, profondeur_restante, wallets_deja_vus):
    """
    Remonte récursivement la chaîne de FINANCEMENT (qui a payé le gas de
    qui) jusqu'à MAX_DEPTH niveaux en amont, symétriquement à
    explorer_arbre_aval qui descend l'arbre de distribution. Chaque wallet
    de la chaîne est ajouté à wallets_deja_vus pour éviter les boucles et
    les doublons avec l'exploration en aval.
    Retourne une liste de dicts {"wallet":..., "montant_recu":..., "niveau":...}
    où "niveau" est une chaîne du type "amont-1", "amont-2", etc.
    """
    resultat = []
    if profondeur_restante < 0 or len(wallets_deja_vus) >= CLUSTER_MAX_WALLETS:
        return resultat

    financeur, montant, _, _ = trouver_wallet_financeur(wallet)
    if not financeur or financeur in wallets_deja_vus:
        return resultat

    wallets_deja_vus.add(financeur)
    niveau_num = MAX_DEPTH - profondeur_restante
    resultat.append({
        "wallet": financeur,
        "montant_recu": round(montant, 3) if montant else None,
        "niveau": f"amont-{niveau_num}",
    })
    if profondeur_restante > 0:
        resultat.extend(explorer_chaine_amont(financeur, profondeur_restante - 1, wallets_deja_vus))

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
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://pump.fun/",
        }
        res = requests.get(url, timeout=8, headers=headers)
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

            wallets_vus = {dev_wallet}

            # Exploration en aval (qui le dev a financé) ET en amont (qui a
            # financé le dev, et ainsi de suite), sur la même profondeur
            # MAX_DEPTH des deux côtés. wallets_vus est partagé pour éviter
            # qu'un même wallet soit exploré deux fois (ex: si un wallet
            # amont est aussi un destinataire aval d'un autre sous-wallet).
            arbre_aval = explorer_arbre_aval(dev_wallet, MAX_DEPTH - 1, wallets_vus)
            chaine_amont = explorer_chaine_amont(dev_wallet, MAX_DEPTH - 1, wallets_vus)

            financeur = chaine_amont[0]["wallet"] if chaine_amont else None
            montant_financeur = chaine_amont[0]["montant_recu"] if chaine_amont else None

            tous_les_wallets = (
                [dev_wallet]
                + [w["wallet"] for w in arbre_aval]
                + [w["wallet"] for w in chaine_amont]
            )

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

            amont_txt = ""
            for w in chaine_amont[:8]:
                addr = w["wallet"]
                montant_txt = f"{w['montant_recu']} SOL" if w["montant_recu"] else "montant inconnu"
                amont_txt += f"   • `{addr[:4]}...{addr[-4:]}` ({montant_txt}, {w['niveau']})\n"
            if len(chaine_amont) > 8:
                amont_txt += f"   … et {len(chaine_amont) - 8} autre(s)\n"
            if not amont_txt:
                amont_txt = "   Aucun détecté\n"

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
                f"🔺 Chaîne de financement en amont : {len(chaine_amont)}\n"
                f"{amont_txt}\n"
                f"📊 Sous-wallets détectés (aval) : {len(arbre_aval)}\n"
                f"{sous_wallets_txt}\n"
                f"📈 Historique du cluster ({len(tous_les_wallets)} wallet(s) analysés) : {stats_txt}\n\n"
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
    # --- colonnes profil DexScreener (nouvelles) ---
    "profil_dexscreener", "site_web", "twitter", "telegram",
    # --- colonnes vélocité/qualité des ordres (nouvelles) ---
    "tx_velocity_5s", "buy_ratio_5s",
    "avg_buy_size_sol", "avg_sell_size_sol",  # approximation : DexScreener ne sépare pas achats/ventes dans le volume
    "unique_buyers_count",  # non disponible via DexScreener -> toujours vide (nécessiterait du parsing on-chain)
    # --- colonnes simulation SL/TP (nouvelles) ---
    "signal_valide", "buy_ratio_source", "position_statut", "resultat_pct_simule",
    "time_to_2x", "time_to_3x", "max_drawdown_before_peak",
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


def extraire_infos_profil(pair):
    """
    Extrait les infos de "profil" DexScreener d'une pair : site web et
    réseaux sociaux (Twitter/X, Telegram, etc.). Sur DexScreener, un token
    qui a un profil rempli (souvent payant / signal d'effort marketing du
    projet) a un champ "info" non vide, avec des sous-listes "websites" et
    "socials".
    Retourne (a_un_profil: bool, site_web: str|None, twitter: str|None,
    telegram: str|None).
    """
    info = pair.get("info") or {}
    websites = info.get("websites") or []
    socials = info.get("socials") or []

    a_un_profil = bool(websites or socials)
    site_web = websites[0].get("url") if websites and isinstance(websites[0], dict) else None

    twitter = None
    telegram = None
    for s in socials:
        if not isinstance(s, dict):
            continue
        type_social = (s.get("type") or "").lower()
        if type_social in ("twitter", "x") and not twitter:
            twitter = s.get("url")
        elif type_social == "telegram" and not telegram:
            telegram = s.get("url")

    return a_un_profil, site_web, twitter, telegram


def passe_les_filtres(market_cap, liquidity_usd):
    if not liquidity_usd:
        return False
    if liquidity_usd < MIN_LIQUIDITY_USD:
        return False
    if market_cap and (liquidity_usd / market_cap) < MIN_LIQUIDITY_RATIO:
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
        elif text in ("/csv_dextools", "/log_dextools"):
            send_telegram_document(DEXTOOLS_LOG_FILE, caption="📊 Historique canal DexToolsPublic (multiplicateur alerte → ATH 24h)")
        elif text == "/help":
            send_telegram_message(
                "Commandes disponibles :\n"
                "/csv — télécharger le fichier de log complet\n"
                "/csv_dextools — télécharger le suivi du canal DexToolsPublic"
            )


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
    dernier_mc_valide = None

    nb_mesures = max(int(ANALYSE_20S_DURATION / ANALYSE_20S_SAMPLE_INTERVAL), 1)
    for i in range(nb_mesures + 1):
        elapsed = round(time.time() - start)
        pair = fetch_pair_data(mint)
        mc = None
        buys_m5 = sells_m5 = None
        if pair:
            mc = pair.get("marketCap", 0) or pair.get("fdv", 0)
            if mc:
                dernier_mc_valide = mc
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

    def _achats_ventes_cumules(fenetre_max_s):
        achats = sum(da for _, e_fin, da, _ in deltas if e_fin <= fenetre_max_s)
        ventes = sum(dv for _, e_fin, _, dv in deltas if e_fin <= fenetre_max_s)
        return achats, ventes

    def _buy_ratio(fenetre_max_s):
        achats, ventes = _achats_ventes_cumules(fenetre_max_s)
        total = achats + ventes
        return round(achats / total, 3) if total else None

    achats_5s, ventes_5s = _achats_ventes_cumules(5)
    tx_velocity_5s = achats_5s + ventes_5s
    buy_ratio_5s = round(achats_5s / tx_velocity_5s, 3) if tx_velocity_5s else None
    buy_ratio_10s = _buy_ratio(10)
    buy_ratio_20s = _buy_ratio(ANALYSE_20S_DURATION)

    if mint in active_tokens:
        active_tokens[mint]["buy_ratio_10s"] = buy_ratio_10s
        active_tokens[mint]["buy_ratio_20s"] = buy_ratio_20s
        active_tokens[mint]["achats_bruts_2s"] = achats_bruts_2s
        active_tokens[mint]["buy_ratio_2s"] = buy_ratio_2s
        active_tokens[mint]["tx_velocity_5s"] = tx_velocity_5s
        active_tokens[mint]["buy_ratio_5s"] = buy_ratio_5s

        # --- Validation du signal + ouverture éventuelle de la simulation SL/TP ---
        entry_stats = active_tokens[mint].get("entry_stats", {})
        price_change_m5 = entry_stats.get("price_change_m5")
        tx_accel = entry_stats.get("tx_accel")

        # Ratio buy/sell utilisé pour la validation du signal : buy_ratio_20s
        # en priorité (mesuré après l'alerte, sur les 20 premières secondes
        # de trading). S'il est absent (NaN — ex: DexScreener a renvoyé une
        # valeur nulle sur au moins une des mesures, rendant les deltas
        # incalculables), on bascule sur un ratio de repli calculé à partir
        # des transactions m5 déjà connues à l'alerte (achats_m5/ventes_m5) :
        # ce n'est pas la même fenêtre temporelle que buy_ratio_20s, mais ça
        # évite de rejeter automatiquement un token valide à cause d'un bug
        # de collecte de données plutôt qu'un vrai signal faible.
        ratio_utilise = buy_ratio_20s
        ratio_source = "buy_ratio_20s"
        if ratio_utilise is None and FALLBACK_BUY_RATIO_ENABLED:
            achats_m5 = entry_stats.get("txns_buys_m5")
            ventes_m5 = entry_stats.get("txns_sells_m5")
            if achats_m5 is not None and ventes_m5 is not None and (achats_m5 + ventes_m5) > 0:
                ratio_utilise = round(achats_m5 / (achats_m5 + ventes_m5), 3)
                ratio_source = "fallback_m5"
        if ratio_utilise is None:
            ratio_source = "aucune_donnee"  # ni buy_ratio_20s ni fallback m5 n'étaient exploitables

        seuil_ratio = FALLBACK_BUY_RATIO_MIN if ratio_source == "fallback_m5" else TRIGGER_BUY_RATIO_20S_MIN

        signal_valide = (
            ratio_utilise is not None and ratio_utilise >= seuil_ratio
            and passe_filtres_triggers(initial_mc, price_change_m5, tx_accel)
        )
        active_tokens[mint]["signal_valide"] = signal_valide
        active_tokens[mint]["buy_ratio_source"] = ratio_source  # persiste la donnée pour le CSV (étape 1/3)
        if ratio_source == "fallback_m5":
            print(f"[simulation] {data['symbol']} ({mint}) — buy_ratio_20s absent, fallback_m5 utilisé = {ratio_utilise}")

        if signal_valide and dernier_mc_valide:
            active_tokens[mint]["prix_entree_simule"] = dernier_mc_valide
            active_tokens[mint]["sl_prix_simule"] = dernier_mc_valide * (1 + SIMULATION_SL_PCT)
            active_tokens[mint]["position_statut"] = "ouverte"
            active_tokens[mint]["entry_time"] = time.time()  # horodatage exact de l'achat simulé, pour le Time-based Exit
            print(f"[simulation] {data['symbol']} ({mint}) — signal validé, position simulée ouverte à ${dernier_mc_valide:,.0f}")
        else:
            active_tokens[mint]["position_statut"] = "signal_non_valide"

    print(
        f"[analyse_20s] {data['symbol']} ({mint}) — "
        f"buy_ratio_2s={buy_ratio_2s} buy_ratio_5s={buy_ratio_5s} buy_ratio_10s={buy_ratio_10s} "
        f"buy_ratio_20s={buy_ratio_20s} achats_bruts_2s={achats_bruts_2s}"
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

    # --- Calcul anticipé de price_change_m5 / tx_accel pour le filtre de trigger ---
    txns = pair.get("txns") or {}
    volume = pair.get("volume") or {}
    txns_m5 = txns.get("m5") or {}
    txns_h1 = txns.get("h1") or {}
    price_change_m5 = (pair.get("priceChange") or {}).get("m5")

    tot_m5 = (txns_m5.get("buys") or 0) + (txns_m5.get("sells") or 0)
    tot_h1 = (txns_h1.get("buys") or 0) + (txns_h1.get("sells") or 0)
    tx_accel = round((tot_m5 * 12) / tot_h1, 3) if tot_h1 > 0 else None

    if not passe_filtres_triggers(market_cap, price_change_m5, tx_accel):
        print(f"[trigger] {symbol} ({mint}) rejeté — MC=${market_cap:,.0f} price_change_m5={price_change_m5} tx_accel={tx_accel}")
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

    # --- Détection boost DexScreener ---
    boost_detecte, nombre_boosts_actifs = extraire_infos_boost(pair)

    # --- Détection profil DexScreener (site web / réseaux sociaux) ---
    a_un_profil, site_web, twitter, telegram_link = extraire_infos_profil(pair)

    # --- Taille moyenne des ordres (approximation, cf. limitation ci-dessous) ---
    # DexScreener ne distingue PAS le volume acheteur du volume vendeur : on ne
    # peut donc calculer qu'une taille d'ordre MOYENNE globale, appliquée aux
    # deux colonnes achat/vente faute de meilleure donnée disponible.
    sol_price = get_sol_usd_price()
    volume_m5 = volume.get("m5")
    avg_order_size_sol = None
    if volume_m5 and tot_m5 and sol_price:
        avg_order_size_usd = volume_m5 / tot_m5
        avg_order_size_sol = round(avg_order_size_usd / sol_price, 4)

    entry_stats = {
        "liquidity_ratio": round(liquidity_usd / market_cap, 4) if market_cap else None,
        "txns_buys_m5": txns_m5.get("buys"),
        "txns_sells_m5": txns_m5.get("sells"),
        "volume_m5": volume_m5,
        "txns_buys_h1": txns_h1.get("buys"),
        "txns_sells_h1": txns_h1.get("sells"),
        "volume_h1": volume.get("h1"),
        "price_change_m5": price_change_m5,
        "tx_accel": tx_accel,
        "avg_buy_size_sol": avg_order_size_sol,
        "avg_sell_size_sol": avg_order_size_sol,
    }

    flags_txt = ", ".join(rug_flags) if rug_flags else "Aucun"
    top10_txt = f"{top10_pct}%" if top10_pct is not None else "N/A"
    holders_txt = f"{total_holders}" if total_holders is not None else "N/A"
    bundle_txt = "⚠️ Oui" if bundle_detected else "Non"
    boost_txt = f"⚡ Oui ({nombre_boosts_actifs})" if boost_detecte else "Non"
    profil_txt = "✅ Oui" if a_un_profil else "Non"
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
        f"🌐 Profil DexScreener : {profil_txt}\n"
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
        "min_price": market_cap or 1.0,
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
        # champs profil DexScreener (mis à jour aussi dans monitor_ath si le
        # profil apparaît/se complète après coup)
        "profil_dexscreener": a_un_profil,
        "site_web": site_web,
        "twitter": twitter,
        "telegram": telegram_link,
        # champs cluster, remplis plus tard en arrière-plan (peuvent rester
        # à None si l'analyse cluster n'a pas terminé avant le rapport 30 min)
        "cluster_dev_wallet": None,
        "cluster_funding_wallet": None,
        "cluster_wallet_count": None,
        "cluster_tokens_historiques": None,
        "cluster_mult_moyen": None,
        "cluster_mult_max": None,
        "cluster_mult_min": None,
        # champs simulation SL/TP (remplis après les 20 premières secondes)
        "signal_valide": None,
        "buy_ratio_source": None,  # "buy_ratio_20s" | "fallback_m5" | "aucune_donnee"
        "position_statut": "analyse_20s_en_cours",
        "prix_entree_simule": None,
        "sl_prix_simule": None,
        "entry_time": None,  # horodatage exact de l'achat simulé, utilisé par le Time-based Exit
        "tp1_atteint": False,
        "tp2_atteint": False,
        "max_price_apres_tp2": None,
        "resultat_pct_simule": None,
        "time_to_2x": None,
        "time_to_3x": None,
        "tx_velocity_5s": None,
        "buy_ratio_5s": None,
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
                        if current_mc and current_mc < active_tokens[mint].get("min_price", current_mc):
                            active_tokens[mint]["min_price"] = current_mc

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

                        # Le profil DexScreener peut aussi être ajouté/complété
                        # après l'alerte initiale : on met à jour si détecté.
                        profil_detecte_maj, site_maj, twitter_maj, telegram_maj = extraire_infos_profil(pairs[0])
                        if profil_detecte_maj:
                            active_tokens[mint]["profil_dexscreener"] = True
                            active_tokens[mint]["site_web"] = active_tokens[mint].get("site_web") or site_maj
                            active_tokens[mint]["twitter"] = active_tokens[mint].get("twitter") or twitter_maj
                            active_tokens[mint]["telegram"] = active_tokens[mint].get("telegram") or telegram_maj

                        # --- Simulation SL/TP (aucun ordre réel, tracking seulement) ---
                        if current_mc:
                            gerer_simulation_position(mint, current_mc, elapsed)
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

            # --- Finalisation de la simulation SL/TP si jamais clôturée ---
            if data.get("signal_valide") and data.get("resultat_pct_simule") is None:
                entree = data.get("prix_entree_simule") or initial_mc
                active_tokens[mint]["resultat_pct_simule"] = round((max_mc / entree - 1) * 100, 2)
                if data.get("position_statut") in ("ouverte", "tp1", "trailing"):
                    active_tokens[mint]["position_statut"] = "expire_30min"
                data = active_tokens[mint]

            if data.get("signal_valide"):
                resultat_txt = (
                    f"{data.get('resultat_pct_simule', 0):+.1f}%"
                    if data.get("resultat_pct_simule") is not None else "N/A"
                )
                simulation_txt = f"🎯 Signal validé — Résultat simulé : {resultat_txt} (statut: {data.get('position_statut')})\n"
            else:
                simulation_txt = "🎯 Signal non validé par les triggers (pas de simulation)\n"

            msg_rapport = (
                f"📋 *Rapport 30 min*\n\n"
                f"🪙 Token : {data['symbol']}\n"
                f"💰 Market Cap initial (à la migration) : ${initial_mc:,.0f}\n"
                f"🏆 Market Cap max atteint : ${max_mc:,.0f}\n"
                f"✖️ Multiplicateur : x{multiplicateur:,.2f}\n"
                f"🚀 Boosté : {boost_txt_rapport}\n"
                f"{simulation_txt}"
                f"🔗 [Voir sur DexScreener]({dex_url})\n"
                f"⚡ [Trader sur Axiom](https://axiom.trade/meme/{mint})"
            )
            send_telegram_message(msg_rapport)
            print(f"Rapport 30 min envoyé pour : {data['symbol']} — x{multiplicateur:,.2f}")

            entry_stats = data.get("entry_stats", {})

            min_price = data.get("min_price", initial_mc)
            max_drawdown_before_peak = round((min_price / initial_mc - 1) * 100, 2) if initial_mc else None

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
                # --- stats profil DexScreener ---
                "profil_dexscreener": data.get("profil_dexscreener", False),
                "site_web": data.get("site_web"),
                "twitter": data.get("twitter"),
                "telegram": data.get("telegram"),
                # --- vélocité / qualité des ordres ---
                "tx_velocity_5s": data.get("tx_velocity_5s"),
                "buy_ratio_5s": data.get("buy_ratio_5s"),
                "avg_buy_size_sol": entry_stats.get("avg_buy_size_sol"),
                "avg_sell_size_sol": entry_stats.get("avg_sell_size_sol"),
                "unique_buyers_count": None,  # non disponible via DexScreener
                # --- simulation SL/TP ---
                "signal_valide": data.get("signal_valide"),
                "buy_ratio_source": data.get("buy_ratio_source"),
                "position_statut": data.get("position_statut"),
                "resultat_pct_simule": data.get("resultat_pct_simule"),
                "time_to_2x": data.get("time_to_2x"),
                "time_to_3x": data.get("time_to_3x"),
                "max_drawdown_before_peak": max_drawdown_before_peak,
            })

            tokens_to_remove.append(mint)

    for mint in tokens_to_remove:
        active_tokens.pop(mint, None)


# ============================================================
# --- MODULE CANAL DEXTOOLSPUBLIC : suivi indépendant ---
# ============================================================
#
# Ce module n'a AUCUN rapport avec la logique de détection/filtres du
# bot ci-dessus (DexScreener token-profiles, filtres qualité/trigger,
# RugCheck, simulation SL/TP, dev cluster...). Il lit TOUS les posts du
# canal Telegram public t.me/DexToolsPublic, SANS AUCUN FILTRE, capture
# les infos affichées dans le message d'alerte (Pair Age, 24h %, volume,
# buys/sells, boosts...), puis suit chaque token pendant 24h pour
# calculer le multiplicateur entre le market cap au moment de l'alerte
# et l'ATH atteint sur ces 24h. Le résultat est loggé dans un CSV séparé
# (DEXTOOLS_LOG_FILE), distinct de LOG_FILE ci-dessus.
#
# Il réutilise volontairement les fonctions déjà définies plus haut
# (fetch_pair_data, rugcheck_verdict, extraire_infos_boost,
# extraire_infos_profil, send_telegram_message, send_telegram_document,
# _to_float) au lieu de les redéfinir.

DEXTOOLS_CHANNEL = "DexToolsPublic"
DEXTOOLS_CHANNEL_URL = f"https://t.me/s/{DEXTOOLS_CHANNEL}"

DEXTOOLS_LOG_FILE = "dextools_channel_log.csv"
DEXTOOLS_LOG_FIELDS = [
    "horodatage_alerte", "mint", "symbole",
    "pair_age_alerte", "variation_24h_alerte", "volume_alerte_txt",
    "buys_alerte_txt", "sells_alerte_txt", "boosts_alerte", "message_brut",
    "mc_initial_alerte", "prix_initial_alerte", "liquidite_usd_alerte",
    "score_rugcheck", "alertes_rugcheck", "pct_top10_holders",
    "insiders_detectes", "nombre_holders", "bundle_detecte", "lp_locked_pct",
    "profil_dexscreener", "site_web", "twitter", "telegram",
    "mc_ath_24h", "multiplicateur_24h", "horodatage_ath",
    "lien_dexscreener", "lien_post_telegram",
]

DEXTOOLS_CHANNEL_CHECK_INTERVAL = 60     # secondes entre 2 lectures du canal
DEXTOOLS_PRICE_CHECK_INTERVAL = 300      # secondes entre 2 vérifications de prix pendant le suivi 24h
DEXTOOLS_TRACK_DURATION = 24 * 3600      # durée totale de suivi avant de clôturer et écrire la ligne CSV

dextools_tracked = {}        # mint -> données de suivi en cours
dextools_seen_posts = set()  # ids de posts déjà traités (anti-doublon)

_dextools_last_channel_check = 0
_dextools_last_price_check = 0

# Adresse Solana (base58, ni 0/O/I/l) — best-effort pour repérer la CA dans le texte brut
SOLANA_ADDRESS_RE = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')

# Liens vers un explorer/aggrégateur contenant directement le mint (plus fiable que le texte brut)
CA_FROM_LINK_RE = re.compile(
    r'(?:dexscreener\.com/solana/|solscan\.io/token/|birdeye\.so/token/|pump\.fun/(?:coin/)?)'
    r'([1-9A-HJ-NP-Za-km-z]{32,44})'
)

POST_BLOCK_RE = re.compile(
    r'data-post="' + re.escape(DEXTOOLS_CHANNEL) + r'/(?P<post_id>\d+)".*?'
    r'<time[^>]*datetime="(?P<datetime>[^"]+)"',
    re.DOTALL,
)
MESSAGE_TEXT_RE = re.compile(
    r'<div class="tgme_widget_message_text[^"]*"[^>]*>(?P<text>.*?)</div>',
    re.DOTALL,
)

TICKER_RE = re.compile(r'\$([A-Za-z0-9]{2,15})\b')
PAIR_AGE_RE = re.compile(r'Pair Age:\s*([^\n📈🔥💧👥🛡️🚩🕵️🚀🌐🔗⚡️👶📊]+)', re.IGNORECASE)
CHANGE_24H_RE = re.compile(r'24h:\s*([+\-0-9.,]+%)\s*\|\s*V:\s*\$?([0-9.,a-zA-Z]+)', re.IGNORECASE)
BUYS_SELLS_RE = re.compile(r'Buys:\s*([0-9.,a-zA-Z]+)\s*\|\s*Sells:\s*([0-9.,a-zA-Z]+)', re.IGNORECASE)
BOOSTS_RE = re.compile(r'Boosts?:\s*([0-9]+)', re.IGNORECASE)


def _dextools_nettoyer_html(fragment_html):
    """Retire les balises HTML d'un fragment de message tout en gardant le texte lisible (br -> saut de ligne)."""
    if not fragment_html:
        return ""
    texte = re.sub(r'<br\s*/?>', '\n', fragment_html)
    texte = re.sub(r'<[^>]+>', '', texte)
    return html.unescape(texte).strip()


def fetch_dextools_channel_html():
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        }
        res = requests.get(DEXTOOLS_CHANNEL_URL, headers=headers, timeout=15)
        if res.status_code != 200:
            print(f"[dextools][fetch_channel] status={res.status_code}")
            return None
        return res.text
    except Exception as e:
        print(f"[dextools][fetch_channel] erreur : {e}")
        return None


def extraire_mint_dextools(message_html, message_txt):
    """Cherche d'abord une CA dans un lien connu (plus fiable), sinon retombe sur un pattern base58 brut."""
    m = CA_FROM_LINK_RE.search(message_html)
    if m:
        return m.group(1)
    candidats = SOLANA_ADDRESS_RE.findall(message_txt)
    return candidats[0] if candidats else None


def extraire_infos_alerte_dextools(message_html):
    """
    Extrait les infos affichées dans le message d'alerte du canal, au format
    donné en exemple : Pair Age / 24h % / Volume / Buys / Sells / Boosts.
    Tout est best-effort et stocké tel quel (texte brut) : le canal peut
    changer son format, chaque champ manque proprement (None) sans planter.
    """
    texte = _dextools_nettoyer_html(message_html)

    symbole = None
    m = TICKER_RE.search(texte)
    if m:
        symbole = m.group(1)

    pair_age = None
    m = PAIR_AGE_RE.search(texte)
    if m:
        pair_age = m.group(1).strip()

    variation_24h, volume_txt = None, None
    m = CHANGE_24H_RE.search(texte)
    if m:
        variation_24h, volume_txt = m.group(1).strip(), m.group(2).strip()

    buys_txt, sells_txt = None, None
    m = BUYS_SELLS_RE.search(texte)
    if m:
        buys_txt, sells_txt = m.group(1).strip(), m.group(2).strip()

    boosts = None
    m = BOOSTS_RE.search(texte)
    if m:
        boosts = m.group(1).strip()

    return {
        "symbole": symbole,
        "pair_age": pair_age,
        "variation_24h": variation_24h,
        "volume_txt": volume_txt,
        "buys_txt": buys_txt,
        "sells_txt": sells_txt,
        "boosts": boosts,
        "message_brut": texte.replace("\n", " | "),
    }


def check_dextools_channel():
    """Lit le canal, repère les nouveaux posts, démarre le suivi 24h pour chaque nouveau token trouvé (sans exception, aucun filtre)."""
    page_html = fetch_dextools_channel_html()
    if not page_html:
        return

    for match in POST_BLOCK_RE.finditer(page_html):
        post_id = match.group("post_id")
        if post_id in dextools_seen_posts:
            continue
        dextools_seen_posts.add(post_id)

        bloc = page_html[match.start():match.start() + 6000]  # fenêtre large autour du post pour capter le texte du message
        text_match = MESSAGE_TEXT_RE.search(bloc)
        message_html = text_match.group("text") if text_match else ""
        message_txt = _dextools_nettoyer_html(message_html)

        mint = extraire_mint_dextools(bloc, message_txt)
        if not mint:
            print(f"[dextools] post {post_id} — aucune adresse token trouvée, ignoré")
            continue

        if mint in dextools_tracked:
            print(f"[dextools] {mint} déjà en cours de suivi, post {post_id} ignoré")
            continue

        alert_datetime_str = match.group("datetime")
        try:
            alert_dt = datetime.fromisoformat(alert_datetime_str.replace("Z", "+00:00"))
            alert_time = alert_dt.timestamp()
        except Exception:
            alert_dt = datetime.now()
            alert_time = time.time()

        infos_alerte = extraire_infos_alerte_dextools(message_html)
        demarrer_suivi_dextools(mint, infos_alerte, post_id, alert_time, alert_dt)


def check_dextools_channel_throttled():
    """Ne lit le canal qu'au rythme de DEXTOOLS_CHANNEL_CHECK_INTERVAL, même si appelée plus souvent depuis la boucle principale."""
    global _dextools_last_channel_check
    now = time.time()
    if (now - _dextools_last_channel_check) < DEXTOOLS_CHANNEL_CHECK_INTERVAL:
        return
    _dextools_last_channel_check = now
    check_dextools_channel()


def demarrer_suivi_dextools(mint, infos_alerte, post_id, alert_time, alert_dt):
    """Enrichit le token avec les données dispo au moment de l'alerte (réutilise les fonctions du bot existant) et démarre le suivi 24h."""

    pair = fetch_pair_data(mint)
    mc_initial, prix_initial, liquidite_usd, pool_address = None, None, None, None
    if pair:
        mc_initial = pair.get("marketCap", 0) or pair.get("fdv", 0) or None
        prix_initial = _to_float(pair.get("priceUsd"))
        liquidite_usd = (pair.get("liquidity") or {}).get("usd")
        pool_address = pair.get("pairAddress")

    # rugcheck_verdict() sert ici UNIQUEMENT à remplir des colonnes
    # d'information : on ignore volontairement le booléen "ok", aucun
    # token n'est filtré/rejeté dans ce module.
    _rug_ok, score_rugcheck, flags_rugcheck, top10_pct, insiders_detected, lp_locked_pct, total_holders, bundle_detected = rugcheck_verdict(mint)

    a_un_profil, site_web, twitter, telegram_link = (False, None, None, None)
    if pair:
        a_un_profil, site_web, twitter, telegram_link = extraire_infos_profil(pair)

    dextools_tracked[mint] = {
        "post_id": post_id,
        "alert_time": alert_time,
        "alert_datetime_str": alert_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "symbole": infos_alerte.get("symbole"),
        "pair_age_alerte": infos_alerte.get("pair_age"),
        "variation_24h_alerte": infos_alerte.get("variation_24h"),
        "volume_alerte_txt": infos_alerte.get("volume_txt"),
        "buys_alerte_txt": infos_alerte.get("buys_txt"),
        "sells_alerte_txt": infos_alerte.get("sells_txt"),
        "boosts_alerte": infos_alerte.get("boosts"),
        "message_brut": infos_alerte.get("message_brut"),
        "mc_initial": mc_initial,
        "prix_initial": prix_initial,
        "liquidite_usd": liquidite_usd,
        "pool_address": pool_address,
        "score_rugcheck": score_rugcheck,
        "alertes_rugcheck": ", ".join(flags_rugcheck) if flags_rugcheck else None,
        "pct_top10_holders": top10_pct,
        "insiders_detectes": insiders_detected,
        "nombre_holders": total_holders,
        "bundle_detecte": bundle_detected,
        "lp_locked_pct": lp_locked_pct,
        "profil_dexscreener": a_un_profil,
        "site_web": site_web,
        "twitter": twitter,
        "telegram": telegram_link,
        "max_mc": mc_initial,
        "max_mc_time": alert_time,
        "lien_post_telegram": f"https://t.me/{DEXTOOLS_CHANNEL}/{post_id}",
        "lien_dexscreener": f"https://dexscreener.com/solana/{mint}",
    }

    print(f"[dextools] nouveau suivi démarré pour {mint} (post {post_id}, MC initial={mc_initial})")


def get_ath_24h_via_gecko(pool_address):
    """
    Récupère jusqu'à 24 bougies horaires GeckoTerminal et retourne
    (plus_haut_prix_usd, timestamp_unix_de_la_bougie) ou (None, None).
    Best-effort : si le pool n'existe pas encore sur GeckoTerminal ou que
    l'API échoue, on retombe sur le suivi par polling (max_mc/max_mc_time).
    """
    if not pool_address:
        return None, None
    try:
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/hour"
        params = {"aggregate": 1, "limit": 24, "currency": "usd", "token": "base"}
        res = requests.get(url, params=params, timeout=8)
        if res.status_code != 200:
            return None, None
        ohlcv_list = res.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        if not ohlcv_list:
            return None, None
        # chaque bougie : [timestamp, open, high, low, close, volume]
        meilleure = max((c for c in ohlcv_list if len(c) >= 3), key=lambda c: c[2], default=None)
        if not meilleure:
            return None, None
        return meilleure[2], meilleure[0]
    except Exception as e:
        print(f"[dextools][get_ath_24h_via_gecko] erreur pour {pool_address} : {e}")
        return None, None


def log_resultat_dextools_csv(row):
    try:
        file_existe = os.path.isfile(DEXTOOLS_LOG_FILE)
        with open(DEXTOOLS_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=DEXTOOLS_LOG_FIELDS)
            if not file_existe:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[dextools][log_resultat_dextools_csv] erreur : {e}")


def cloturer_suivi_dextools(mint):
    data = dextools_tracked.get(mint)
    if not data:
        return

    mc_initial = data.get("mc_initial") or 1.0

    gecko_high, gecko_ts = get_ath_24h_via_gecko(data.get("pool_address"))
    mc_ath_gecko = None
    if gecko_high and data.get("prix_initial"):
        mc_ath_gecko = mc_initial * (gecko_high / data["prix_initial"])

    # On garde la valeur la plus haute entre le suivi par polling et
    # l'estimation GeckoTerminal, avec l'horodatage correspondant.
    candidats = [(data.get("max_mc") or mc_initial, data.get("max_mc_time"))]
    if mc_ath_gecko:
        candidats.append((mc_ath_gecko, gecko_ts))
    mc_ath, ts_ath = max(candidats, key=lambda x: x[0])

    try:
        horodatage_ath = datetime.fromtimestamp(ts_ath).strftime("%Y-%m-%d %H:%M:%S") if ts_ath else ""
    except Exception:
        horodatage_ath = ""

    multiplicateur = round(mc_ath / mc_initial, 3) if mc_initial else None

    log_resultat_dextools_csv({
        "horodatage_alerte": data.get("alert_datetime_str"),
        "mint": mint,
        "symbole": data.get("symbole"),
        "pair_age_alerte": data.get("pair_age_alerte"),
        "variation_24h_alerte": data.get("variation_24h_alerte"),
        "volume_alerte_txt": data.get("volume_alerte_txt"),
        "buys_alerte_txt": data.get("buys_alerte_txt"),
        "sells_alerte_txt": data.get("sells_alerte_txt"),
        "boosts_alerte": data.get("boosts_alerte"),
        "message_brut": data.get("message_brut"),
        "mc_initial_alerte": mc_initial,
        "prix_initial_alerte": data.get("prix_initial"),
        "liquidite_usd_alerte": data.get("liquidite_usd"),
        "score_rugcheck": data.get("score_rugcheck"),
        "alertes_rugcheck": data.get("alertes_rugcheck"),
        "pct_top10_holders": data.get("pct_top10_holders"),
        "insiders_detectes": data.get("insiders_detectes"),
        "nombre_holders": data.get("nombre_holders"),
        "bundle_detecte": data.get("bundle_detecte"),
        "lp_locked_pct": data.get("lp_locked_pct"),
        "profil_dexscreener": data.get("profil_dexscreener"),
        "site_web": data.get("site_web"),
        "twitter": data.get("twitter"),
        "telegram": data.get("telegram"),
        "mc_ath_24h": round(mc_ath, 2) if mc_ath else None,
        "multiplicateur_24h": multiplicateur,
        "horodatage_ath": horodatage_ath,
        "lien_dexscreener": data.get("lien_dexscreener"),
        "lien_post_telegram": data.get("lien_post_telegram"),
    })

    print(f"[dextools] suivi 24h clôturé pour {mint} ({data.get('symbole')}) — x{multiplicateur}")
    dextools_tracked.pop(mint, None)


def monitor_dextools_ath():
    """Met à jour le max market cap observé pour chaque token suivi, et clôture ceux dont les 24h sont écoulées."""
    global _dextools_last_price_check
    now = time.time()

    do_price_check = (now - _dextools_last_price_check) >= DEXTOOLS_PRICE_CHECK_INTERVAL
    if do_price_check:
        _dextools_last_price_check = now

    tokens_a_cloturer = []

    for mint, data in list(dextools_tracked.items()):
        elapsed = now - data["alert_time"]

        if do_price_check:
            pair = fetch_pair_data(mint)
            if pair:
                current_mc = pair.get("marketCap", 0) or pair.get("fdv", 0)
                if current_mc and current_mc > (data.get("max_mc") or 0):
                    data["max_mc"] = current_mc
                    data["max_mc_time"] = now
                if not data.get("pool_address"):
                    data["pool_address"] = pair.get("pairAddress")

        if elapsed >= DEXTOOLS_TRACK_DURATION:
            tokens_a_cloturer.append(mint)

    for mint in tokens_a_cloturer:
        cloturer_suivi_dextools(mint)


# ============================================================
# --- FIN MODULE CANAL DEXTOOLSPUBLIC ---
# ============================================================


if __name__ == "__main__":
    print("Bot de surveillance démarré...")
    while True:
        check_telegram_commands()
        check_new_solana_tokens()
        check_pending_tokens()
        monitor_ath()
        # --- module indépendant : canal Telegram DexToolsPublic (voir plus haut) ---
        check_dextools_channel_throttled()
        monitor_dextools_ath()
        time.sleep(10)
