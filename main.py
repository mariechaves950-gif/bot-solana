import os
import re
import csv
import html
import time
import threading
import traceback
import operator
import requests
from datetime import datetime

# pandas n'est nécessaire QUE pour le rapport comparatif des stratégies
# (/comparatif). On ne veut pas que tout le bot plante au démarrage si
# pandas n'est pas encore dans requirements.txt : on dégrade proprement.
try:
    import pandas as pd
except ImportError:
    pd = None

# --- CONFIGURATION ---
# Le token ne doit JAMAIS être écrit en dur ici.
# Sur Railway : Settings > Variables > ajoute TELEGRAM_TOKEN et CHAT_ID
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    raise RuntimeError("TELEGRAM_TOKEN ou CHAT_ID manquant dans les variables d'environnement")

active_tokens = {}  # tokens alertés, en cours de suivi ATH
seen_mints = set()  # mints déjà ALERTÉS (pour ne jamais ré-alerter le même token)
pending_mints = {}  # mints vus mais PAS ENCORE migrés : mint -> premier vu (timestamp)

# Anti-doublon CSV : mints dont une ligne a déjà été écrite dans LOG_FILE au
# cours de ce processus. Empêche qu'un même token soit journalisé deux fois
# (ex: si un même mint réapparaît dans active_tokens après un redémarrage
# partiel de la boucle, ou tout autre cas limite).
tokens_traites = set()

# Un mint en attente de migration est abandonné après ce délai (il ne migrera
# probablement plus, inutile de continuer à interroger l'API pour lui).
PENDING_MAX_AGE = 24 * 3600  # 24h

# --- FILTRES DE QUALITÉ ---
# Ces seuils réduisent le bruit (tokens sans intérêt / scams grossiers)
# mais ne garantissent JAMAIS qu'un token est fiable. Aucun filtre
# automatique ne protège d'un rug pull.
MIN_LIQUIDITY_USD = 5000  # liquidité minimum sur le pool
MIN_LIQUIDITY_RATIO = 0.03  # liquidité doit représenter au moins 3% du market cap (si market cap connu)
# MIN_MARKET_CAP a été retiré : le bot ne filtre plus par Market Cap, tous
# les tokens sont traités sans exception, quel que soit leur Market Cap.

# Un token pump.fun reste sur "pumpfun" (bonding curve) tant qu'il n'a pas
# gradué. Une fois la bonding curve terminée (migration), il apparaît sur
# PumpSwap ou Raydium. On ne veut alerter QUE sur les tokens déjà migrés.
DEX_MIGRES = {"pumpswap", "raydium"}

# --- CHAÎNE UNIQUE : SOLANA UNIQUEMENT ---
# Toute source de tokens (profils DexScreener, boosts, canal Telegram
# DexToolsPublic) est filtrée sur cette valeur avant d'être acceptée
# n'importe où dans le pipeline. Aucun token Ethereum / BSC / Base / etc.
# ne doit jamais atteindre essayer_alerter() ou demarrer_suivi_dextools().
CHAIN_ID_SOLANA = "solana"

# --- CONCENTRATION DES HOLDERS (topHolders / risks, via RugCheck) ---
REJECT_TOP10_PCT = None
REJECT_IF_INSIDERS = False

# --- ANALYSE DES 20 PREMIÈRES SECONDES ---
# Intervalle allongé de 2s -> 3s pour réduire le volume d'appels API
# DexScreener (fetch_pair_data) pendant la phase d'analyse initiale de
# chaque token suivi — voir discussion sur le risque de dépassement des
# rate limits (60 req/min profils/boosts, 300 req/min pairs) maintenant
# que tous les tokens migrés sont suivis sans pré-filtre.
ANALYSE_20S_SAMPLE_INTERVAL = 3  # secondes entre 2 mesures
ANALYSE_20S_DURATION = 20  # durée totale observée


def passe_filtres_triggers(market_cap, price_change_m5, tx_accel):
    if price_change_m5 is None or not (TRIGGER_PRICE_CHANGE_M5_MIN < price_change_m5 < TRIGGER_PRICE_CHANGE_M5_MAX):
        return False
    if tx_accel is None or tx_accel <= TRIGGER_TX_ACCEL_MIN:
        return False
    return True


# ============================================================
# --- FILTRES DE TRIGGER (signal d'entrée optimisé) ---
# ============================================================
TRIGGER_PRICE_CHANGE_M5_MIN = 10
TRIGGER_PRICE_CHANGE_M5_MAX = 50
TRIGGER_TX_ACCEL_MIN = 1.2
TRIGGER_BUY_RATIO_20S_MIN = 0.55


SIMULATION_SL_PCT = -0.25
SIMULATION_TP1_MULT = 2.0
SIMULATION_TP2_MULT = 3.0
SIMULATION_TRAILING_APRES_TP2_PCT = -0.20

MAX_HOLD_TIME_MINUTES = 60
TIME_EXIT_RATIO_MIN = 1 + (-0.10)
TIME_EXIT_RATIO_MAX = 1 + 0.20

TP1_PARTIAL_GAIN_PCT = 0.35
TP1_PARTIAL_SELL_RATIO = 0.5
TRAILING_ACTIVATION_GAIN_PCT = 0.20

FALLBACK_BUY_RATIO_ENABLED = True
FALLBACK_BUY_RATIO_MIN = 0.55

POOL_AGE_IDEAL_MAX_SECONDS = 180
# Auparavant à False : le filtre n'était jamais appliqué, ce qui laissait
# passer des tokens migrés depuis des heures (ex: un cas observé à 29h).
# Passé à True pour rejeter tout token dont le pool a plus de
# POOL_AGE_IDEAL_MAX_SECONDS au moment de la migration détectée.
# Seuil abaissé à 180s (3 min) : l'analyse doit démarrer dès la migration,
# pas des minutes après, sinon les signaux à 30s/3min entrent sur un token
# déjà "vieux".
POOL_AGE_STRICT_FILTER = True

CREDIBILITY_BONUS_ENABLED = True
CREDIBILITY_BONUS_REQUIRES_BOTH = True

GOLDEN_WINDOW_MAX_SECONDS = 420


def gerer_simulation_position(mint, current_mc, elapsed_seconds):
    data = active_tokens.get(mint)
    if not data or not current_mc:
        return

    statut = data.get("position_statut")
    if statut not in ("ouverte", "tp1", "trailing"):
        return

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
            data["sl_prix_simule"] = entree
            data["position_statut"] = "tp1"
            print(f"[simulation] {data['symbol']} — TP1 (x2) touché à {elapsed_seconds:.0f}s, 50% vendus (simulé), SL -> breakeven")
            return

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


_sol_price_cache = {"prix": None, "ts": 0}
SOL_PRICE_CACHE_TTL = 300


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
    return _sol_price_cache["prix"]


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
    "boost_detecte", "nombre_boosts_actifs",
    "profil_dexscreener", "site_web", "twitter", "telegram",
    "tx_velocity_5s", "buy_ratio_5s",
    "avg_order_size_sol",
    "unique_buyers_count",
    "buy_ratio_diag",
    "signal_valide", "buy_ratio_source", "position_statut", "resultat_pct_simule",
    "time_to_2x", "time_to_3x", "max_drawdown_before_peak",
    "achats_10s", "ventes_10s",
    "volume_m1", "ratio_volume_m1_m5",
    "price_change_m1", "price_change_m3",
    "buy_ratio_1m", "achats_m1", "ratio_achats_m1_m5",
    "buy_tx_ratio_m5",
    "mult_10s", "mult_30s", "mult_1m",
    "ratio_liquidite_mc", "sell_ratio_1m", "max_tx_per_second",
    "pool_age_minutes", "is_golden_window", "time_to_peak",
    "time_to_max_drawdown", "vitesse_chute_pct_par_min",
    "sim_remb_pct", "sim_remb_usd",
    "sim_ts20_pct", "sim_ts20_usd",
    "sim_ts30_pct", "sim_ts30_usd",
    "sim_3paliers_pct", "sim_3paliers_usd",
    "sim_ts_immediat_pct", "sim_ts_immediat_usd",
    "sim_peak_pct", "sim_peak_usd",
    "strat_a_pct", "strat_a_usd",
    "strat_b_pct", "strat_b_usd",
    "strat_c_pct", "strat_c_usd",
    "strat_d_pct", "strat_d_usd",
    "strat_e_pct", "strat_e_usd",
    "strat_f_pct", "strat_f_usd",
    "strat_g_pct", "strat_g_usd",
    # CORRECTIF (2026-08-19) : ATH recalculé à 1h (en plus des 30min
    # ci-dessus, mc_max/multiplicateur), avec le même filet de sécurité
    # sur le suivi live que calculer_ath_depuis_entree — voir
    # finaliser_token_log_1h(). Rempli après coup (colonnes vides tant
    # que l'heure n'est pas écoulée), la ligne d'origine est mise à jour
    # en place dans le CSV.
    "mc_max_1h", "multiplicateur_1h", "horodatage_maj_1h",
]


def log_resultat_csv(row):
    mint = row.get("mint")
    if mint and mint in tokens_traites:
        print(f"[log_resultat_csv] {mint} déjà journalisé précédemment — doublon ignoré")
        return
    try:
        file_existe = os.path.isfile(LOG_FILE)
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LOG_FIELDS)
            if not file_existe:
                writer.writeheader()
            writer.writerow(row)
        if mint:
            tokens_traites.add(mint)
    except Exception as e:
        print(f"[log_resultat_csv] erreur : {e}")


def _to_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_div(numerateur, denominateur, defaut=0):
    try:
        if not denominateur:
            return defaut
        return numerateur / denominateur
    except (TypeError, ZeroDivisionError):
        return defaut


def extraire_infos_boost(pair):
    boosts_info = pair.get("boosts") or {}
    nombre_boosts_actifs = boosts_info.get("active", 0) or 0
    boost_detecte = nombre_boosts_actifs > 0
    return boost_detecte, nombre_boosts_actifs


def extraire_infos_profil(pair):
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


def fetch_ohlcv_minute(pool_address, minutes_limit):
    if not pool_address:
        return None
    try:
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/minute"
        params = {"aggregate": 1, "limit": min(minutes_limit, 1000), "currency": "usd", "token": "base"}
        res = requests.get(url, params=params, timeout=8)
        if res.status_code != 200:
            print(f"[geckoterminal] status={res.status_code} pour {pool_address}")
            return None
        ohlcv_list = res.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        return ohlcv_list or None
    except Exception as e:
        print(f"[geckoterminal] erreur pour {pool_address} : {e}")
        return None


def fetch_ohlcv_hour(pool_address, hours_limit=25):
    """Bougies HORAIRES (pas minute) pour couvrir jusqu'à 24-25h en un
    seul appel GeckoTerminal. Actuellement NON appelée dans le flux
    principal (suivi 24h désactivé temporairement, voir finaliser_signaux
    et SIGNAUX_24H_ENABLED) — conservée telle quelle pour réactivation
    future sans tout réécrire."""
    if not pool_address:
        return None
    try:
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/hour"
        params = {"aggregate": 1, "limit": min(hours_limit, 1000), "currency": "usd", "token": "base"}
        res = requests.get(url, params=params, timeout=8)
        if res.status_code != 200:
            print(f"[geckoterminal_hour] status={res.status_code} pour {pool_address}")
            return None
        ohlcv_list = res.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        return ohlcv_list or None
    except Exception as e:
        print(f"[geckoterminal_hour] erreur pour {pool_address} : {e}")
        return None


def get_true_ath_mc(pool_address, initial_mc, initial_price, start_time, ohlcv_list=None):
    if not pool_address or not initial_price:
        return None
    try:
        if ohlcv_list is None:
            elapsed_minutes = max(int((time.time() - start_time) / 60) + 5, 10)
            ohlcv_list = fetch_ohlcv_minute(pool_address, elapsed_minutes)
        if not ohlcv_list:
            return None
        max_high = max(candle[2] for candle in ohlcv_list if len(candle) >= 3)
        if not max_high:
            return None
        return initial_mc * (max_high / initial_price)
    except Exception as e:
        print(f"[geckoterminal] erreur pour {pool_address} : {e}")
        return None


def calculer_timing_drawdown(ohlcv_list, prix_initial, start_time, min_price_fallback, min_price_time_fallback):
    time_to_dd = None
    prix_au_plus_bas = None

    if ohlcv_list and prix_initial:
        try:
            bougies = sorted(ohlcv_list, key=lambda c: c[0])
            candidate = min(
                (c for c in bougies if len(c) >= 4 and c[3]),
                key=lambda c: c[3],
                default=None,
            )
            if candidate:
                time_to_dd = max(round(candidate[0] - start_time), 0)
                prix_au_plus_bas = candidate[3]
        except (TypeError, ValueError, IndexError):
            time_to_dd = None
            prix_au_plus_bas = None

    if time_to_dd is None:
        time_to_dd = min_price_time_fallback

    minutes_ecoulees = (time_to_dd or 0) / 60
    return time_to_dd, minutes_ecoulees


# ============================================================
# --- ATH recalculé depuis l'ENTRÉE RÉELLE D'UN SIGNAL ---
# ============================================================
def calculer_ath_depuis_entree(ohlcv_list, initial_price, initial_mc, mc_entree, start_time, entree_elapsed_s,
                                live_max_mc=None, live_max_elapsed_s=None):
    """Retourne (mc_max_depuis_entree, multiplicateur_depuis_entree,
    temps_jusquau_ath_depuis_entree, horodatage_ath), calculés à partir
    des bougies OHLCV POSTÉRIEURES à l'instant d'entrée réel (start_time
    + entree_elapsed_s), AVEC un filet de sécurité sur le suivi live
    (voir CORRECTIF 2026-08-19 ci-dessous). Retourne (None, None, None,
    None) seulement si ni l'OHLCV ni le suivi live n'apportent de donnée
    exploitable.

    Fonction générique : utilisée avec des bougies MINUTE couvrant 30 min
    (ohlcv_30m) ou 1h (ohlcv_1h) — le calcul est identique quelle que
    soit la fenêtre/résolution passée en entrée.

    live_max_mc / live_max_elapsed_s : max market cap suivi EN LIVE par
    polling DexScreener sur toute la durée du token (data["max_price"] /
    data["max_price_time"] dans active_tokens), et l'instant (en
    secondes depuis start_time) où ce max a été observé. Optionnels.

    CORRECTIF (2026-08-19) : GeckoTerminal peut avoir un retard
    d'indexation important sur les pools tout juste créés (le cas
    typique visé par ce bot). Quand c'est le cas, les bougies OHLCV ne
    contiennent pas encore le vrai pic, et calculer_ath_depuis_entree
    clampait à tort le multiplicateur à 1.0 (aucune bougie ne dépassait
    le prix d'entrée) alors que le token avait réellement pris de la
    valeur — visible sur le graphique DexScreener en direct, mais absent
    des bougies GeckoTerminal au moment du calcul.
    get_true_ath_mc (utilisée pour token_log.csv) s'en sortait mieux
    car elle applique déjà `max(max_price suivi en live, ATH GeckoTerminal)`
    — cette fonction-ci ne le faisait PAS, d'où l'écart observé entre
    apres__multiplicateur (token_log.csv) et multiplicateur_depuis_entree
    (signaux_log.csv) sur un même token.
    Le filet de sécurité ci-dessous applique la même logique ici : si le
    max suivi en live après l'entrée dépasse le max trouvé via l'OHLCV,
    on retient le max live à sa place.

    (Correctifs précédents, conservés) :
    1) Le clamp qui empêche mc_max_depuis_entree de descendre sous
       mc_entree ne remettait pas à jour temps_jusquau_ath_depuis_entree,
       qui restait pointé sur la bougie écartée par le clamp — on
       obtenait un multiplicateur=1.0 correct mais associé à un temps
       aberrant (parfois >20h). Corrigé : quand le clamp s'active, le
       "pic" retenu est l'entrée elle-même (rien n'a dépassé le prix
       d'entrée après coup), donc temps=0.
    2) Aucun horodatage absolu de l'ATH n'était conservé (seulement un
       delta relatif à l'entrée). horodatage_ath est désormais toujours
       renvoyé — égal à l'instant d'entrée quand le clamp s'active,
       sinon à l'horodatage d'ouverture de la bougie retenue."""
    if not mc_entree or not start_time:
        return None, None, None, None

    mc_max_depuis_entree = None
    temps_jusquau_ath_depuis_entree = None
    horodatage_ath = None

    seuil_temps_debut = start_time + (entree_elapsed_s or 0)

    if ohlcv_list and initial_price and initial_mc:
        try:
            bougies = sorted(ohlcv_list, key=lambda c: c[0])
        except (TypeError, IndexError):
            bougies = []

        bougies_apres_entree = [c for c in bougies if len(c) >= 3 and c[0] >= seuil_temps_debut]

        if bougies_apres_entree:
            meilleure_bougie = max(bougies_apres_entree, key=lambda c: c[2])
            high = meilleure_bougie[2]
            if high:
                mc_max_brut = initial_mc * (high / initial_price)

                if mc_max_brut < mc_entree:
                    # Rien n'a dépassé le prix d'entrée après coup (selon
                    # l'OHLCV) -> le "pic" est l'entrée elle-même, atteint
                    # à temps=0 depuis l'entrée. Peut être remplacé
                    # ci-dessous par le filet de sécurité live.
                    mc_max_depuis_entree = mc_entree
                    temps_jusquau_ath_depuis_entree = 0
                    horodatage_ath = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(seuil_temps_debut))
                else:
                    mc_max_depuis_entree = mc_max_brut
                    temps_jusquau_ath_depuis_entree = max(round(meilleure_bougie[0] - seuil_temps_debut), 0)
                    horodatage_ath = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(meilleure_bougie[0]))

    # --- Filet de sécurité : suivi live (polling DexScreener) ---
    # Si un max suivi en direct après l'entrée dépasse ce qui a été
    # trouvé (ou pas trouvé) via l'OHLCV GeckoTerminal, on le retient.
    if live_max_mc and live_max_elapsed_s is not None and live_max_elapsed_s >= (entree_elapsed_s or 0):
        if mc_max_depuis_entree is None or live_max_mc > mc_max_depuis_entree:
            mc_max_depuis_entree = live_max_mc
            temps_jusquau_ath_depuis_entree = max(round(live_max_elapsed_s - (entree_elapsed_s or 0)), 0)
            horodatage_ath = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(start_time + live_max_elapsed_s)
            )

    if mc_max_depuis_entree is None:
        return None, None, None, None

    multiplicateur_depuis_entree = round(mc_max_depuis_entree / mc_entree, 4)
    return round(mc_max_depuis_entree, 2), multiplicateur_depuis_entree, temps_jusquau_ath_depuis_entree, horodatage_ath


def _mc_a_instant(ohlcv_list, initial_price, initial_mc, start_time, elapsed_s):
    """Estime le market cap au temps start_time+elapsed_s, à partir de la
    bougie OHLCV minute la plus proche de cet instant (prix de clôture).

    NOTE (2026-08-19) : cette estimation reste nécessaire au-delà de la
    fenêtre des 180 premières secondes (pas de polling live disponible
    plus loin dans le temps). En-dessous de 180s, utiliser en priorité
    _mc_a_instant_live() qui réutilise le prix RÉELLEMENT observé en
    direct — voir evaluer_et_logger_signaux_croises."""
    if not ohlcv_list or not initial_price or not initial_mc or not start_time:
        return None
    cible = start_time + elapsed_s
    try:
        bougies = sorted(ohlcv_list, key=lambda c: c[0])
    except (TypeError, IndexError):
        return None
    candidate = min(bougies, key=lambda c: abs(c[0] - cible), default=None)
    if not candidate or len(candidate) < 5 or not candidate[4]:
        return None
    prix_close = candidate[4]
    return initial_mc * (prix_close / initial_price)


def _mc_a_instant_live(data, elapsed_cible):
    """CORRECTIF (2026-08-19) : réutilise le market cap RÉELLEMENT
    observé en direct (poll DexScreener toutes les 10s pendant les 180
    premières secondes, voir analyser_metriques_etendues), au lieu de
    l'interpolation OHLCV GeckoTerminal (_mc_a_instant) qui a produit
    des écarts mesurés jusqu'à x180 sur signaux_log.csv (2026-08-19).

    Retourne le mc du sample le plus proche de elapsed_cible parmi
    data['samples_180s'], ou None si aucune donnée live n'est
    disponible (positions anciennes créées avant ce correctif, ou
    instant hors de la fenêtre 0-180s couverte par ce polling)."""
    samples = data.get("samples_180s") or []
    candidats = [s for s in samples if s[1]]  # s[1] = mc, doit être renseigné
    if not candidats:
        return None
    candidat = min(candidats, key=lambda s: abs(s[0] - elapsed_cible))
    return candidat[1]


def calculer_timing_drawdown_v2_placeholder():
    # (placeholder supprimé — conservé volontairement vide pour ne pas
    # décaler les diffs ; calculer_timing_drawdown() ci-dessus est la
    # fonction réellement utilisée)
    pass


def calculer_time_to_multiple(ohlcv_list, prix_entree, start_time, multiple):
    if not ohlcv_list or not prix_entree or not multiple:
        return None
    seuil_prix = prix_entree * multiple
    try:
        bougies = sorted(ohlcv_list, key=lambda c: c[0])
        for c in bougies:
            if len(c) < 3 or not c[2]:
                continue
            if c[2] >= seuil_prix:
                return max(round(c[0] - start_time), 0)
    except (TypeError, ValueError, IndexError):
        return None
    return None


MISE_SIMULATION_USD = 100
SIMU_TRAILING_TECH_B_PCT = -0.20
SIMU_TRAILING_TECH_C_PCT = -0.30
SIMU_TRAILING_TECH_E_PCT = -0.20


def _mult_path_depuis_ohlcv(ohlcv_list, prix_initial):
    if not ohlcv_list or not prix_initial:
        return []
    bougies = sorted(ohlcv_list, key=lambda c: c[0])
    path = []
    for c in bougies:
        if len(c) < 5:
            continue
        _, o, h, l, cl = c[:5]
        if not o or not h or not l or not cl:
            continue
        path.append((o / prix_initial, h / prix_initial, l / prix_initial, cl / prix_initial))
    return path


def _simuler_trailing_apres_seuil(path, seuil_activation, trailing_pct, seuil_sortie_anticipee=None):
    if not path:
        return 0.0

    peak = None
    for o_m, h_m, l_m, c_m in path:
        if seuil_sortie_anticipee is not None and h_m >= seuil_sortie_anticipee:
            return (seuil_sortie_anticipee - 1) * 100

        if peak is None and h_m >= seuil_activation:
            peak = h_m
        elif peak is not None and h_m > peak:
            peak = h_m

        if peak is not None:
            seuil_trailing = peak * (1 + trailing_pct)
            if l_m <= seuil_trailing:
                return (seuil_trailing - 1) * 100

    return (path[-1][3] - 1) * 100


def simuler_strategies_sortie(ohlcv_list, prix_initial, mise_usd=MISE_SIMULATION_USD):
    cles = ("sim_remb", "sim_ts20", "sim_ts30", "sim_3paliers", "sim_ts_immediat", "sim_peak")
    path = _mult_path_depuis_ohlcv(ohlcv_list, prix_initial)
    if not path:
        return {f"{c}_pct": None for c in cles} | {f"{c}_usd": None for c in cles}

    pct_a = _simuler_trailing_apres_seuil(path, seuil_activation=float("inf"), trailing_pct=0.0, seuil_sortie_anticipee=2.0)
    pct_b = _simuler_trailing_apres_seuil(path, seuil_activation=2.0, trailing_pct=SIMU_TRAILING_TECH_B_PCT)
    pct_c = _simuler_trailing_apres_seuil(path, seuil_activation=2.0, trailing_pct=SIMU_TRAILING_TECH_C_PCT)

    p1 = pct_a
    p2 = _simuler_trailing_apres_seuil(path, seuil_activation=2.0, trailing_pct=SIMU_TRAILING_TECH_B_PCT, seuil_sortie_anticipee=3.0)
    p3 = pct_b
    pct_3paliers = 0.5 * p1 + 0.25 * p2 + 0.25 * p3

    pct_e = _simuler_trailing_apres_seuil(path, seuil_activation=1.0, trailing_pct=SIMU_TRAILING_TECH_E_PCT)

    peak_max = max(h_m for _, h_m, _, _ in path)
    pct_f = (peak_max - 1) * 100

    def _usd(pct):
        return round(mise_usd * (pct / 100), 4) if pct is not None else None

    resultats = {
        "sim_remb": pct_a,
        "sim_ts20": pct_b,
        "sim_ts30": pct_c,
        "sim_3paliers": pct_3paliers,
        "sim_ts_immediat": pct_e,
        "sim_peak": pct_f,
    }
    sortie = {}
    for cle, pct in resultats.items():
        sortie[f"{cle}_pct"] = round(pct, 2)
        sortie[f"{cle}_usd"] = _usd(pct)
    return sortie


# ============================================================
# --- STRATÉGIES DE SORTIE AVANCÉES (A à G) ---
# ============================================================
STRAT_A_SL_PCT = -0.25
STRAT_A_SEUIL_ARMEMENT = 0.50
STRAT_A_TRAILING_PCT = -0.15

STRAT_B_SL_PCT = -0.25
STRAT_B_SEUIL_VENTE = 0.50
STRAT_B_TRAILING_PCT = -0.20

STRAT_C_SL_PCT = -0.25
STRAT_C_CAP_PCT = 0.40

STRAT_D_SL_PCT = -0.25
STRAT_D_PALIERS = (
    (0.20, 0.0),
    (0.40, 0.15),
    (0.70, 0.40),
)

STRAT_E_SL_PLANCHER_PCT = -0.30
STRAT_E_K_ATR = 2.0
STRAT_E_FENETRE_BOUGIES = 5

STRAT_F_SL_PCT = -0.25
STRAT_F_MAX_MINUTES = 60

STRAT_G_SL_PCT = -0.25
STRAT_G_PALIERS = (
    (0.30, 1 / 3),
    (0.60, 1 / 3),
)
STRAT_G_TRAILING_FINAL_PCT = -0.10


def _mult_path_avec_temps(ohlcv_list, prix_initial, start_time):
    if not ohlcv_list or not prix_initial or not start_time:
        return []
    bougies = sorted(ohlcv_list, key=lambda c: c[0])
    path = []
    for c in bougies:
        if len(c) < 5:
            continue
        ts, o, h, l, cl = c[:5]
        if not o or not h or not l or not cl:
            continue
        t_min = (ts - start_time) / 60
        path.append((t_min, o / prix_initial, h / prix_initial, l / prix_initial, cl / prix_initial))
    return path


def _simuler_trailing_arme_be(path, sl_initial_pct, seuil_armement, trailing_pct):
    if not path:
        return None
    armed = False
    peak = None
    for o_m, h_m, l_m, c_m in path:
        if not armed:
            if l_m <= (1 + sl_initial_pct):
                return sl_initial_pct * 100
            if h_m >= (1 + seuil_armement):
                armed = True
                peak = h_m
        if armed:
            peak = max(peak, h_m)
            seuil_effectif = max(1.0, peak * (1 + trailing_pct))
            if l_m <= seuil_effectif:
                return (seuil_effectif - 1) * 100
    return (path[-1][3] - 1) * 100


def _simuler_reverse_dca_5050(path, sl_initial_pct, seuil_vente, trailing_pct):
    if not path:
        return None
    vendu_50 = False
    peak_reste = None
    ret1 = seuil_vente
    for o_m, h_m, l_m, c_m in path:
        if not vendu_50:
            if l_m <= (1 + sl_initial_pct):
                return sl_initial_pct * 100
            if h_m >= (1 + seuil_vente):
                vendu_50 = True
                peak_reste = h_m
        if vendu_50:
            peak_reste = max(peak_reste, h_m)
            seuil_trailing = peak_reste * (1 + trailing_pct)
            if l_m <= seuil_trailing:
                ret2 = seuil_trailing - 1
                return round((0.5 * ret1 + 0.5 * ret2) * 100, 4)
    if vendu_50:
        ret2 = path[-1][3] - 1
        return round((0.5 * ret1 + 0.5 * ret2) * 100, 4)
    return (path[-1][3] - 1) * 100


def _simuler_cap_tp(path, sl_initial_pct, cap_pct):
    if not path:
        return None
    for o_m, h_m, l_m, c_m in path:
        if l_m <= (1 + sl_initial_pct):
            return sl_initial_pct * 100
        if h_m >= (1 + cap_pct):
            return cap_pct * 100
    return (path[-1][3] - 1) * 100


def _simuler_be_echelonne(path, sl_initial_pct, paliers):
    if not path:
        return None
    sl_courant = sl_initial_pct
    for o_m, h_m, l_m, c_m in path:
        if l_m <= (1 + sl_courant):
            return round(sl_courant * 100, 4)
        for seuil_gain, nouveau_sl in paliers:
            if h_m >= (1 + seuil_gain) and nouveau_sl > sl_courant:
                sl_courant = nouveau_sl
    return (path[-1][3] - 1) * 100


def _simuler_trailing_atr(path, k_atr, fenetre, sl_plancher_pct):
    if not path:
        return None
    peak = 1.0
    amplitudes = []
    for o_m, h_m, l_m, c_m in path:
        amplitudes.append(h_m - l_m)
        atr = sum(amplitudes[-fenetre:]) / min(len(amplitudes), fenetre)
        peak = max(peak, h_m)
        seuil_trailing = max(peak - k_atr * atr, 1 + sl_plancher_pct)
        if l_m <= seuil_trailing:
            return (seuil_trailing - 1) * 100
    return (path[-1][3] - 1) * 100


def _simuler_time_exit(path_avec_temps, sl_initial_pct, max_minutes):
    if not path_avec_temps:
        return None
    dernier_close = None
    for t_min, o_m, h_m, l_m, c_m in path_avec_temps:
        if l_m <= (1 + sl_initial_pct):
            return sl_initial_pct * 100
        dernier_close = c_m
        if t_min >= max_minutes:
            return (c_m - 1) * 100
    return (dernier_close - 1) * 100 if dernier_close is not None else None


def _simuler_laddering(path, sl_initial_pct, paliers, trailing_final_pct):
    if not path:
        return None
    vendu = 0.0
    gains_ponderes = 0.0
    paliers_restants = list(paliers)
    peak = None
    for o_m, h_m, l_m, c_m in path:
        if vendu == 0.0 and l_m <= (1 + sl_initial_pct):
            return sl_initial_pct * 100
        while paliers_restants and h_m >= (1 + paliers_restants[0][0]):
            seuil, fraction = paliers_restants.pop(0)
            gains_ponderes += fraction * seuil
            vendu += fraction
        if not paliers_restants:
            peak = h_m if peak is None else max(peak, h_m)
            seuil_trailing = peak * (1 + trailing_final_pct)
            if l_m <= seuil_trailing:
                fraction_restante = 1 - vendu
                gains_ponderes += fraction_restante * (seuil_trailing - 1)
                return round(gains_ponderes * 100, 4)
    fraction_restante = 1 - vendu
    gains_ponderes += fraction_restante * (path[-1][3] - 1)
    return round(gains_ponderes * 100, 4)


def simuler_strategies_avancees(ohlcv_list, prix_initial, start_time, mise_usd=MISE_SIMULATION_USD):
    cles = ("strat_a", "strat_b", "strat_c", "strat_d", "strat_e", "strat_f", "strat_g")
    path = _mult_path_depuis_ohlcv(ohlcv_list, prix_initial)
    if not path:
        return {f"{c}_pct": None for c in cles} | {f"{c}_usd": None for c in cles}

    path_temps = _mult_path_avec_temps(ohlcv_list, prix_initial, start_time)

    resultats = {
        "strat_a": _simuler_trailing_arme_be(path, STRAT_A_SL_PCT, STRAT_A_SEUIL_ARMEMENT, STRAT_A_TRAILING_PCT),
        "strat_b": _simuler_reverse_dca_5050(path, STRAT_B_SL_PCT, STRAT_B_SEUIL_VENTE, STRAT_B_TRAILING_PCT),
        "strat_c": _simuler_cap_tp(path, STRAT_C_SL_PCT, STRAT_C_CAP_PCT),
        "strat_d": _simuler_be_echelonne(path, STRAT_D_SL_PCT, STRAT_D_PALIERS),
        "strat_e": _simuler_trailing_atr(path, STRAT_E_K_ATR, STRAT_E_FENETRE_BOUGIES, STRAT_E_SL_PLANCHER_PCT),
        "strat_f": _simuler_time_exit(path_temps, STRAT_F_SL_PCT, STRAT_F_MAX_MINUTES) if path_temps else None,
        "strat_g": _simuler_laddering(path, STRAT_G_SL_PCT, STRAT_G_PALIERS, STRAT_G_TRAILING_FINAL_PCT),
    }

    def _usd(pct):
        return round(mise_usd * (pct / 100), 4) if pct is not None else None

    sortie = {}
    for cle, pct in resultats.items():
        sortie[f"{cle}_pct"] = round(pct, 2) if pct is not None else None
        sortie[f"{cle}_usd"] = _usd(pct)
    return sortie


# ============================================================
# --- RAPPORT COMPARATIF DES STRATÉGIES (envoyé sur Telegram) ---
# ============================================================
STRATEGIES_COMPARATIF = [
    ("sim_remb_pct", "Sortie immédiate à x2 (existant)"),
    ("sim_ts20_pct", "Trailing 20% après x2 (existant)"),
    ("sim_ts30_pct", "Trailing 30% après x2 (existant)"),
    ("sim_3paliers_pct", "3 paliers pondérés (existant)"),
    ("sim_ts_immediat_pct", "Trailing 20% immédiat (existant)"),
    ("sim_peak_pct", "Sortie au pic exact (théorique, existant)"),
    ("strat_a_pct", "A — Trailing armé +50% + Break-Even"),
    ("strat_b_pct", "B — Reverse DCA 50/50"),
    ("strat_c_pct", "C — Plafond de Take-Profit +40%"),
    ("strat_d_pct", "D — Break-even échelonné"),
    ("strat_e_pct", "E — Trailing volatilité (ATR approx.)"),
    ("strat_f_pct", "F — Sortie temporelle (60 min)"),
    ("strat_g_pct", "G — Laddering 33/33/34"),
]

COMPARATIF_CSV_FILE = "comparatif_strategies.csv"


def generer_et_envoyer_rapport_comparatif():
    if pd is None:
        send_telegram_message(
            "⚠️ Le module `pandas` n'est pas installé sur le serveur. "
            "Ajoute `pandas` à ton requirements.txt (Railway) puis redéploie "
            "pour activer /comparatif."
        )
        return

    if not os.path.isfile(LOG_FILE):
        send_telegram_message("⚠️ Aucune donnée pour le moment dans le fichier de log.")
        return

    try:
        df = pd.read_csv(LOG_FILE)
    except Exception as e:
        send_telegram_message(f"⚠️ Erreur de lecture du CSV : {e}")
        return

    if "signal_valide" not in df.columns:
        send_telegram_message("⚠️ Colonne signal_valide absente du CSV — relance le bot avec la version à jour.")
        return

    df_filtre = df[df["signal_valide"].astype(str).str.strip() == "True"].copy()

    if df_filtre.empty:
        send_telegram_message("⚠️ Aucun token n'a encore validé le critère de sélection (signal_valide=True) dans le log.")
        return

    lignes = []
    for colonne, libelle in STRATEGIES_COMPARATIF:
        if colonne not in df_filtre.columns:
            continue
        valeurs = pd.to_numeric(df_filtre[colonne], errors="coerce").dropna()
        if valeurs.empty:
            continue
        lignes.append({
            "strategie": libelle,
            "nb_trades": int(len(valeurs)),
            "roi_moyen_pct": round(float(valeurs.mean()), 2),
            "roi_median_pct": round(float(valeurs.median()), 2),
            "taux_succes_pct": round(float((valeurs > 0).sum() / len(valeurs) * 100), 1),
        })

    if not lignes:
        send_telegram_message("⚠️ Aucune colonne de stratégie exploitable dans le CSV pour le moment.")
        return

    df_comparatif = pd.DataFrame(lignes).sort_values("roi_moyen_pct", ascending=False)

    try:
        df_comparatif.to_csv(COMPARATIF_CSV_FILE, index=False, encoding="utf-8")
    except Exception as e:
        send_telegram_message(f"⚠️ Erreur d'écriture du CSV comparatif : {e}")
        return

    lignes_txt = "\n".join(
        f"• {r.strategie} — moy: {r.roi_moyen_pct:+.1f}% | méd: {r.roi_median_pct:+.1f}% | "
        f"succès: {r.taux_succes_pct:.0f}% ({r.nb_trades} trades)"
        for r in df_comparatif.itertuples()
    )
    caption = (
        f"📊 *Comparatif des stratégies* — {len(df_filtre)} tokens ayant validé le signal d'entrée\n\n"
        f"{lignes_txt}\n\n"
        f"ℹ️ Basé sur les bougies minute (OHLCV) sur la fenêtre suivie — voir le CSV joint pour le détail."
    )
    if len(caption) > 1024:
        send_telegram_message(caption[:4000])
        send_telegram_document(COMPARATIF_CSV_FILE, caption="📊 Comparatif détaillé des stratégies (tableau complet)")
    else:
        send_telegram_document(COMPARATIF_CSV_FILE, caption=caption)

    print(f"[comparatif] rapport envoyé — {len(df_filtre)} tokens, {len(lignes)} stratégies")


# ============================================================
# --- COMPARATIF PAR PROPOSITION DE CRITÈRES DE SÉLECTION ---
# ============================================================
FILTRES_PROPOSITIONS = [
    {
        "nom": "Actuel (signal_valide)",
        "condition": lambda df: df["signal_valide"].astype(str).str.strip() == "True",
    },
    {
        "nom": "Proposition 1 — Compromis équilibré",
        "condition": lambda df: (
            (pd.to_numeric(df["price_change_m3"], errors="coerce") >= 10)
            & (pd.to_numeric(df["ventes_m5"], errors="coerce") >= 100)
            & (pd.to_numeric(df["max_drawdown_before_peak"], errors="coerce") >= -50)
        ),
    },
    {
        "nom": "Proposition 2 — Approche large",
        "condition": lambda df: (
            (pd.to_numeric(df["price_change_m3"], errors="coerce") >= 5)
            & (pd.to_numeric(df["ventes_m5"], errors="coerce") >= 75)
            & (pd.to_numeric(df["max_drawdown_before_peak"], errors="coerce") >= -50)
        ),
    },
    {
        "nom": "Proposition 3 — Filtre très souple",
        "condition": lambda df: (
            (pd.to_numeric(df["price_change_m3"], errors="coerce") >= 0)
            & (pd.to_numeric(df["ventes_m5"], errors="coerce") >= 50)
            & (pd.to_numeric(df["max_drawdown_before_peak"], errors="coerce") >= -60)
        ),
    },
    {
        "nom": "M3+Ventes+DD — M3≥5 + Ventes M5≥75 + Max_DD≥-50%",
        "condition": lambda df: (
            (pd.to_numeric(df["price_change_m3"], errors="coerce") >= 5)
            & (pd.to_numeric(df["ventes_m5"], errors="coerce") >= 75)
            & (pd.to_numeric(df["max_drawdown_before_peak"], errors="coerce") >= -50)
        ),
    },
]

STRATEGIE_MISE_EN_AVANT = ("sim_3paliers_pct", "3 paliers pondérés")

PROPOSITIONS_CSV_FILE = "comparatif_propositions.csv"

COLONNES_REQUISES_PROPOSITIONS = (
    "signal_valide", "price_change_m3", "ventes_m5", "max_drawdown_before_peak",
)


def generer_et_envoyer_rapport_propositions():
    if pd is None:
        send_telegram_message(
            "⚠️ Le module `pandas` n'est pas installé sur le serveur. "
            "Ajoute `pandas` à ton requirements.txt (Railway) puis redéploie "
            "pour activer /propositions."
        )
        return

    if not os.path.isfile(LOG_FILE):
        send_telegram_message("⚠️ Aucune donnée pour le moment dans le fichier de log.")
        return

    try:
        df = pd.read_csv(LOG_FILE)
    except Exception as e:
        send_telegram_message(f"⚠️ Erreur de lecture du CSV : {e}")
        return

    colonnes_manquantes = [c for c in COLONNES_REQUISES_PROPOSITIONS if c not in df.columns]
    if colonnes_manquantes:
        send_telegram_message(
            f"⚠️ Colonnes manquantes dans le CSV : {', '.join(colonnes_manquantes)} — "
            "relance le bot avec la version à jour pour qu'elles soient journalisées."
        )
        return

    lignes_csv = []
    resumes_msg = []

    for proposition in FILTRES_PROPOSITIONS:
        try:
            masque = proposition["condition"](df)
        except Exception as e:
            resumes_msg.append(f"⚠️ {proposition['nom']} — erreur de filtrage : {e}")
            continue

        sous_ensemble = df[masque.fillna(False)]
        nb_tokens = len(sous_ensemble)

        if nb_tokens == 0:
            resumes_msg.append(f"*{proposition['nom']}* — 0 token éligible")
            continue

        stats_par_strategie = {}
        for colonne, libelle in STRATEGIES_COMPARATIF:
            if colonne not in sous_ensemble.columns:
                continue
            valeurs = pd.to_numeric(sous_ensemble[colonne], errors="coerce").dropna()
            if valeurs.empty:
                continue
            stats = {
                "roi_moyen_pct": round(float(valeurs.mean()), 2),
                "roi_median_pct": round(float(valeurs.median()), 2),
                "taux_succes_pct": round(float((valeurs > 0).sum() / len(valeurs) * 100), 1),
            }
            stats_par_strategie[colonne] = stats
            lignes_csv.append({
                "proposition": proposition["nom"],
                "nb_tokens_eligibles": nb_tokens,
                "strategie": libelle,
                **stats,
            })

        colonne_avant, libelle_avant = STRATEGIE_MISE_EN_AVANT
        if colonne_avant in stats_par_strategie:
            s = stats_par_strategie[colonne_avant]
            resumes_msg.append(
                f"*{proposition['nom']}* — {nb_tokens} tokens éligibles\n"
                f"  {libelle_avant} : moy {s['roi_moyen_pct']:+.1f}% | méd {s['roi_median_pct']:+.1f}% | "
                f"succès {s['taux_succes_pct']:.0f}%"
            )
        else:
            resumes_msg.append(f"*{proposition['nom']}* — {nb_tokens} tokens éligibles (pas de données '{libelle_avant}')")

    if not lignes_csv:
        send_telegram_message(
            "⚠️ Aucune proposition n'a trouvé de token éligible avec au moins une stratégie exploitable."
        )
        return

    try:
        pd.DataFrame(lignes_csv).to_csv(PROPOSITIONS_CSV_FILE, index=False, encoding="utf-8")
    except Exception as e:
        send_telegram_message(f"⚠️ Erreur d'écriture du CSV : {e}")
        return

    caption = (
        "📊 *Comparatif par proposition de critères de sélection*\n\n"
        + "\n\n".join(resumes_msg)
        + "\n\nℹ️ CSV joint : détail complet (toutes les stratégies) pour chaque proposition."
    )
    if len(caption) > 1024:
        send_telegram_message(caption[:4000])
        send_telegram_document(PROPOSITIONS_CSV_FILE, caption="📊 Comparatif détaillé par proposition (tableau complet)")
    else:
        send_telegram_document(PROPOSITIONS_CSV_FILE, caption=caption)

    print(f"[propositions] rapport envoyé — {len(FILTRES_PROPOSITIONS)} propositions comparées")


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
            return False
        return True
    except Exception as e:
        print(f"Erreur d'envoi Telegram : {e}")
        return False


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
        elif text in ("/csv_signaux", "/signaux", "/log_signaux"):
            send_telegram_document(SIGNAUX_LOG_FILE, caption="🎯 Historique des signaux (12 signaux + conditions croisées, ATH 30min/1h)")
        elif text in ("/csv_dextools", "/log_dextools"):
            send_telegram_document(DEXTOOLS_LOG_FILE, caption="📊 Historique canal DexToolsPublic (multiplicateur alerte → ATH 24h)")
        elif text in ("/csv_combos", "/combos", "/log_combos"):
            send_telegram_document(COMBOS_LOG_FILE, caption="🧩 Historique des combos personnalisés (entrée capturée au bon instant → ATH)")
        elif text in ("/comparatif", "/strategies", "/rapport_strategies"):
            generer_et_envoyer_rapport_comparatif()
        elif text in ("/propositions", "/filtres"):
            generer_et_envoyer_rapport_propositions()
        elif text == "/help":
            send_telegram_message(
                "Commandes disponibles :\n"
                "/csv — télécharger le fichier de log complet (tous les tokens suivis)\n"
                "/csv_signaux — télécharger l'historique des signaux : les 12 signaux 'purs' "
                "(entrée + multiplicateur ATH depuis l'entrée à 30min/1h, sans simulation de gain/stop-loss) "
                "ET les paires croisées (signal validé × condition parmi les conditions supplémentaires)\n"
                "/csv_dextools — télécharger le suivi du canal DexToolsPublic\n"
                "/csv_combos — télécharger l'historique des combos personnalisés (instant de validation, "
                "prix d'entrée capturé à cet instant, multiplicateur réel jusqu'à l'ATH depuis l'entrée)\n"
                "/comparatif — comparatif ROI moyen / médian / taux de succès de toutes les stratégies "
                "(A à G + anciennes), calculé uniquement sur les tokens ayant validé le signal d'entrée\n"
                "/propositions — compare le filtre actuel à plusieurs propositions de critères de sélection "
                "plus souples (price_change_m3, ventes_m5, max_drawdown_before_peak), avec le nombre de tokens "
                "éligibles et le ROI par stratégie pour chacune"
            )


def fetch_pair_data(mint):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        res = requests.get(url, timeout=5)
        if res.status_code != 200:
            return None
        pairs = res.json().get("pairs")
        if not pairs:
            return None
        # DexScreener peut retourner des paires sur PLUSIEURS chaînes pour
        # une même adresse si celle-ci "collisionne" avec un autre réseau.
        # On ne garde QUE les paires Solana, puis la plus liquide d'entre
        # elles — jamais la première paire brute retournée par l'API.
        pairs_solana = [p for p in pairs if p and p.get("chainId") == CHAIN_ID_SOLANA]
        if not pairs_solana:
            return None
        pairs_solana.sort(key=lambda p: (p.get("liquidity") or {}).get("usd", 0) or 0, reverse=True)
        return pairs_solana[0]
    except Exception as e:
        print(f"[fetch_pair_data] erreur pour {mint} : {e}")
        return None


def fetch_geckoterminal_trades(pool_address):
    if not pool_address:
        return None
    try:
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/trades"
        res = requests.get(url, timeout=8)
        if res.status_code != 200:
            print(f"[geckoterminal_trades] status={res.status_code} pour {pool_address}")
            return None
        items = res.json().get("data") or []
        trades = []
        for item in items:
            attrs = item.get("attributes") or {}
            kind = attrs.get("kind")
            ts_str = attrs.get("block_timestamp")
            if kind not in ("buy", "sell") or not ts_str:
                continue
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
            except (TypeError, ValueError):
                continue
            trades.append((ts, kind))
        trades.sort(key=lambda t: t[0])
        return trades or None
    except Exception as e:
        print(f"[geckoterminal_trades] erreur pour {pool_address} : {e}")
        return None


def _buy_ratio_depuis_trades(trades, start_time, fenetre_max_s):
    if not trades:
        return None, 0, 0
    achats = ventes = 0
    borne_sup = start_time + fenetre_max_s
    for ts, kind in trades:
        if ts < start_time or ts > borne_sup:
            continue
        if kind == "buy":
            achats += 1
        else:
            ventes += 1
    total = achats + ventes
    if total == 0:
        return None, 0, 0
    return round(achats / total, 3), achats, ventes


def calculer_buy_ratios_fallback_gecko(mint, pool_address, start_time):
    resultat_vide = {
        "reussi": False,
        "buy_ratio_2s": None, "achats_bruts_2s": None,
        "buy_ratio_5s": None, "tx_velocity_5s": None,
        "buy_ratio_10s": None, "buy_ratio_20s": None,
    }
    trades = fetch_geckoterminal_trades(pool_address)
    if not trades:
        print(f"[fallback_gecko] {mint} — aucune transaction récupérée via GeckoTerminal /trades")
        return resultat_vide

    br2, a2, v2 = _buy_ratio_depuis_trades(trades, start_time, 2)
    br5, a5, v5 = _buy_ratio_depuis_trades(trades, start_time, 5)
    br10, a10, v10 = _buy_ratio_depuis_trades(trades, start_time, 10)
    br20, a20, v20 = _buy_ratio_depuis_trades(trades, start_time, ANALYSE_20S_DURATION)

    if br20 is None and br10 is None:
        print(f"[fallback_gecko] {mint} — aucun trade GeckoTerminal dans la fenêtre des 20 premières secondes")
        return resultat_vide

    return {
        "reussi": True,
        "buy_ratio_2s": br2, "achats_bruts_2s": (a2 - v2) if (a2 or v2) else None,
        "buy_ratio_5s": br5, "tx_velocity_5s": a5 + v5,
        "buy_ratio_10s": br10, "buy_ratio_20s": br20,
    }


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
        # CORRECTIF (2026-08-19) : le point le plus HAUT observé pendant
        # ces 20 premières secondes doit aussi mettre à jour max_price /
        # max_price_time — jusqu'ici seul min_price était mis à jour ici.
        # Or max_price est LE filet de sécurité utilisé par get_true_ath_mc
        # (token_log.csv) et par calculer_ath_depuis_entree (signaux_log.csv)
        # pour compenser un ATH GeckoTerminal en retard/incomplet. Sans ce
        # correctif, ce filet de sécurité était lui-même aveugle aux 180
        # premières secondes — exactement la fenêtre la plus volatile,
        # où le vrai pic se produit le plus souvent. Confirmé sur
        # token_log.csv : plusieurs tokens avec multiplicateur final=1.000
        # alors que mult_10s/mult_30s dépassait déjà 1.25.
        high_second, high_mc = max(valides, key=lambda x: x[1])
        if mint in active_tokens:
            active_tokens[mint]["low_second_20s"] = low_second
            active_tokens[mint]["low_mult_20s"] = round(low_mult, 3)
            if low_mc < active_tokens[mint].get("min_price", low_mc):
                active_tokens[mint]["min_price"] = low_mc
                active_tokens[mint]["min_price_time"] = low_second
            if high_mc > active_tokens[mint].get("max_price", high_mc):
                active_tokens[mint]["max_price"] = high_mc
                active_tokens[mint]["max_price_time"] = high_second
        print(f"[analyse_20s] {data['symbol']} ({mint}) — point le plus bas à {low_second}s (x{low_mult:,.2f})")
    else:
        print(f"[analyse_20s] aucune donnée de market cap exploitable pour {mint}")

    deltas = []
    echecs_pair = 0
    for (e_prev, mc_prev, b_prev, s_prev), (e_next, mc_next, b_next, s_next) in zip(samples, samples[1:]):
        if None in (b_prev, s_prev, b_next, s_next):
            echecs_pair += 1
            continue
        delta_achats = max(b_next - b_prev, 0)
        delta_ventes = max(s_next - s_prev, 0)
        deltas.append((e_prev, e_next, delta_achats, delta_ventes))

    if not deltas:
        buy_ratio_diag = "pair_introuvable" if echecs_pair >= len(samples) - 1 else "aucune_activite_detectee"
    elif all((da + dv) == 0 for _, _, da, dv in deltas):
        buy_ratio_diag = "aucune_activite_detectee"
    else:
        buy_ratio_diag = "ok"

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

    if buy_ratio_diag != "ok":
        pool_address = data.get("pool_address")
        fallback = calculer_buy_ratios_fallback_gecko(mint, pool_address, start)
        if fallback["reussi"]:
            diag_original = buy_ratio_diag
            buy_ratio_2s = fallback["buy_ratio_2s"]
            achats_bruts_2s = fallback["achats_bruts_2s"]
            buy_ratio_5s = fallback["buy_ratio_5s"]
            tx_velocity_5s = fallback["tx_velocity_5s"]
            buy_ratio_10s = fallback["buy_ratio_10s"]
            buy_ratio_20s = fallback["buy_ratio_20s"]
            buy_ratio_diag = "ok_geckoterminal"
            print(
                f"[fallback_gecko] {data['symbol']} ({mint}) — bascule réussie sur GeckoTerminal /trades "
                f"(diag DexScreener original : {diag_original}) — buy_ratio_20s={buy_ratio_20s}"
            )

    if mint in active_tokens:
        active_tokens[mint]["buy_ratio_10s"] = buy_ratio_10s
        active_tokens[mint]["buy_ratio_20s"] = buy_ratio_20s
        active_tokens[mint]["achats_bruts_2s"] = achats_bruts_2s
        active_tokens[mint]["buy_ratio_2s"] = buy_ratio_2s
        active_tokens[mint]["tx_velocity_5s"] = tx_velocity_5s
        active_tokens[mint]["buy_ratio_5s"] = buy_ratio_5s
        active_tokens[mint]["buy_ratio_diag"] = buy_ratio_diag

        entry_stats = active_tokens[mint].get("entry_stats", {})
        price_change_m5 = entry_stats.get("price_change_m5")
        tx_accel = entry_stats.get("tx_accel")

        ratio_utilise = buy_ratio_20s
        ratio_source = "buy_ratio_20s"
        if ratio_utilise is None and FALLBACK_BUY_RATIO_ENABLED:
            achats_m5 = entry_stats.get("txns_buys_m5")
            ventes_m5 = entry_stats.get("txns_sells_m5")
            if achats_m5 is not None and ventes_m5 is not None and (achats_m5 + ventes_m5) > 0:
                ratio_utilise = round(achats_m5 / (achats_m5 + ventes_m5), 3)
                ratio_source = "fallback_m5"
        if ratio_utilise is None:
            ratio_source = "aucune_donnee"

        seuil_ratio = FALLBACK_BUY_RATIO_MIN if ratio_source == "fallback_m5" else TRIGGER_BUY_RATIO_20S_MIN

        signal_valide = (
            ratio_utilise is not None and ratio_utilise >= seuil_ratio
            and passe_filtres_triggers(initial_mc, price_change_m5, tx_accel)
        )
        active_tokens[mint]["signal_valide"] = signal_valide
        active_tokens[mint]["buy_ratio_source"] = ratio_source
        if ratio_source == "fallback_m5":
            print(f"[simulation] {data['symbol']} ({mint}) — buy_ratio_20s absent, fallback_m5 utilisé = {ratio_utilise}")

        if signal_valide and dernier_mc_valide:
            active_tokens[mint]["prix_entree_simule"] = dernier_mc_valide
            active_tokens[mint]["sl_prix_simule"] = dernier_mc_valide * (1 + SIMULATION_SL_PCT)
            active_tokens[mint]["position_statut"] = "ouverte"
            active_tokens[mint]["entry_time"] = time.time()
            print(f"[simulation] {data['symbol']} ({mint}) — signal validé, position simulée ouverte à ${dernier_mc_valide:,.0f}")
        else:
            active_tokens[mint]["position_statut"] = "signal_non_valide"

        evaluer_signal3_si_pret(mint)
        evaluer_signal_lp_light_si_pret(mint)

        # Signaux supplémentaires : évaluation à ~t=20s, avec les données
        # déjà en mémoire — aucun appel API en plus.
        if dernier_mc_valide:
            evaluer_signaux_supplementaires(mint, dernier_mc_valide)

    print(
        f"[analyse_20s] {data['symbol']} ({mint}) — "
        f"buy_ratio_2s={buy_ratio_2s} buy_ratio_5s={buy_ratio_5s} buy_ratio_10s={buy_ratio_10s} "
        f"buy_ratio_20s={buy_ratio_20s} achats_bruts_2s={achats_bruts_2s} buy_ratio_diag={buy_ratio_diag}"
    )


METRIQUES_ETENDUES_DUREE = 180
METRIQUES_ETENDUES_INTERVAL = 10


# ============================================================
# --- 12 SIGNAUX : entrée simulée (SANS trailing/simulation de gain) ---
# ============================================================
SIGNAUX_MISE_USD = MISE_SIMULATION_USD

SIGNAL1_MC_MIN = 10000
SIGNAL1_BUY_RATIO_20S_MIN = 0.55
SIGNAL1_SEUIL = 24.2

SIGNAL2_MC_MIN = 20000
SIGNAL2_BUY_RATIO_20S_MIN = 0.55
SIGNAL2_SEUIL = 15.3

SIGNAL3_MC_MIN = 40000
SIGNAL3_BUY_RATIO_20S_MIN = 0.63
SIGNAL3_SEUIL = 95.6

SIGNAL_LP_LIGHT_SEUIL = 95.6

SIGNAL4_PRICE_CHANGE_M3_MIN = 12
SIGNAL4_PRICE_CHANGE_M1_MIN = 10

signaux_traites = set()


def _lien_dexscreener(mint):
    return f"https://dexscreener.com/solana/{mint}"


def _lien_axiom(mint):
    return f"https://axiom.trade/meme/{mint}"


def _echapper_markdown(texte):
    if texte is None:
        return texte
    texte = str(texte)
    for car in ("_", "*", "`", "["):
        texte = texte.replace(car, f"\\{car}")
    return texte


def _construire_snapshot_avant(mint):
    """Capture un instantané des 48 métriques 'avant__' (AVANT_KEYS) tel
    qu'il apparaît dans active_tokens[mint] À L'INSTANT DE L'APPEL.

    CORRECTIF (2026-08-17) : auparavant, ces métriques étaient lues une
    seule fois dans row_rapport, construit à la clôture des 30 minutes de
    suivi (elapsed >= 1800) et réutilisé tel quel pour TOUS les signaux
    d'un même token, quel que soit leur entree_a_s réel (0s, 90s, 180s...).
    Résultat : 65,7% des lignes de signaux_log.csv décrivaient l'état du
    marché au mauvais instant (jusqu'à 28 min d'écart avec mc déjà
    doublé/triplé dans 14%/3% des cas).

    Cette fonction doit être appelée à l'ouverture de CHAQUE position
    (au moment précis où le signal se déclenche), pour que le snapshot
    'avant__' reflète l'état réel à l'entrée plutôt qu'un état futur."""
    data = active_tokens.get(mint)
    if not data:
        return {}

    entry_stats = data.get("entry_stats", {})
    initial_mc = data.get("initial_mc")
    liquidite_usd = data.get("liquidity_usd")
    pool_age_seconds = data.get("pool_age_seconds")
    pool_age_minutes = round(pool_age_seconds / 60, 2) if pool_age_seconds is not None else None
    ratio_liquidite_mc = round(_safe_div(liquidite_usd, initial_mc), 4) if initial_mc else None
    is_golden_window = (pool_age_seconds is not None and pool_age_seconds <= GOLDEN_WINDOW_MAX_SECONDS)

    return {
        "dex": data.get("dex"),
        "mc_initial": initial_mc,
        "liquidite_usd": liquidite_usd,
        "ratio_liquidite": entry_stats.get("liquidity_ratio"),
        "alertes_rugcheck": data.get("rugcheck_flags"),
        "pct_top10_holders": data.get("top10_pct"),
        "insiders_detectes": data.get("insiders_detected"),
        "nombre_holders": data.get("total_holders"),
        "bundle_detecte": data.get("bundle_detected"),
        "pool_age_seconds": pool_age_seconds,
        "pool_age_minutes": pool_age_minutes,
        "achats_m5": entry_stats.get("txns_buys_m5"),
        "ventes_m5": entry_stats.get("txns_sells_m5"),
        "volume_m5": entry_stats.get("volume_m5"),
        "achats_h1": entry_stats.get("txns_buys_h1"),
        "ventes_h1": entry_stats.get("txns_sells_h1"),
        "volume_h1": entry_stats.get("volume_h1"),
        "buy_ratio_10s": data.get("buy_ratio_10s"),
        "buy_ratio_2s": data.get("buy_ratio_2s"),
        "buy_ratio_5s": data.get("buy_ratio_5s"),
        "buy_ratio_1m": data.get("buy_ratio_1m"),
        "buy_ratio_diag": data.get("buy_ratio_diag"),
        "achats_bruts_2s": data.get("achats_bruts_2s"),
        "achats_10s": data.get("achats_10s"),
        "ventes_10s": data.get("ventes_10s"),
        "achats_m1": data.get("achats_m1"),
        "price_change_m5": entry_stats.get("price_change_m5"),
        "price_change_m1": data.get("price_change_m1"),
        "price_change_m3": data.get("price_change_m3"),
        "tx_accel": entry_stats.get("tx_accel"),
        "tx_velocity_5s": data.get("tx_velocity_5s"),
        "max_tx_per_second": data.get("max_tx_per_second"),
        "boost_detecte": data.get("boost_detecte", False),
        "nombre_boosts_actifs": data.get("nombre_boosts_actifs", 0),
        "unique_buyers_count": None,
        "volume_m1": data.get("volume_m1"),
        "ratio_volume_m1_m5": data.get("ratio_volume_m1_m5"),
        "ratio_achats_m1_m5": data.get("ratio_achats_m1_m5"),
        "buy_tx_ratio_m5": data.get("buy_tx_ratio_m5"),
        "mult_10s": data.get("mult_10s"),
        "mult_1m": data.get("mult_1m"),
        "ratio_liquidite_mc": ratio_liquidite_mc,
        "sell_ratio_1m": data.get("sell_ratio_1m"),
        "is_golden_window": is_golden_window,
        "signal_valide": data.get("signal_valide"),
        "buy_ratio_source": data.get("buy_ratio_source"),
        "seconde_prix_plus_bas_20s": data.get("low_second_20s"),
        "multiplicateur_plus_bas_20s": data.get("low_mult_20s"),
        "profil_dexscreener": data.get("profil_dexscreener", False),
        "site_web": data.get("site_web"),
        "twitter": data.get("twitter"),
        "telegram": data.get("telegram"),
    }


# Sous-ensemble de AVANT_KEYS qui varie réellement dans le temps après
# l'ouverture du token (tout le reste — entry_stats, buy_ratio_*,
# price_change_*, pool_age_seconds, etc. — est calculé UNE SEULE FOIS
# à un instant fixe (voir TIMING_INSTANT_T0/T20/T30/T180 plus bas) et
# n'est ensuite jamais réécrit dans active_tokens[mint]). Utilisé pour
# corriger le résidu de dérive avant__ sur les métriques croisées
# (voir evaluer_et_logger_signaux_croises) sans avoir à recapturer les
# 48 champs ni faire d'appel API.
VARIABLE_AVANT_KEYS = [
    "boost_detecte", "nombre_boosts_actifs",
    "profil_dexscreener", "site_web", "twitter", "telegram",
]


def _construire_snapshot_variable(mint):
    """Capture uniquement VARIABLE_AVANT_KEYS, à l'instant de l'appel."""
    data = active_tokens.get(mint)
    if not data:
        return {}
    return {
        "boost_detecte": data.get("boost_detecte", False),
        "nombre_boosts_actifs": data.get("nombre_boosts_actifs", 0),
        "profil_dexscreener": data.get("profil_dexscreener", False),
        "site_web": data.get("site_web"),
        "twitter": data.get("twitter"),
        "telegram": data.get("telegram"),
    }


def ouvrir_position_signal(mint, cle, nom_signal, formule_txt, valeur_calculee, seuil_txt, current_mc):
    """Enregistre l'entrée (prix + instant) pour un signal ou une
    condition, ET un snapshot des métriques 'avant__' à ce même instant
    (voir _construire_snapshot_avant). AUCUNE simulation de gain/
    stop-loss/trailing : le multiplicateur est calculé séparément via
    calculer_ath_depuis_entree, à la clôture des 30 min de suivi."""
    data = active_tokens.get(mint)
    if not data or not current_mc:
        return

    positions = data.setdefault("positions", {})
    if cle in positions:
        return

    positions[cle] = {
        "nom": nom_signal,
        "entry_price": current_mc,
        "entry_time": time.time(),
        "entry_elapsed_s": round(time.time() - data["start_time"]),
        "formule_txt": formule_txt,
        "valeur_calculee": valeur_calculee,
        "seuil_txt": seuil_txt,
        "avant_snapshot": _construire_snapshot_avant(mint),
    }

    print(f"[{cle}] {data['symbol']} ({mint}) — condition remplie, entrée enregistrée à ${current_mc:,.0f} (à {positions[cle]['entry_elapsed_s']}s)")


def evaluer_signal_base(mint):
    data = active_tokens.get(mint)
    if not data or data.get("signal_base_evalue"):
        return
    data["signal_base_evalue"] = True

    current_mc = data.get("initial_mc")
    ouvrir_position_signal(
        mint, "signal_base", "Signal de base (aucun filtre)",
        "aucune", "N/A", "aucun seuil",
        current_mc,
    )


def evaluer_signal_base_bis(mint):
    data = active_tokens.get(mint)
    if not data or data.get("signal_base_bis_evalue"):
        return
    data["signal_base_bis_evalue"] = True

    if not data.get("filtres_extra_ok"):
        print(f"[signal_base_bis] {data['symbol']} ({mint}) rejeté — filtres additionnels non remplis")
        return

    current_mc = data.get("initial_mc")
    ouvrir_position_signal(
        mint, "signal_base_bis", "Signal de base bis (+ filtres liquidité/trigger/pool_age)",
        "aucune (formule) + filtres additionnels", "N/A", "aucun seuil (formule)",
        current_mc,
    )


def evaluer_signal3_si_pret(mint):
    data = active_tokens.get(mint)
    if not data:
        return

    buy_ratio_20s = data.get("buy_ratio_20s")
    mult_30s = data.get("mult_30s")
    lp_locked_pct = data.get("lp_locked_pct")
    mc_initial = data.get("initial_mc")

    if buy_ratio_20s is None or not mult_30s or lp_locked_pct is None:
        return

    valeur = round(lp_locked_pct / mult_30s, 3)

    if not data.get("signal3_evalue"):
        data["signal3_evalue"] = True
        if not mc_initial or mc_initial < SIGNAL3_MC_MIN:
            print(f"[signal3] {data['symbol']} ({mint}) rejeté — mc_initial=${mc_initial}")
        elif buy_ratio_20s < SIGNAL3_BUY_RATIO_20S_MIN:
            print(f"[signal3] {data['symbol']} ({mint}) rejeté — buy_ratio_20s={buy_ratio_20s}")
        elif valeur > SIGNAL3_SEUIL:
            print(f"[signal3] {data['symbol']} ({mint}) rejeté — lp_locked_pct/mult_30s={valeur}")
        else:
            pair = fetch_pair_data(mint)
            current_mc = (pair.get("marketCap", 0) or pair.get("fdv", 0)) if pair else None
            if not current_mc:
                current_mc = (mc_initial or 1.0) * mult_30s
            ouvrir_position_signal(
                mint, "signal3", "Signal 3",
                "lp_locked_pct ÷ mult_30s", valeur, f"≤ {SIGNAL3_SEUIL}",
                current_mc,
            )

    if not data.get("signal3_bis_evalue"):
        data["signal3_bis_evalue"] = True
        if not mc_initial or mc_initial < SIGNAL3_MC_MIN:
            print(f"[signal3_bis] {data['symbol']} ({mint}) rejeté — mc_initial=${mc_initial}")
        elif buy_ratio_20s < SIGNAL3_BUY_RATIO_20S_MIN:
            print(f"[signal3_bis] {data['symbol']} ({mint}) rejeté — buy_ratio_20s={buy_ratio_20s}")
        elif valeur > SIGNAL3_SEUIL:
            print(f"[signal3_bis] {data['symbol']} ({mint}) rejeté — lp_locked_pct/mult_30s={valeur}")
        elif not data.get("filtres_extra_ok"):
            print(f"[signal3_bis] {data['symbol']} ({mint}) rejeté — filtres additionnels non remplis")
        else:
            pair = fetch_pair_data(mint)
            current_mc = (pair.get("marketCap", 0) or pair.get("fdv", 0)) if pair else None
            if not current_mc:
                current_mc = (mc_initial or 1.0) * mult_30s
            ouvrir_position_signal(
                mint, "signal3_bis", "Signal 3 bis (+ filtres liquidité/trigger/pool_age)",
                "lp_locked_pct ÷ mult_30s", valeur, f"≤ {SIGNAL3_SEUIL}",
                current_mc,
            )


def evaluer_signal_lp_light_si_pret(mint):
    data = active_tokens.get(mint)
    if not data:
        return

    mult_30s = data.get("mult_30s")
    lp_locked_pct = data.get("lp_locked_pct")

    if not mult_30s or lp_locked_pct is None:
        return

    valeur = round(lp_locked_pct / mult_30s, 3)

    if not data.get("signal_lp_light_evalue"):
        data["signal_lp_light_evalue"] = True
        if valeur > SIGNAL_LP_LIGHT_SEUIL:
            print(f"[signal_lp_light] {data['symbol']} ({mint}) rejeté — lp_locked_pct/mult_30s={valeur}")
        else:
            pair = fetch_pair_data(mint)
            current_mc = (pair.get("marketCap", 0) or pair.get("fdv", 0)) if pair else None
            if not current_mc:
                current_mc = (data.get("initial_mc") or 1.0) * mult_30s
            ouvrir_position_signal(
                mint, "signal_lp_light", "Signal LP light (sans filtre)",
                "lp_locked_pct ÷ mult_30s", valeur, f"≤ {SIGNAL_LP_LIGHT_SEUIL}",
                current_mc,
            )

    if not data.get("signal_lp_light_bis_evalue"):
        data["signal_lp_light_bis_evalue"] = True
        if valeur > SIGNAL_LP_LIGHT_SEUIL:
            print(f"[signal_lp_light_bis] {data['symbol']} ({mint}) rejeté — lp_locked_pct/mult_30s={valeur}")
        elif not data.get("filtres_extra_ok"):
            print(f"[signal_lp_light_bis] {data['symbol']} ({mint}) rejeté — filtres additionnels non remplis")
        else:
            pair = fetch_pair_data(mint)
            current_mc = (pair.get("marketCap", 0) or pair.get("fdv", 0)) if pair else None
            if not current_mc:
                current_mc = (data.get("initial_mc") or 1.0) * mult_30s
            ouvrir_position_signal(
                mint, "signal_lp_light_bis", "Signal LP light bis (+ filtres liquidité/trigger/pool_age)",
                "lp_locked_pct ÷ mult_30s", valeur, f"≤ {SIGNAL_LP_LIGHT_SEUIL}",
                current_mc,
            )


def evaluer_signal4_si_pret(mint, mc_reference=None):
    data = active_tokens.get(mint)
    if not data:
        return

    price_change_m3 = data.get("price_change_m3")
    price_change_m1 = data.get("price_change_m1")

    if price_change_m3 is None or price_change_m1 is None:
        return

    condition_ok = (
        price_change_m3 >= SIGNAL4_PRICE_CHANGE_M3_MIN
        and price_change_m1 >= SIGNAL4_PRICE_CHANGE_M1_MIN
    )
    valeur_txt = f"m3={price_change_m3}% / m1={price_change_m1}%"

    def _mc_actuel():
        current_mc = mc_reference
        if not current_mc:
            pair = fetch_pair_data(mint)
            current_mc = (pair.get("marketCap", 0) or pair.get("fdv", 0)) if pair else None
        if not current_mc:
            current_mc = data.get("initial_mc") or 1.0
        return current_mc

    if not data.get("signal4_evalue"):
        data["signal4_evalue"] = True
        if not condition_ok:
            print(
                f"[signal4] {data['symbol']} ({mint}) rejeté — "
                f"price_change_m3={price_change_m3} price_change_m1={price_change_m1}"
            )
        else:
            ouvrir_position_signal(
                mint, "signal4", "Signal 4 — Momentum M3+M1",
                "price_change_m3 & price_change_m1", valeur_txt,
                f"m3 ≥ {SIGNAL4_PRICE_CHANGE_M3_MIN}% et m1 ≥ {SIGNAL4_PRICE_CHANGE_M1_MIN}%",
                _mc_actuel(),
            )

    if not data.get("signal4_bis_evalue"):
        data["signal4_bis_evalue"] = True
        if not condition_ok:
            print(
                f"[signal4_bis] {data['symbol']} ({mint}) rejeté — "
                f"price_change_m3={price_change_m3} price_change_m1={price_change_m1}"
            )
        elif not data.get("filtres_extra_ok"):
            print(f"[signal4_bis] {data['symbol']} ({mint}) rejeté — filtres additionnels non remplis")
        else:
            ouvrir_position_signal(
                mint, "signal4_bis", "Signal 4 bis — Momentum M3+M1 (+ filtres liquidité/trigger/pool_age)",
                "price_change_m3 & price_change_m1", valeur_txt,
                f"m3 ≥ {SIGNAL4_PRICE_CHANGE_M3_MIN}% et m1 ≥ {SIGNAL4_PRICE_CHANGE_M1_MIN}%",
                _mc_actuel(),
            )


def evaluer_signaux_1_et_2(mint, price_change_m3, mc_reference):
    data = active_tokens.get(mint)
    if not data or data.get("signaux_1_2_evalues"):
        return
    data["signaux_1_2_evalues"] = True

    if price_change_m3 is None:
        print(f"[signal1/2] {data['symbol']} ({mint}) — price_change_m3 indisponible, signaux ignorés")
        return

    entry_stats = data.get("entry_stats", {})
    buy_ratio_20s = data.get("buy_ratio_20s")
    mc_initial = data.get("initial_mc")
    avg_order_size_sol = entry_stats.get("avg_order_size_sol")
    score_rugcheck = data.get("rugcheck_score")
    current_mc = mc_reference or data.get("initial_mc")
    filtres_extra_ok = data.get("filtres_extra_ok")

    filtres_base_1_ok = (
        mc_initial and mc_initial >= SIGNAL1_MC_MIN
        and buy_ratio_20s is not None and buy_ratio_20s >= SIGNAL1_BUY_RATIO_20S_MIN
        and avg_order_size_sol
    )
    valeur1 = round(price_change_m3 / avg_order_size_sol, 3) if filtres_base_1_ok else None
    signal1_ok = filtres_base_1_ok and valeur1 >= SIGNAL1_SEUIL

    if signal1_ok:
        ouvrir_position_signal(
            mint, "signal1", "Signal 1",
            "price_change_m3 ÷ avg_order_size_sol", valeur1, f"≥ {SIGNAL1_SEUIL}",
            current_mc,
        )
    else:
        print(
            f"[signal1] {data['symbol']} ({mint}) rejeté — "
            f"filtres_base_ok={bool(filtres_base_1_ok)} valeur={valeur1}"
        )

    if signal1_ok and filtres_extra_ok:
        ouvrir_position_signal(
            mint, "signal1_bis", "Signal 1 bis (+ filtres liquidité/trigger/pool_age)",
            "price_change_m3 ÷ avg_order_size_sol", valeur1, f"≥ {SIGNAL1_SEUIL}",
            current_mc,
        )
    elif signal1_ok:
        print(f"[signal1_bis] {data['symbol']} ({mint}) rejeté — filtres additionnels non remplis")

    filtres_base_2_ok = (
        mc_initial and mc_initial >= SIGNAL2_MC_MIN
        and buy_ratio_20s is not None and buy_ratio_20s >= SIGNAL2_BUY_RATIO_20S_MIN
        and score_rugcheck is not None
    )
    valeur2 = round(score_rugcheck * price_change_m3, 3) if filtres_base_2_ok else None
    signal2_ok = filtres_base_2_ok and valeur2 >= SIGNAL2_SEUIL

    if signal2_ok:
        ouvrir_position_signal(
            mint, "signal2", "Signal 2",
            "score_rugcheck × price_change_m3", valeur2, f"≥ {SIGNAL2_SEUIL}",
            current_mc,
        )
    else:
        print(
            f"[signal2] {data['symbol']} ({mint}) rejeté — "
            f"filtres_base_ok={bool(filtres_base_2_ok)} valeur={valeur2}"
        )

    if signal2_ok and filtres_extra_ok:
        ouvrir_position_signal(
            mint, "signal2_bis", "Signal 2 bis (+ filtres liquidité/trigger/pool_age)",
            "score_rugcheck × price_change_m3", valeur2, f"≥ {SIGNAL2_SEUIL}",
            current_mc,
        )
    elif signal2_ok:
        print(f"[signal2_bis] {data['symbol']} ({mint}) rejeté — filtres additionnels non remplis")


SIGNAUX_LOG_FILE = "signaux_log.csv"

# Colonnes existantes pour les 12 signaux "purs" — les colonnes
# trail25/30/40_* ont été retirées : plus de simulation de gain/SL, on ne
# garde que l'entrée et le multiplicateur ATH depuis cette entrée
# (calculé à 2 horizons : 30min / 1h — voir SIGNAUX_24H_ENABLED plus bas
# pour le suivi 24h, désactivé temporairement).
SIGNAUX_LOG_FIELDS_EXISTANTES = [
    "horodatage", "mint", "symbole", "signal",
    "formule", "valeur_calculee", "seuil",
    "mc_initial_token", "mc_entree", "entree_a_s",
    "buy_ratio_20s", "score_rugcheck", "avg_order_size_sol",
    "lp_locked_pct", "mult_30s",
    "lien_dexscreener", "lien_axiom",
]

SIGNAUX_LOG_FIELDS_FILTRES_EXTRA = ["filtres_extra_ok"]

# ATH depuis l'entrée, calculé à 2 horizons distincts avec la même
# fonction (calculer_ath_depuis_entree) et la même entrée (mc_entree /
# entree_a_s) — seule la fenêtre/résolution des bougies OHLCV change :
#   - _30m  : bougies minute couvrant les 30 premières minutes du token
#   - _1h   : bougies minute couvrant la 1ère heure du token
#
# CORRECTIF (2026-08-19) — SUIVI 24H DÉSACTIVÉ TEMPORAIREMENT : le suivi
# à 24h obligeait à attendre 24h avant de voir la moindre ligne dans
# signaux_log.csv, ce qui rendait le débogage des bugs d'ATH beaucoup
# trop lent (voir aussi le correctif ATH ci-dessous, dans
# calculer_ath_depuis_entree). Les colonnes _24h sont retirées pour
# l'instant ; SIGNAUX_24H_ENABLED / fetch_ohlcv_hour restent en place
# pour réactivation future sans tout réécrire.
SIGNAUX_LOG_FIELDS_ATH_DEPUIS_ENTREE = [
    "mc_max_depuis_entree_30m", "multiplicateur_depuis_entree_30m",
    "temps_jusquau_ath_depuis_entree_30m", "horodatage_ath_30m",
    "mc_max_depuis_entree_1h", "multiplicateur_depuis_entree_1h",
    "temps_jusquau_ath_depuis_entree_1h", "horodatage_ath_1h",
]

AVANT_KEYS = [
    "dex", "mc_initial", "liquidite_usd", "ratio_liquidite",
    "alertes_rugcheck", "pct_top10_holders", "insiders_detectes",
    "nombre_holders", "bundle_detecte", "pool_age_seconds", "pool_age_minutes",
    "achats_m5", "ventes_m5", "volume_m5", "achats_h1", "ventes_h1", "volume_h1",
    "buy_ratio_10s", "buy_ratio_2s", "buy_ratio_5s", "buy_ratio_1m", "buy_ratio_diag",
    "achats_bruts_2s", "achats_10s", "ventes_10s", "achats_m1",
    "price_change_m5", "price_change_m1", "price_change_m3",
    "tx_accel", "tx_velocity_5s", "max_tx_per_second",
    "boost_detecte", "nombre_boosts_actifs",
    "unique_buyers_count", "volume_m1", "ratio_volume_m1_m5",
    "ratio_achats_m1_m5", "buy_tx_ratio_m5",
    "mult_10s", "mult_1m", "ratio_liquidite_mc", "sell_ratio_1m",
    "is_golden_window", "signal_valide", "buy_ratio_source",
    "seconde_prix_plus_bas_20s", "multiplicateur_plus_bas_20s",
    "profil_dexscreener", "site_web", "twitter", "telegram",
]

APRES_KEYS = [
    "mc_max", "multiplicateur", "position_statut", "resultat_pct_simule",
    "time_to_2x", "time_to_3x", "max_drawdown_before_peak",
    "time_to_peak", "time_to_max_drawdown", "vitesse_chute_pct_par_min",
    "sim_remb_pct", "sim_remb_usd", "sim_ts20_pct", "sim_ts20_usd",
    "sim_ts30_pct", "sim_ts30_usd", "sim_3paliers_pct", "sim_3paliers_usd",
    "sim_ts_immediat_pct", "sim_ts_immediat_usd", "sim_peak_pct", "sim_peak_usd",
    "strat_a_pct", "strat_a_usd", "strat_b_pct", "strat_b_usd",
    "strat_c_pct", "strat_c_usd", "strat_d_pct", "strat_d_usd",
    "strat_e_pct", "strat_e_usd", "strat_f_pct", "strat_f_usd",
    "strat_g_pct", "strat_g_usd",
]

SIGNAUX_LOG_FIELDS = (
    SIGNAUX_LOG_FIELDS_EXISTANTES
    + ["horodatage_fin_suivi"]
    + SIGNAUX_LOG_FIELDS_FILTRES_EXTRA
    + SIGNAUX_LOG_FIELDS_ATH_DEPUIS_ENTREE
    + [f"avant__{k}" for k in AVANT_KEYS]
    + [f"apres__{k}" for k in APRES_KEYS]
)


def log_resultat_signal_csv(row):
    try:
        file_existe = os.path.isfile(SIGNAUX_LOG_FILE)
        with open(SIGNAUX_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SIGNAUX_LOG_FIELDS)
            if not file_existe:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[log_resultat_signal_csv] erreur : {e}")


def log_resultats_signaux(mint, data, row_rapport=None, ohlcv_30m=None, ohlcv_1h=None):
    """Écrit une ligne dans SIGNAUX_LOG_FILE pour chaque signal (parmi les
    12) qui s'est déclenché sur ce token. Le multiplicateur est calculé
    via calculer_ath_depuis_entree (avec filet de sécurité sur le suivi
    live — CORRECTIF 2026-08-19), appliquée deux fois avec la MÊME
    entrée (mc_entree / entree_a_s) mais des bougies OHLCV différentes :
      - ohlcv_30m : bougies minute sur les 30 premières minutes
      - ohlcv_1h  : bougies minute sur la 1ère heure
    Le suivi 24h est désactivé temporairement (voir SIGNAUX_24H_ENABLED)
    pour permettre de tester/corriger les bugs d'ATH sans attendre 24h à
    chaque itération. Aucune simulation de gain/stop-loss ici."""
    positions = data.get("positions") or {}
    entry_stats = data.get("entry_stats", {})
    initial_price = data.get("initial_price")
    initial_mc = data.get("initial_mc")
    start_time = data.get("start_time")
    live_max_mc = data.get("max_price")
    live_max_elapsed_s = data.get("max_price_time")

    for cle, pos in positions.items():
        cle_unique = (mint, cle)
        if cle_unique in signaux_traites:
            continue
        signaux_traites.add(cle_unique)

        mc_entree = pos.get("entry_price")
        entree_elapsed_s = pos.get("entry_elapsed_s")

        mc_max_30m, mult_30m, temps_30m, horo_30m = calculer_ath_depuis_entree(
            ohlcv_30m, initial_price, initial_mc, mc_entree, start_time, entree_elapsed_s,
            live_max_mc, live_max_elapsed_s,
        )
        mc_max_1h, mult_1h, temps_1h, horo_1h = calculer_ath_depuis_entree(
            ohlcv_1h, initial_price, initial_mc, mc_entree, start_time, entree_elapsed_s,
            live_max_mc, live_max_elapsed_s,
        )

        row_signal = {
            "horodatage": time.strftime("%Y-%m-%d %H:%M:%S"),
            "mint": mint,
            "symbole": data.get("symbol"),
            "signal": pos.get("nom"),
            "formule": pos.get("formule_txt"),
            "valeur_calculee": pos.get("valeur_calculee"),
            "seuil": pos.get("seuil_txt"),
            "mc_initial_token": data.get("initial_mc"),
            "mc_entree": mc_entree,
            "entree_a_s": entree_elapsed_s,
            "buy_ratio_20s": data.get("buy_ratio_20s"),
            "score_rugcheck": data.get("rugcheck_score"),
            "avg_order_size_sol": entry_stats.get("avg_order_size_sol"),
            "lp_locked_pct": data.get("lp_locked_pct"),
            "mult_30s": data.get("mult_30s"),
            "lien_dexscreener": data.get("dex_url"),
            "lien_axiom": _lien_axiom(mint),
            "filtres_extra_ok": data.get("filtres_extra_ok"),
            "mc_max_depuis_entree_30m": mc_max_30m,
            "multiplicateur_depuis_entree_30m": mult_30m,
            "temps_jusquau_ath_depuis_entree_30m": temps_30m,
            "horodatage_ath_30m": horo_30m,
            "mc_max_depuis_entree_1h": mc_max_1h,
            "multiplicateur_depuis_entree_1h": mult_1h,
            "temps_jusquau_ath_depuis_entree_1h": temps_1h,
            "horodatage_ath_1h": horo_1h,
        }

        # CORRECTIF (2026-08-17) : on utilise en priorité le snapshot
        # 'avant__' capturé À L'INSTANT DU DÉCLENCHEMENT du signal
        # (pos["avant_snapshot"], voir _construire_snapshot_avant), et
        # non plus row_rapport qui reflète l'état du token 30 min plus
        # tard, identique pour tous les signaux d'un même token.
        # Fallback sur row_rapport uniquement pour d'anciennes positions
        # créées avant ce correctif (pas de avant_snapshot stocké).
        avant_snapshot = pos.get("avant_snapshot")

        if row_rapport is not None:
            row_signal["horodatage_fin_suivi"] = row_rapport.get("horodatage")
            for k in APRES_KEYS:
                row_signal[f"apres__{k}"] = row_rapport.get(k)

        for k in AVANT_KEYS:
            if avant_snapshot:
                row_signal[f"avant__{k}"] = avant_snapshot.get(k)
            elif row_rapport is not None:
                row_signal[f"avant__{k}"] = row_rapport.get(k)

        log_resultat_signal_csv(row_signal)
    if positions:
        print(f"[signaux_log] {len(positions)} signal(aux) pur(s) journalisé(s) pour {mint} (ATH 30min/1h)")


# ============================================================
# --- ÉVALUATEUR GÉNÉRIQUE DE COMBOS PERSONNALISÉS ---
# ============================================================
_OPS = {
    "<=": operator.le, ">=": operator.ge,
    "<": operator.lt, ">": operator.gt,
    "==": operator.eq,
}

TIMING_INSTANT_T0 = {
    "liquidity_usd", "top10_pct", "initial_mc", "tx_accel",
    "achats_h1", "ventes_h1", "achats_m5", "ventes_m5",
    "volume_h1", "volume_m5", "ratio_liquidite", "ratio_liquidite_mc",
    "pool_age_seconds", "lp_locked_pct", "avg_order_size_sol",
    "price_change_m5", "insiders_detected", "bundle_detected", "total_holders",
}
TIMING_INSTANT_T20 = {
    "buy_ratio_2s", "buy_ratio_5s", "buy_ratio_10s", "buy_ratio_20s",
    "tx_velocity_5s", "achats_bruts_2s",
}
TIMING_INSTANT_T30 = {"mult_30s"}
TIMING_INSTANT_T180 = {
    "price_change_m1", "price_change_m3", "buy_ratio_1m", "buy_tx_ratio_m5",
    "achats_m1", "ventes_m1", "volume_m1", "mult_10s", "mult_1m",
    "achats_10s", "ventes_10s", "sell_ratio_1m", "max_tx_per_second",
    "ratio_volume_m1_m5", "ratio_achats_m1_m5",
}
# CORRECTIF (2026-08-19) : ces métriques évoluent réellement pendant les
# 180 premières secondes ET sont désormais échantillonnées en continu
# (voir analyser_metriques_etendues -> data['serie_metriques']), donc
# determiner_instant_combo peut chercher le VRAI instant de franchissement
# au lieu de supposer un checkpoint générique fixe (_instant_metrique).
# Gratuit : ces valeurs sont déjà collectées, on change juste comment on
# les interroge. Fusion de T180 (échantillonné toutes les 10s) et T30
# (mult_30s, qui est juste "mult_10s/30s/1m" vu à un instant différent —
# même série continue de multiplicateur, cf. analyser_metriques_etendues).
METRIQUES_SERIE_PROGRESSIVE = TIMING_INSTANT_T180 | TIMING_INSTANT_T30

# Seule catégorie de métrique en plus qui évolue réellement dans le temps
# au-delà de 180s (suivi manuel via boosts_history, pas de série continue
# comme METRIQUES_SERIE_PROGRESSIVE).
METRIQUES_CONTINUES = {"nombre_boosts_actifs", "boost_detecte"}
# Jamais calculée nulle part dans ce bot -> tout combo/condition qui
# l'utilise ne se déclenchera JAMAIS. On le signale explicitement plutôt
# que de l'ignorer silencieusement.
METRIQUES_INDISPONIBLES = {"unique_buyers_count"}

_MAPPING_ENTRY_STATS = {
    "tx_accel": "tx_accel",
    "achats_h1": "txns_buys_h1",
    "ventes_h1": "txns_sells_h1",
    "achats_m5": "txns_buys_m5",
    "ventes_m5": "txns_sells_m5",
    "volume_h1": "volume_h1",
    "volume_m5": "volume_m5",
    "ratio_liquidite": "liquidity_ratio",
    "ratio_liquidite_mc": "liquidity_ratio",  # même valeur dans ce bot (figée à t0)
    "avg_order_size_sol": "avg_order_size_sol",
    "price_change_m5": "price_change_m5",
}


def _instant_metrique(key):
    if key in TIMING_INSTANT_T0:
        return 0
    if key in TIMING_INSTANT_T20:
        return 20
    if key in TIMING_INSTANT_T30:
        return 30
    if key in TIMING_INSTANT_T180:
        return 180
    return 0


def _valeur_metrique(data, key):
    if key in _MAPPING_ENTRY_STATS:
        entry_stats = data.get("entry_stats") or {}
        return entry_stats.get(_MAPPING_ENTRY_STATS[key])
    if key == "unique_buyers_count":
        return None
    return data.get(key)


def _instant_croisement_serie(data, key, op, seuil):
    """CORRECTIF (2026-08-19) : cherche le PREMIER instant (elapsed, en
    secondes depuis le début du token) où `key` a franchi `seuil` selon
    `op`, en parcourant data['serie_metriques'] (snapshots enregistrés
    toutes les ~10s pendant les 180 premières secondes par
    analyser_metriques_etendues). Remplace l'ancienne hypothèse d'un
    checkpoint générique fixe (0/20/30/180s) par une vraie recherche
    temporelle, sans appel API supplémentaire (les données sont déjà en
    mémoire).

    Retourne None si la métrique n'a jamais franchi le seuil sur la
    fenêtre suivie (0-180s), ou si aucune série n'est disponible
    (positions anciennes créées avant ce correctif — dans ce cas
    l'appelant doit retomber sur l'ancien comportement, voir
    determiner_instant_combo)."""
    serie = data.get("serie_metriques") or []
    for snap in serie:  # déjà en ordre croissant d'elapsed (append séquentiel)
        valeur = snap.get(key)
        if valeur is None:
            continue
        try:
            if _OPS[op](valeur, seuil):
                return snap.get("elapsed")
        except TypeError:
            continue
    return None


def determiner_instant_combo(data, conditions):
    """conditions : liste de (metric_key, op, seuil).
    Retourne l'instant (secondes depuis start_time) où TOUTES les
    conditions sont devenues vraies simultanément, ou None si le combo
    ne s'est jamais entièrement validé sur ce token.

    CORRECTIF (2026-08-19) : pour les métriques classées
    METRIQUES_SERIE_PROGRESSIVE (T180 + mult_30s), l'instant est
    désormais déterminé par une vraie recherche dans la série temporelle
    échantillonnée (_instant_croisement_serie) au lieu du checkpoint
    générique fixe précédent (_instant_metrique). Fallback automatique
    sur l'ancien comportement si aucune série n'est disponible (positions
    ouvertes avant ce correctif, pas de régression sur les données déjà
    en cours de suivi au moment du déploiement)."""
    instant_max = 0
    for key, op, seuil in conditions:
        if key in METRIQUES_INDISPONIBLES:
            return None

        if key in METRIQUES_CONTINUES:
            hist = data.get("boosts_history") or []
            t_cross = None
            for t_h, val in hist:
                val_test = (val > 0) if key == "boost_detecte" else val
                if _OPS[op](val_test, seuil):
                    t_cross = t_h
                    break
            if t_cross is None:
                return None
            instant_max = max(instant_max, t_cross)
            continue

        if key in METRIQUES_SERIE_PROGRESSIVE and data.get("serie_metriques"):
            t_cross = _instant_croisement_serie(data, key, op, seuil)
            if t_cross is None:
                return None
            instant_max = max(instant_max, t_cross)
            continue

        valeur = _valeur_metrique(data, key)
        if valeur is None:
            return None
        try:
            if not _OPS[op](valeur, seuil):
                return None
        except TypeError:
            return None
        instant_max = max(instant_max, _instant_metrique(key))

    return instant_max


# --- Combos personnalisés (conservés tels quels, log séparé) ---
COMBOS_PERSONNALISES = [
      ("MC_entree<=16967", [("initial_mc", "<=", 16967)]),
    ("Achats_h1>=1531 + ventes_m5>=398 + sell_ratio_1m<=0.395 + pool_age>=6.13min",
     [("achats_h1", ">=", 1531), ("ventes_m5", ">=", 398), ("sell_ratio_1m", "<=", 0.395), ("pool_age_seconds", ">=", 6.13 * 60)]),
    ("MC_entree>16967 + M3>11.89% + liquidite>28324",
     [("initial_mc", ">", 16967), ("price_change_m3", ">", 11.89), ("liquidity_usd", ">", 28324)]),
    ("Boosts>=10 + volume_h1>=12702 + Top10>=26.36% + M3>=-36.8%",
     [("nombre_boosts_actifs", ">=", 10), ("volume_h1", ">=", 12702), ("top10_pct", ">=", 26.36), ("price_change_m3", ">=", -36.8)]),
    ("Liquidite<=8466 + Top10>=47.25%", [("liquidity_usd", "<=", 8466), ("top10_pct", ">=", 47.25)]),
    ("M3>=35.4% + tx_accel<=5.54", [("price_change_m3", ">=", 35.4), ("tx_accel", "<=", 5.54)]),
    ("MC_initial<=16526", [("initial_mc", "<=", 16526)]),
    ("Boosts>=10 + buy_ratio_5s<=0.748 + buy_tx_ratio_m5<=0.579", [("nombre_boosts_actifs", ">=", 10), ("buy_ratio_5s", "<=", 0.748), ("buy_tx_ratio_m5", "<=", 0.579)]),
    ("M3>=62.8%", [("price_change_m3", ">=", 62.8)]),
    ("MC_initial<=17509", [("initial_mc", "<=", 17509)]),
    ("buy_ratio_2s<=0.5 + boost_detecte", [("buy_ratio_2s", "<=", 0.5), ("boost_detecte", "==", True)]),
    ("M3>=51.5%", [("price_change_m3", ">=", 51.5)]),
    ("M3>=39.0%", [("price_change_m3", ">=", 39.0)]),
    ("Ratio_liquidite>=0.467", [("ratio_liquidite", ">=", 0.467)]),
    ("Ratio_liquidite_mc>=0.467", [("ratio_liquidite_mc", ">=", 0.467)]),
    ("M3>=40.3%", [("price_change_m3", ">=", 40.3)]),
    ("Ratio_liquidite>=0.480", [("ratio_liquidite", ">=", 0.480)]),
    ("MC_initial<=18324", [("initial_mc", "<=", 18324)]),
    ("M3>=8.7% + ventes_m5>=360 + Top10<=33.4%", [("price_change_m3", ">=", 8.7), ("ventes_m5", ">=", 360), ("top10_pct", "<=", 33.4)]),
    ("M3>=8.7% + achats_m5>=473 + Top10<=33.4%", [("price_change_m3", ">=", 8.7), ("achats_m5", ">=", 473), ("top10_pct", "<=", 33.4)]),
    ("Achats_h1>=2284 + buy_ratio_5s>=53.7%", [("achats_h1", ">=", 2284), ("buy_ratio_5s", ">=", 0.537)]),
    ("Boosts>=10 + tx_velocity_5s>=2 + pool_age>=4.92min", [("nombre_boosts_actifs", ">=", 10), ("tx_velocity_5s", ">=", 2), ("pool_age_seconds", ">=", 4.92 * 60)]),
    ("Boosts>=10 + buy_ratio_2s<=1 + pool_age>=4.27min", [("nombre_boosts_actifs", ">=", 10), ("buy_ratio_2s", "<=", 1), ("pool_age_seconds", ">=", 4.27 * 60)]),
    ("Boosts>=10 + buy_ratio_5s<=0.667 + volume_h1>=6561", [("nombre_boosts_actifs", ">=", 10), ("buy_ratio_5s", "<=", 0.667), ("volume_h1", ">=", 6561)]),
    ("buy_tx_ratio_m5>=0.537 + tx_accel<=9.37 + achats_m5>=712.8", [("buy_tx_ratio_m5", ">=", 0.537), ("tx_accel", "<=", 9.37), ("achats_m5", ">=", 712.8)]),
    ("Achats_m5>=712.8 + pool_age>=6.13min + volume_m1<=7424", [("achats_m5", ">=", 712.8), ("pool_age_seconds", ">=", 6.13 * 60), ("volume_m1", "<=", 7424)]),
    ("INDISPONIBLE: Unique_buyers>=4 + ratio_liquidite>=0.349 + pool_age>=6.13min", [("unique_buyers_count", ">=", 4), ("ratio_liquidite", ">=", 0.349), ("pool_age_seconds", ">=", 6.13 * 60)]),
    ("INDISPONIBLE: Unique_buyers>=39 + ventes_h1>=1280", [("unique_buyers_count", ">=", 39), ("ventes_h1", ">=", 1280)]),
    ("M3>=28.7% + pool_age>=7.8min", [("price_change_m3", ">=", 28.7), ("pool_age_seconds", ">=", 7.8 * 60)]),
    ("Achats_m5>=712.8 + achats_h1>=1801 + pool_age>=6.13min", [("achats_m5", ">=", 712.8), ("achats_h1", ">=", 1801), ("pool_age_seconds", ">=", 6.13 * 60)]),
    ("MC<=34700 + M3>=40.4%", [("initial_mc", "<=", 34700), ("price_change_m3", ">=", 40.4)]),
    ("Boosts>=10 + tx_accel>=1.42 + ventes_m5>=102.9", [("nombre_boosts_actifs", ">=", 10), ("tx_accel", ">=", 1.42), ("ventes_m5", ">=", 102.9)]),
    ("tx_accel<=8.39 + achats_m5>699", [("tx_accel", "<=", 8.39), ("achats_m5", ">", 699)]),
    ("INDISPONIBLE: Boosts>=10 + Unique_buyers>=5", [("nombre_boosts_actifs", ">=", 10), ("unique_buyers_count", ">=", 5)]),
    ("Achats_h1>=2272 + ventes_m5>=522", [("achats_h1", ">=", 2272), ("ventes_m5", ">=", 522)]),
    ("Achats_m5>=712.8 + achats_h1>=1801", [("achats_m5", ">=", 712.8), ("achats_h1", ">=", 1801)]),
    ("buy_ratio_2s<=0.415", [("buy_ratio_2s", "<=", 0.415)]),
    ("MC<=16526 + ratio_liquidite_mc>=0.410", [("initial_mc", "<=", 16526), ("ratio_liquidite_mc", ">=", 0.410)]),
    ("ratio_liquidite_mc>=0.497", [("ratio_liquidite_mc", ">=", 0.497)]),
]


COMBOS_LOG_FILE = "combos_personnalises_log.csv"
COMBOS_LOG_FIELDS = [
    "horodatage", "mint", "symbole", "combo",
    "instant_validation_s", "mc_entree",
    "mc_max_depuis_entree", "multiplicateur_depuis_entree",
    "temps_jusquau_ath_depuis_entree", "horodatage_ath",
    "lien_dexscreener",
]


def log_resultat_combo_csv(row):
    try:
        file_existe = os.path.isfile(COMBOS_LOG_FILE)
        with open(COMBOS_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COMBOS_LOG_FIELDS)
            if not file_existe:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[log_resultat_combo_csv] erreur : {e}")


def evaluer_et_logger_combos_personnalises(mint, data, ohlcv_list):
    """À appeler UNE FOIS par token, à la finalisation (t>=1800s dans
    monitor_ath), avec le même ohlcv_list déjà téléchargé pour
    token_log.csv / signaux_log.csv (zéro appel API supplémentaire).
    Ce log reste sur la fenêtre de 30 min (inchangé)."""
    initial_price = data.get("initial_price")
    initial_mc = data.get("initial_mc")
    start_time = data.get("start_time")
    live_max_mc = data.get("max_price")
    live_max_elapsed_s = data.get("max_price_time")

    for nom, conditions in COMBOS_PERSONNALISES:
        instant = determiner_instant_combo(data, conditions)
        if instant is None:
            continue  # combo jamais entièrement validé sur ce token

        # CORRECTIF (2026-08-19) : réutilise le prix live si l'instant
        # retenu est dans la fenêtre 0-180s couverte par le polling
        # (voir _mc_a_instant_live) au lieu de systématiquement estimer
        # via OHLCV — même logique que evaluer_et_logger_signaux_croises.
        mc_entree = _mc_a_instant_live(data, instant) if instant <= 180 else None
        if not mc_entree:
            mc_entree = _mc_a_instant(ohlcv_list, initial_price, initial_mc, start_time, instant)
        if not mc_entree:
            continue

        mc_max, multiplicateur, temps_ath, horo_ath = calculer_ath_depuis_entree(
            ohlcv_list, initial_price, initial_mc, mc_entree, start_time, instant,
            live_max_mc, live_max_elapsed_s,
        )

        log_resultat_combo_csv({
            "horodatage": time.strftime("%Y-%m-%d %H:%M:%S"),
            "mint": mint,
            "symbole": data.get("symbol"),
            "combo": nom,
            "instant_validation_s": instant,
            "mc_entree": round(mc_entree, 2),
            "mc_max_depuis_entree": mc_max,
            "multiplicateur_depuis_entree": multiplicateur,
            "temps_jusquau_ath_depuis_entree": temps_ath,
            "horodatage_ath": horo_ath,
            "lien_dexscreener": data.get("dex_url"),
        })


# ============================================================
# --- CONDITIONS SUPPLÉMENTAIRES, CROISÉES avec les 12 signaux ---
# Purement déclaratif : testées rétroactivement (comme
# COMBOS_PERSONNALISES) via determiner_instant_combo(), à la clôture des
# 30 minutes, en utilisant l'ohlcv_list déjà téléchargé pour ce token.
# Aucun appel API supplémentaire.
# ============================================================
SIGNAUX_SUPPLEMENTAIRES = [
    ("cond_01", "MC entrée ≤16 967", [("initial_mc", "<=", 16967)]),
    ("cond_02", "Boosts≥10 + buy_ratio_5s≤0,748 + buy_tx_ratio_m5≤0,579", [("nombre_boosts_actifs", ">=", 10), ("buy_ratio_5s", "<=", 0.748), ("buy_tx_ratio_m5", "<=", 0.579)]),
    ("cond_03", "Achats H1≥1531 + ventes M5≥398 + sell_ratio_1m≤0,395 + pool_age≥6,13min", [("achats_h1", ">=", 1531), ("ventes_m5", ">=", 398), ("sell_ratio_1m", "<=", 0.395), ("pool_age_seconds", ">=", 6.13 * 60)]),
    ("cond_04", "MC entrée>16967 + M3>11,89% + liquidité>28 324", [("initial_mc", ">", 16967), ("price_change_m3", ">", 11.89), ("liquidity_usd", ">", 28324)]),
    ("cond_05", "buy_ratio_2s≤0,5 + boost_detecte=True", [("buy_ratio_2s", "<=", 0.5), ("boost_detecte", "==", True)]),
    ("cond_06", "M3≥8,7% + achats M5≥473 + Top10≤33,4%", [("price_change_m3", ">=", 8.7), ("achats_m5", ">=", 473), ("top10_pct", "<=", 33.4)]),
    ("cond_07", "M3≥8,7% + ventes M5≥360 + Top10≤33,4%", [("price_change_m3", ">=", 8.7), ("ventes_m5", ">=", 360), ("top10_pct", "<=", 33.4)]),
    ("cond_08", "Achats H1≥2284 + buy_ratio_5s≥53,7%", [("achats_h1", ">=", 2284), ("buy_ratio_5s", ">=", 0.537)]),
    ("cond_09", "INDISPONIBLE: Unique buyers≥4 + ratio_liquidité≥0,349 + pool_age≥6,13", [("unique_buyers_count", ">=", 4), ("ratio_liquidite", ">=", 0.349), ("pool_age_seconds", ">=", 6.13 * 60)]),
    ("cond_10", "INDISPONIBLE: Unique buyers≥39 + ventes H1≥1280", [("unique_buyers_count", ">=", 39), ("ventes_h1", ">=", 1280)]),
    ("cond_11", "Boosts≥10 + volume H1≥12702 + Top10≥26,36% + M3≥-36,8%", [("nombre_boosts_actifs", ">=", 10), ("volume_h1", ">=", 12702), ("top10_pct", ">=", 26.36), ("price_change_m3", ">=", -36.8)]),
    ("cond_12", "buy_tx_ratio_m5≥0,537 + tx_accel≤9,37 + achats M5≥712,8", [("buy_tx_ratio_m5", ">=", 0.537), ("tx_accel", "<=", 9.37), ("achats_m5", ">=", 712.8)]),
    ("cond_13", "Achats H1≥2272 + ventes M5≥522", [("achats_h1", ">=", 2272), ("ventes_m5", ">=", 522)]),
    ("cond_14", "Boosts≥10 + buy_ratio_2s≤1 + pool_age≥4,27min", [("nombre_boosts_actifs", ">=", 10), ("buy_ratio_2s", "<=", 1), ("pool_age_seconds", ">=", 4.27 * 60)]),
    ("cond_15", "Achats M5≥712,8 + achats H1≥1801 + pool_age≥6,13", [("achats_m5", ">=", 712.8), ("achats_h1", ">=", 1801), ("pool_age_seconds", ">=", 6.13 * 60)]),
    ("cond_16", "Boosts≥10 + buy_ratio_5s≤0,667 + volume H1≥6561", [("nombre_boosts_actifs", ">=", 10), ("buy_ratio_5s", "<=", 0.667), ("volume_h1", ">=", 6561)]),
    ("cond_17", "tx_accel≤8,39 + achats M5>699", [("tx_accel", "<=", 8.39), ("achats_m5", ">", 699)]),
    ("cond_18", "Achats M5≥712,8 + pool_age≥6,13 + volume M1≤7424", [("achats_m5", ">=", 712.8), ("pool_age_seconds", ">=", 6.13 * 60), ("volume_m1", "<=", 7424)]),
    ("cond_19", "Boosts≥10 + tx_accel≥1,42 + ventes M5≥102,9", [("nombre_boosts_actifs", ">=", 10), ("tx_accel", ">=", 1.42), ("ventes_m5", ">=", 102.9)]),
    ("cond_20", "Boosts≥10 + tx_velocity_5s≥2 + pool_age≥4,92min", [("nombre_boosts_actifs", ">=", 10), ("tx_velocity_5s", ">=", 2), ("pool_age_seconds", ">=", 4.92 * 60)]),
    ("cond_21", "INDISPONIBLE: Boosts≥10 + unique_buyers≥5", [("nombre_boosts_actifs", ">=", 10), ("unique_buyers_count", ">=", 5)]),
    ("cond_22", "MC initial≤16 526", [("initial_mc", "<=", 16526)]),
    ("cond_23", "MC≤34 700 + M3≥40,4%", [("initial_mc", "<=", 34700), ("price_change_m3", ">=", 40.4)]),
    ("cond_24", "MC≤16 526 + ratio_liquidité/MC≥0,410", [("initial_mc", "<=", 16526), ("ratio_liquidite_mc", ">=", 0.410)]),
    ("cond_25", "Ratio_liquidité/MC≥0,497", [("ratio_liquidite_mc", ">=", 0.497)]),
    ("cond_26", "Ratio_liquidité≥0,480", [("ratio_liquidite", ">=", 0.480)]),
    ("cond_27", "MC initial≤17 509", [("initial_mc", "<=", 17509)]),
    ("cond_28", "Liquidité≤8466 + Top10≥47,25%", [("liquidity_usd", "<=", 8466), ("top10_pct", ">=", 47.25)]),
    ("cond_29", "buy_ratio_2s≤0,415", [("buy_ratio_2s", "<=", 0.415)]),
    ("cond_30", "MC initial≤18 324", [("initial_mc", "<=", 18324)]),
    ("cond_31", "Ratio_liquidité/MC≥0,467", [("ratio_liquidite_mc", ">=", 0.467)]),
    ("cond_32", "Ratio_liquidité≥0,467", [("ratio_liquidite", ">=", 0.467)]),
    ("cond_33", "M3≥35,4% + tx_accel≤5,54", [("price_change_m3", ">=", 35.4), ("tx_accel", "<=", 5.54)]),
    ("cond_34", "M3≥62,8%", [("price_change_m3", ">=", 62.8)]),
    ("cond_35", "M3≥40,3%", [("price_change_m3", ">=", 40.3)]),
    ("cond_36", "M3≥51,5%", [("price_change_m3", ">=", 51.5)]),
    ("cond_37", "M3≥39,0%", [("price_change_m3", ">=", 39.0)]),
    ("cond_38", "H — M3≥6 + Age≤90 + Boosts=0 + Ratio_vol≥0,5",
     [("price_change_m3", ">=", 6), ("pool_age_seconds", "<=", 90), ("nombre_boosts_actifs", "==", 0), ("ratio_volume_m1_m5", ">=", 0.5)]),
    ("cond_39", "D — M3≥6 + Age≤120 + Boosts≤10 + Ratio_vol≥0,70",
     [("price_change_m3", ">=", 6), ("pool_age_seconds", "<=", 120), ("nombre_boosts_actifs", "<=", 10), ("ratio_volume_m1_m5", ">=", 0.70)]),
    ("cond_40", "E — M3≥3 + Age≤600 + Boosts≤10 + Ratio_vol≥0,30",
     [("price_change_m3", ">=", 3), ("pool_age_seconds", "<=", 600), ("nombre_boosts_actifs", "<=", 10), ("ratio_volume_m1_m5", ">=", 0.30)]),
    ("cond_41", "F — M3≥6 + Age≤120 + Boosts≤10",
     [("price_change_m3", ">=", 6), ("pool_age_seconds", "<=", 120), ("nombre_boosts_actifs", "<=", 10)]),
    ("cond_42", "Principal — M3≥6,69 + tx_accel≥13,8 + M1≥-3,68",
     [("price_change_m3", ">=", 6.69), ("tx_accel", ">=", 13.8), ("price_change_m1", ">=", -3.68)]),
    ("cond_43", "Complémentaire — tx_accel≤8,8 + Age≤793 + MC≤24 840",
     [("tx_accel", "<=", 8.8), ("pool_age_seconds", "<=", 793), ("initial_mc", "<=", 24840)]),
    ("cond_44", "M3+TX+Liq — M3≥0 + tx_accel≥8,92 + Liq/MC≥0,3781",
     [("price_change_m3", ">=", 0), ("tx_accel", ">=", 8.92), ("ratio_liquidite_mc", ">=", 0.3781)]),
    ("cond_45", "Liq+Buy — Liq/MC≤0,3925 + Buy1m≤0,6344",
     [("ratio_liquidite_mc", "<=", 0.3925), ("buy_ratio_1m", "<=", 0.6344)]),
    ("cond_46", "M3+M1+Buy10s — M3≥5,122 + M1≥-1,41 + Buy10s≥0,40",
     [("price_change_m3", ">=", 5.122), ("price_change_m1", ">=", -1.41), ("buy_ratio_10s", ">=", 0.40)]),
    ("cond_47", "M3+Achats+Liq — M3≥2,058 + Achats M1/M5≥0,1804 + Liq/MC≥0,3533",
     [("price_change_m3", ">=", 2.058), ("ratio_achats_m1_m5", ">=", 0.1804), ("ratio_liquidite_mc", ">=", 0.3533)]),
    ("cond_48", "M3+Volume+Liq — M3≥2,058 + Vol M1/M5≥0,0858 + Liq/MC≥0,3533",
     [("price_change_m3", ">=", 2.058), ("ratio_volume_m1_m5", ">=", 0.0858), ("ratio_liquidite_mc", ">=", 0.3533)]),
    ("cond_49", "M3+Age — M3≥9,69 + Age≤88,2s",
     [("price_change_m3", ">=", 9.69), ("pool_age_seconds", "<=", 88.2)]),
    ("cond_50", "Age+Buy+Achats — Age≤25520 + Buy1m≥0,534 + Achats M5≤38,8",
     [("pool_age_seconds", "<=", 25520), ("buy_ratio_1m", ">=", 0.534), ("achats_m5", "<=", 38.8)]),
    ("cond_51", "Volume+Liq — Vol M1/M5≥0,1728 + Liq/MC≤0,3925",
     [("ratio_volume_m1_m5", ">=", 0.1728), ("ratio_liquidite_mc", "<=", 0.3925)]),
    ("cond_52", "M3+M1+Achats — M3≥7 + M1≥5 + Achats M1/M5≥0,90",
     [("price_change_m3", ">=", 7), ("price_change_m1", ">=", 5), ("ratio_achats_m1_m5", ">=", 0.90)]),
    ("cond_53", "M3+Ventes — M3≥7 + Ventes M5≤25",
     [("price_change_m3", ">=", 7), ("ventes_m5", "<=", 25)]),
    ("cond_54", "M3+Age — M3≥7 + Age≤60s",
     [("price_change_m3", ">=", 7), ("pool_age_seconds", "<=", 60)]),
    ("cond_55", "M3+Age+TX — M3≥7 + Age≤90s + tx_accel≥10",
     [("price_change_m3", ">=", 7), ("pool_age_seconds", "<=", 90), ("tx_accel", ">=", 10)]),
    ("cond_56", "M3+Age+Boosts — M3≥12 + Age≤60s + Boosts≤5",
     [("price_change_m3", ">=", 12), ("pool_age_seconds", "<=", 60), ("nombre_boosts_actifs", "<=", 5)]),
    ("cond_57", "M3+Age+Boosts — M3≥12 + Age≤180s + Boosts≤5",
     [("price_change_m3", ">=", 12), ("pool_age_seconds", "<=", 180), ("nombre_boosts_actifs", "<=", 5)]),
    ("cond_58", "M3+Age — M3≥24,1 + Age≤66s",
     [("price_change_m3", ">=", 24.1), ("pool_age_seconds", "<=", 66)]),
    ("cond_59", "M1+MC — M1≥24,4 + MC_initial≤38,5k",
     [("price_change_m1", ">=", 24.4), ("initial_mc", "<=", 38500)]),
    ("cond_60", "M3+Age — M3≥24,1 + Age≤385s",
     [("price_change_m3", ">=", 24.1), ("pool_age_seconds", "<=", 385)]),
    ("cond_61", "M3+Liq — M3≥24,1 + Ratio_Liq/MC≥0,361",
     [("price_change_m3", ">=", 24.1), ("ratio_liquidite_mc", ">=", 0.361)]),
    ("cond_62", "M3+MC — M3≥24,1 + MC_initial≤33,3k",
     [("price_change_m3", ">=", 24.1), ("initial_mc", "<=", 33300)]),
    ("cond_63", "M3+M1+Age — M3≥6,96 + M1≥5,48 + Age≤50s",
     [("price_change_m3", ">=", 6.96), ("price_change_m1", ">=", 5.48), ("pool_age_seconds", "<=", 50)]),
    ("cond_64", "M3+Age+TX — M3≥6,96 + Age≤50s + tx_accel≥10,8",
     [("price_change_m3", ">=", 6.96), ("pool_age_seconds", "<=", 50), ("tx_accel", ">=", 10.8)]),
    ("cond_65", "TX+Ventes — tx_accel≥10,8 + Ventes M5≤32",
     [("tx_accel", ">=", 10.8), ("ventes_m5", "<=", 32)]),
    ("cond_66", "Achats — Achats M1/M5≥2,73",
     [("ratio_achats_m1_m5", ">=", 2.73)]),
]


def evaluer_signaux_supplementaires(mint, current_mc):
    """DÉCLARATIF UNIQUEMENT — cette fonction existe pour compat/future
    utilisation en direct, mais dans le flux actuel, le croisement des
    conditions supplémentaires avec les 12 signaux se fait de façon
    rétroactive à la clôture des 30 minutes via
    evaluer_et_logger_signaux_croises(), pas ici. On la laisse sans effet
    de bord bloquant : elle ne fait rien d'utile seule (le croisement
    retient toujours le DERNIER instant validé, jamais un instant "en
    direct" partiel)."""
    return


signaux_croises_traites = set()


def evaluer_et_logger_signaux_croises(mint, data, ohlcv_30m, ohlcv_1h, row_rapport=None):
    """Pour chaque signal parmi les 12 qui s'est RÉELLEMENT déclenché sur
    ce token (data['positions']), croise avec chacune des conditions
    supplémentaires. Si le signal ne s'est jamais déclenché, aucune ligne
    n'est produite pour les conditions supplémentaires (positions vide ->
    return direct).

    Instant d'entrée retenu = max(instant du signal, instant de
    validation de la condition), désormais déterminé via une vraie
    recherche temporelle pour les métriques T180/T30 (voir
    determiner_instant_combo, corrigé le 2026-08-19).

    Le multiplicateur ATH depuis cette entrée est ensuite calculé aux 2
    horizons (30min/1h) via calculer_ath_depuis_entree (avec filet de
    sécurité sur le suivi live — CORRECTIF 2026-08-19), comme pour les 12
    signaux purs (voir log_resultats_signaux). Le suivi 24h est
    désactivé temporairement (voir SIGNAUX_24H_ENABLED)."""
    positions = data.get("positions") or {}
    if not positions:
        return  # aucun des 12 signaux déclenché -> rien à croiser

    initial_price = data.get("initial_price")
    initial_mc = data.get("initial_mc")
    start_time = data.get("start_time")
    live_max_mc = data.get("max_price")
    live_max_elapsed_s = data.get("max_price_time")

    nb_lignes = 0

    for signal_cle, pos in positions.items():
        instant_signal = pos.get("entry_elapsed_s")
        if instant_signal is None:
            continue

        for cond_cle, cond_nom, conditions in SIGNAUX_SUPPLEMENTAIRES:
            cle_unique = (mint, signal_cle, cond_cle)
            if cle_unique in signaux_croises_traites:
                continue

            instant_condition = determiner_instant_combo(data, conditions)
            if instant_condition is None:
                continue  # condition jamais validée sur ce token

            signaux_croises_traites.add(cle_unique)
            instant_final = max(instant_signal, instant_condition)

            if instant_condition <= instant_signal:
                # La condition s'est validée avant ou en même temps que le
                # signal (instant_final == instant_signal) : on réutilise
                # le prix réellement capturé EN LIVE à l'ouverture du signal
                # (pos["entry_price"]) — toujours la source la plus fiable.
                mc_entree = pos.get("entry_price")
            else:
                # CORRECTIF (2026-08-19) : la condition s'est validée
                # APRÈS le signal. Auparavant on reconstruisait
                # systématiquement mc_entree via l'OHLCV GeckoTerminal
                # (_mc_a_instant), source d'un écart mesuré jusqu'à x180
                # sur signaux_log.csv. Désormais, si instant_final tombe
                # dans la fenêtre 0-180s couverte par le polling live
                # (analyser_metriques_etendues, toutes les 10s), on
                # réutilise ce prix RÉEL (_mc_a_instant_live) au lieu de
                # l'estimer. On ne retombe sur l'OHLCV que pour les
                # instants > 180s (essentiellement les conditions basées
                # sur boosts_history) ou si aucune donnée live n'est
                # disponible (positions ouvertes avant ce correctif).
                mc_entree = None
                if instant_final <= 180:
                    mc_entree = _mc_a_instant_live(data, instant_final)
                if not mc_entree:
                    mc_entree = _mc_a_instant(ohlcv_30m, initial_price, initial_mc, start_time, instant_final)

            if not mc_entree:
                continue

            mc_max_30m, mult_30m, temps_30m, horo_30m = calculer_ath_depuis_entree(
                ohlcv_30m, initial_price, initial_mc, mc_entree, start_time, instant_final,
                live_max_mc, live_max_elapsed_s,
            )
            mc_max_1h, mult_1h, temps_1h, horo_1h = calculer_ath_depuis_entree(
                ohlcv_1h, initial_price, initial_mc, mc_entree, start_time, instant_final,
                live_max_mc, live_max_elapsed_s,
            )

            formule_condition = " + ".join(f"{k} {o} {s}" for k, o, s in conditions)

            row_signal = {
                "horodatage": time.strftime("%Y-%m-%d %H:%M:%S"),
                "mint": mint,
                "symbole": data.get("symbol"),
                "signal": f"{pos.get('nom')} + {cond_nom}",
                "formule": f"[{signal_cle}: {pos.get('formule_txt')}] ET [{cond_cle}: {formule_condition}]",
                "valeur_calculee": f"signal@{instant_signal}s | condition@{instant_condition}s | retenu@{instant_final}s",
                "seuil": pos.get("seuil_txt"),
                "mc_initial_token": data.get("initial_mc"),
                "mc_entree": round(mc_entree, 2),
                "entree_a_s": instant_final,
                "buy_ratio_20s": data.get("buy_ratio_20s"),
                "score_rugcheck": data.get("rugcheck_score"),
                "avg_order_size_sol": (data.get("entry_stats") or {}).get("avg_order_size_sol"),
                "lp_locked_pct": data.get("lp_locked_pct"),
                "mult_30s": data.get("mult_30s"),
                "lien_dexscreener": data.get("dex_url"),
                "lien_axiom": _lien_axiom(mint),
                "filtres_extra_ok": data.get("filtres_extra_ok"),
                "mc_max_depuis_entree_30m": mc_max_30m,
                "multiplicateur_depuis_entree_30m": mult_30m,
                "temps_jusquau_ath_depuis_entree_30m": temps_30m,
                "horodatage_ath_30m": horo_30m,
                "mc_max_depuis_entree_1h": mc_max_1h,
                "multiplicateur_depuis_entree_1h": mult_1h,
                "temps_jusquau_ath_depuis_entree_1h": temps_1h,
                "horodatage_ath_1h": horo_1h,
            }

            # CORRECTIF (2026-08-17) : même logique que log_resultats_signaux,
            # PLUS un correctif du résidu signalé le même jour. Base =
            # pos["avant_snapshot"], capturé à instant_signal — correct pour
            # 42 des 48 champs AVANT_KEYS car ceux-ci sont figés dès
            # l'ouverture du token et ne changent plus jamais dans
            # active_tokens[mint] (voir TIMING_INSTANT_T0/T20/T30/T180).
            # Seuls les VARIABLE_AVANT_KEYS (boosts, profil dexscreener)
            # évoluent réellement dans le temps : si la condition s'est
            # validée APRÈS le signal (instant_condition > instant_signal),
            # on les remplace par le snapshot capturé en direct à
            # instant_condition (voir monitor_ath) — sans ça ils
            # refléteraient l'état à instant_signal au lieu de instant_final.
            avant_snapshot = pos.get("avant_snapshot")
            if instant_condition > instant_signal:
                cond_snap = (data.get("conditions_variable_snapshots") or {}).get(cond_cle)
                if cond_snap:
                    avant_snapshot = dict(avant_snapshot or {})
                    for k in VARIABLE_AVANT_KEYS:
                        avant_snapshot[k] = cond_snap.get(k)

            if row_rapport is not None:
                row_signal["horodatage_fin_suivi"] = row_rapport.get("horodatage")
                for k in APRES_KEYS:
                    row_signal[f"apres__{k}"] = row_rapport.get(k)

            for k in AVANT_KEYS:
                if avant_snapshot:
                    row_signal[f"avant__{k}"] = avant_snapshot.get(k)
                elif row_rapport is not None:
                    row_signal[f"avant__{k}"] = row_rapport.get(k)

            log_resultat_signal_csv(row_signal)
            nb_lignes += 1

    if nb_lignes:
        print(f"[signaux_croises] {nb_lignes} paire(s) signal×condition journalisée(s) pour {mint} (ATH 30min/1h)")


def analyser_metriques_etendues(mint):
    data = active_tokens.get(mint)
    if not data:
        return
    initial_mc = data.get("initial_mc") or 1.0
    initial_price = data.get("initial_price")
    start = data["start_time"]

    samples = []

    # CORRECTIF (2026-08-19) : deltas + max_tx_par_seconde recalculés
    # progressivement à CHAQUE tick (en plus du calcul final identique à
    # l'ancienne version, conservé plus bas sans aucune modification)
    # pour alimenter data['serie_metriques'] — voir determiner_instant_combo.
    deltas_progressifs = []
    max_tx_par_seconde_courant = 0
    entry_stats_ref = data.get("entry_stats", {})
    volume_m5_alerte = entry_stats_ref.get("volume_m5")
    achats_m5_alerte = entry_stats_ref.get("txns_buys_m5")
    ventes_m5_alerte = entry_stats_ref.get("txns_sells_m5")
    buy_tx_ratio_m5_fixe = round(
        _safe_div(achats_m5_alerte, (achats_m5_alerte or 0) + (ventes_m5_alerte or 0)), 4
    )

    prochain_t = 0
    while prochain_t <= METRIQUES_ETENDUES_DUREE:
        attente = (start + prochain_t) - time.time()
        if attente > 0:
            time.sleep(attente)
        elapsed = round(time.time() - start)
        pair = fetch_pair_data(mint)
        mc = price_usd = buys_m5 = sells_m5 = volume_m5 = None
        if pair:
            mc = pair.get("marketCap", 0) or pair.get("fdv", 0)
            price_usd = _to_float(pair.get("priceUsd"))
            txns_m5 = (pair.get("txns") or {}).get("m5") or {}
            buys_m5 = txns_m5.get("buys")
            sells_m5 = txns_m5.get("sells")
            volume_m5 = (pair.get("volume") or {}).get("m5")
        samples.append((elapsed, mc, price_usd, buys_m5, sells_m5, volume_m5))

        if mc and mint in active_tokens and mc < active_tokens[mint].get("min_price", mc):
            active_tokens[mint]["min_price"] = mc
            active_tokens[mint]["min_price_time"] = elapsed

        # CORRECTIF (2026-08-19) : même correctif que dans
        # analyser_20_premieres_secondes — max_price doit aussi être mis
        # à jour ici (polling toutes les 10s jusqu'à 180s), pas seulement
        # dans monitor_ath (toutes les 120s). Sinon le filet de sécurité
        # utilisé par get_true_ath_mc / calculer_ath_depuis_entree reste
        # aveugle à la fenêtre la plus volatile du token.
        if mc and mint in active_tokens and mc > active_tokens[mint].get("max_price", mc):
            active_tokens[mint]["max_price"] = mc
            active_tokens[mint]["max_price_time"] = elapsed

        if elapsed >= 30 and mint in active_tokens and not active_tokens[mint].get("mult_30s"):
            if mc:
                active_tokens[mint]["mult_30s"] = round(mc / initial_mc, 4)
            evaluer_signal3_si_pret(mint)
            evaluer_signal_lp_light_si_pret(mint)

        if elapsed >= 180 and initial_price and price_usd and mint in active_tokens and not active_tokens[mint].get("signaux_1_2_evalues"):
            price_change_m3_instant = round((price_usd / initial_price - 1) * 100, 2)
            evaluer_signaux_1_et_2(mint, price_change_m3_instant, mc)

        # CORRECTIF (2026-08-19) : snapshot progressif des métriques
        # T180/T30, calculé à CHAQUE tick avec les données déjà
        # disponibles à cet instant — permet à determiner_instant_combo
        # de trouver le VRAI instant de franchissement d'un seuil au
        # lieu de supposer un checkpoint fixe (0/20/30/180s). Zéro appel
        # API supplémentaire : on garde juste en mémoire, un peu plus
        # longtemps et de façon plus granulaire, ce qui est déjà
        # récupéré ci-dessus par fetch_pair_data.
        if len(samples) >= 2 and mint in active_tokens:
            e_prev, _, _, b_prev, s_prev, v_prev = samples[-2]
            e_next, mc_next, price_next, b_next, s_next, v_next = samples[-1]

            if None not in (b_prev, s_prev, b_next, s_next):
                delta_achats = max(b_next - b_prev, 0)
                delta_ventes = max(s_next - s_prev, 0)
                delta_volume = None
                if v_prev is not None and v_next is not None:
                    delta_volume = max(v_next - v_prev, 0)
                duree = max(e_next - e_prev, 1)
                deltas_progressifs.append((e_prev, e_next, delta_achats, delta_ventes, delta_volume))

                tx_par_seconde = (delta_achats + delta_ventes) / duree
                if tx_par_seconde > max_tx_par_seconde_courant:
                    max_tx_par_seconde_courant = tx_par_seconde

            achats_10s_prog = sum(da for _, e_fin, da, _, _ in deltas_progressifs if e_fin <= 10)
            ventes_10s_prog = sum(dv for _, e_fin, _, dv, _ in deltas_progressifs if e_fin <= 10)
            achats_m1_prog = sum(da for _, e_fin, da, _, _ in deltas_progressifs if e_fin <= 60)
            ventes_m1_prog = sum(dv for _, e_fin, _, dv, _ in deltas_progressifs if e_fin <= 60)
            volume_m1_prog = sum(
                dv for _, e_fin, _, _, dv in deltas_progressifs if e_fin <= 60 and dv is not None
            )

            buy_ratio_1m_prog = round(_safe_div(achats_m1_prog, achats_m1_prog + ventes_m1_prog), 3)
            sell_ratio_1m_prog = round(_safe_div(ventes_m1_prog, achats_m1_prog + ventes_m1_prog), 3)
            ratio_volume_m1_m5_prog = round(_safe_div(volume_m1_prog, volume_m5_alerte), 4)
            ratio_achats_m1_m5_prog = round(_safe_div(achats_m1_prog, achats_m5_alerte), 4)

            mult_actuel = round(mc_next / initial_mc, 4) if mc_next else None
            price_change_actuel = (
                round((price_next / initial_price - 1) * 100, 2)
                if (initial_price and price_next) else None
            )

            active_tokens[mint].setdefault("serie_metriques", []).append({
                "elapsed": e_next,
                "achats_10s": achats_10s_prog,
                "ventes_10s": ventes_10s_prog,
                "achats_m1": achats_m1_prog,
                "ventes_m1": ventes_m1_prog,
                "volume_m1": round(volume_m1_prog, 2) if volume_m1_prog is not None else None,
                "ratio_volume_m1_m5": ratio_volume_m1_m5_prog,
                "ratio_achats_m1_m5": ratio_achats_m1_m5_prog,
                "buy_ratio_1m": buy_ratio_1m_prog,
                "sell_ratio_1m": sell_ratio_1m_prog,
                "buy_tx_ratio_m5": buy_tx_ratio_m5_fixe,
                # mult_10s/30s/1m et price_change_m1/m3 sont, dans les
                # faits, la MÊME série continue (multiplicateur courant /
                # variation de prix depuis le début) — on l'expose sous
                # les 3 (resp. 2) noms utilisés par les conditions pour
                # que determiner_instant_combo trouve le bon champ quel
                # que soit celui référencé dans SIGNAUX_SUPPLEMENTAIRES /
                # COMBOS_PERSONNALISES.
                "mult_10s": mult_actuel,
                "mult_30s": mult_actuel,
                "mult_1m": mult_actuel,
                "price_change_m1": price_change_actuel,
                "price_change_m3": price_change_actuel,
                "max_tx_per_second": round(max_tx_par_seconde_courant, 2),
            })

        prochain_t += METRIQUES_ETENDUES_INTERVAL
        if mint not in active_tokens:
            return

    # --- Valeurs finales : logique ORIGINALE, inchangée -------------
    # (recalcul complet sur l'ensemble des samples, comme avant ce
    # correctif — ce bloc alimente les champs scalaires utilisés
    # ailleurs, ex. token_log.csv. La série progressive ci-dessus est un
    # AJOUT, pas un remplacement.)
    deltas = []
    max_tx_par_seconde = 0
    for (e_prev, _, _, b_prev, s_prev, v_prev), (e_next, _, _, b_next, s_next, v_next) in zip(samples, samples[1:]):
        if None in (b_prev, s_prev, b_next, s_next):
            continue
        delta_achats = max(b_next - b_prev, 0)
        delta_ventes = max(s_next - s_prev, 0)
        delta_volume = None
        if v_prev is not None and v_next is not None:
            delta_volume = max(v_next - v_prev, 0)
        duree = max(e_next - e_prev, 1)
        deltas.append((e_prev, e_next, delta_achats, delta_ventes, delta_volume))

        tx_par_seconde = (delta_achats + delta_ventes) / duree
        if tx_par_seconde > max_tx_par_seconde:
            max_tx_par_seconde = tx_par_seconde

    def _cumule(fenetre_max_s):
        achats = sum(da for _, e_fin, da, _, _ in deltas if e_fin <= fenetre_max_s)
        ventes = sum(dv for _, e_fin, _, dv, _ in deltas if e_fin <= fenetre_max_s)
        volume = sum(
            dv for _, e_fin, _, _, dv in deltas
            if e_fin <= fenetre_max_s and dv is not None
        )
        return achats, ventes, volume

    achats_10s, ventes_10s, _ = _cumule(10)
    achats_m1, ventes_m1, volume_m1 = _cumule(60)

    buy_ratio_1m = round(_safe_div(achats_m1, achats_m1 + ventes_m1), 3)
    sell_ratio_1m = round(_safe_div(ventes_m1, achats_m1 + ventes_m1), 3)

    entry_stats = data.get("entry_stats", {})
    volume_m5_alerte = entry_stats.get("volume_m5")
    achats_m5_alerte = entry_stats.get("txns_buys_m5")
    ventes_m5_alerte = entry_stats.get("txns_sells_m5")

    ratio_volume_m1_m5 = round(_safe_div(volume_m1, volume_m5_alerte), 4)
    ratio_achats_m1_m5 = round(_safe_div(achats_m1, achats_m5_alerte), 4)
    buy_tx_ratio_m5 = round(_safe_div(achats_m5_alerte, (achats_m5_alerte or 0) + (ventes_m5_alerte or 0)), 4)

    def _valeur_au_plus_proche(t_cible, index_valeur):
        candidats = [s for s in samples if s[index_valeur] is not None]
        if not candidats:
            return None
        return min(candidats, key=lambda s: abs(s[0] - t_cible))[index_valeur]

    def _mult_au_plus_proche(t_cible):
        mc_proche = _valeur_au_plus_proche(t_cible, 1)
        return round(mc_proche / initial_mc, 4) if mc_proche else None

    def _price_au_plus_proche(t_cible):
        return _valeur_au_plus_proche(t_cible, 2)

    mult_10s = _mult_au_plus_proche(10)
    mult_30s = _mult_au_plus_proche(30)
    mult_1m = _mult_au_plus_proche(60)

    price_change_m1 = None
    price_change_m3 = None
    if initial_price:
        prix_60 = _price_au_plus_proche(60)
        prix_180 = _price_au_plus_proche(180)
        if prix_60:
            price_change_m1 = round((prix_60 / initial_price - 1) * 100, 2)
        if prix_180:
            price_change_m3 = round((prix_180 / initial_price - 1) * 100, 2)

    if mint in active_tokens:
        active_tokens[mint].update({
            "achats_10s": achats_10s,
            "ventes_10s": ventes_10s,
            "achats_m1": achats_m1,
            "ventes_m1": ventes_m1,
            "volume_m1": round(volume_m1, 2) if volume_m1 is not None else None,
            "ratio_volume_m1_m5": ratio_volume_m1_m5,
            "price_change_m1": price_change_m1,
            "price_change_m3": price_change_m3,
            "buy_ratio_1m": buy_ratio_1m,
            "sell_ratio_1m": sell_ratio_1m,
            "ratio_achats_m1_m5": ratio_achats_m1_m5,
            "buy_tx_ratio_m5": buy_tx_ratio_m5,
            "mult_10s": mult_10s,
            "mult_30s": active_tokens[mint].get("mult_30s") or mult_30s,
            "mult_1m": mult_1m,
            "max_tx_per_second": round(max_tx_par_seconde, 2),
            # CORRECTIF (2026-08-19) : conserve les samples bruts (prix
            # live toutes les ~10s) pour permettre à
            # _mc_a_instant_live() de retrouver un mc réel sans repasser
            # par l'interpolation OHLCV — voir
            # evaluer_et_logger_signaux_croises / evaluer_et_logger_combos_personnalises.
            "samples_180s": samples,
        })

    evaluer_signal3_si_pret(mint)
    evaluer_signal_lp_light_si_pret(mint)
    mc_180 = _valeur_au_plus_proche(180, 1)
    evaluer_signaux_1_et_2(mint, price_change_m3, mc_180)
    evaluer_signal4_si_pret(mint, mc_180)

    print(
        f"[metriques_etendues] {data.get('symbol')} ({mint}) — "
        f"mult_10s={mult_10s} mult_30s={active_tokens.get(mint, {}).get('mult_30s')} mult_1m={mult_1m} "
        f"buy_ratio_1m={buy_ratio_1m} price_change_m1={price_change_m1} "
        f"serie_metriques={len(active_tokens.get(mint, {}).get('serie_metriques', []))} pts"
    )


def essayer_alerter(mint, pair, source_url):
    if not pair or pair.get("chainId") != CHAIN_ID_SOLANA:
        print(f"[non solana] {mint} — chainId={pair.get('chainId') if pair else None}, ignoré")
        return False

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

    txns = pair.get("txns") or {}
    volume = pair.get("volume") or {}
    txns_m5 = txns.get("m5") or {}
    txns_h1 = txns.get("h1") or {}
    price_change_m5 = (pair.get("priceChange") or {}).get("m5")

    tot_m5 = (txns_m5.get("buys") or 0) + (txns_m5.get("sells") or 0)
    tot_h1 = (txns_h1.get("buys") or 0) + (txns_h1.get("sells") or 0)
    tx_accel = round((tot_m5 * 12) / tot_h1, 3) if tot_h1 > 0 else None

    pair_created_at = pair.get("pairCreatedAt")
    pool_age_seconds = None
    if pair_created_at:
        try:
            pool_age_seconds = round(time.time() - (float(pair_created_at) / 1000))
        except (TypeError, ValueError):
            pool_age_seconds = None

    rug_ok, rug_score, rug_flags, top10_pct, insiders_detected, lp_locked_pct, total_holders, bundle_detected = rugcheck_verdict(mint)
    if not rug_ok:
        print(f"[rugcheck] {symbol} ({mint}) rejeté — score={rug_score} flags={rug_flags} top10={top10_pct}")
        return False

    boost_detecte, nombre_boosts_actifs = extraire_infos_boost(pair)
    a_un_profil, site_web, twitter, telegram_link = extraire_infos_profil(pair)

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
        "avg_order_size_sol": avg_order_size_sol,
    }

    filtres_extra_ok = (
        passe_les_filtres(market_cap, liquidity_usd)
        and passe_filtres_triggers(market_cap, price_change_m5, tx_accel)
        and (pool_age_seconds is not None and pool_age_seconds <= POOL_AGE_IDEAL_MAX_SECONDS)
    )

    flags_txt = ", ".join(rug_flags) if rug_flags else "Aucun"
    top10_txt = f"{top10_pct}%" if top10_pct is not None else "N/A"
    holders_txt = f"{total_holders}" if total_holders is not None else "N/A"
    bundle_txt = "⚠️ Oui" if bundle_detected else "Non"
    boost_txt = f"⚡ Oui ({nombre_boosts_actifs})" if boost_detecte else "Non"
    profil_txt = "✅ Oui" if a_un_profil else "Non"
    print(
        f"Suivi démarré (silencieux) pour : {symbol} ({mint}) — RugCheck score={rug_score} "
        f"top10={top10_pct}% — filtres_extra_ok={filtres_extra_ok}"
    )

    active_tokens[mint] = {
        "symbol": symbol,
        "dex": dex_name,
        "initial_mc": market_cap or 1.0,
        "max_price": market_cap or 1.0,
        "max_price_time": 0,
        "min_price": market_cap or 1.0,
        "min_price_time": 0,
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
        "boost_detecte": boost_detecte,
        "nombre_boosts_actifs": nombre_boosts_actifs,
        # Historique des boosts dans le temps : nécessaire pour les
        # combos/conditions qui utilisent "Boosts >= X" — c'est la SEULE
        # métrique de ce bot (avec la série T180/T30 ci-dessous) qui
        # évolue réellement pendant les 30 minutes de suivi.
        "boosts_history": [(0, nombre_boosts_actifs)],
        # CORRECTIF (2026-08-19) : série temporelle des métriques
        # T180/T30 (alimentée par analyser_metriques_etendues) et
        # samples bruts (prix live 0-180s) pour la détection progressive
        # d'instant de franchissement et la récupération de mc_entree
        # sans passer par l'interpolation OHLCV — voir
        # determiner_instant_combo / _mc_a_instant_live.
        "serie_metriques": [],
        "samples_180s": [],
        "profil_dexscreener": a_un_profil,
        "site_web": site_web,
        "twitter": twitter,
        "telegram": telegram_link,
        "signal_valide": None,
        "buy_ratio_source": None,
        "position_statut": "analyse_20s_en_cours",
        "prix_entree_simule": None,
        "sl_prix_simule": None,
        "entry_time": None,
        "tp1_atteint": False,
        "tp2_atteint": False,
        "max_price_apres_tp2": None,
        "resultat_pct_simule": None,
        "time_to_2x": None,
        "time_to_3x": None,
        "tx_velocity_5s": None,
        "buy_ratio_5s": None,
        "buy_ratio_diag": None,
        "achats_10s": None,
        "ventes_10s": None,
        "achats_m1": None,
        "ventes_m1": None,
        "volume_m1": None,
        "ratio_volume_m1_m5": None,
        "price_change_m1": None,
        "price_change_m3": None,
        "buy_ratio_1m": None,
        "sell_ratio_1m": None,
        "ratio_achats_m1_m5": None,
        "buy_tx_ratio_m5": None,
        "mult_10s": None,
        "mult_30s": None,
        "mult_1m": None,
        "max_tx_per_second": None,
        "filtres_extra_ok": filtres_extra_ok,
        "signal_base_evalue": False,
        "signal_base_bis_evalue": False,
        "signal3_evalue": False,
        "signal3_bis_evalue": False,
        "signal_lp_light_evalue": False,
        "signal_lp_light_bis_evalue": False,
        "signaux_1_2_evalues": False,
        "signal4_evalue": False,
        "signal4_bis_evalue": False,
        "positions": {},
    }
    seen_mints.add(mint)

    evaluer_signal_base(mint)
    evaluer_signal_base_bis(mint)

    threading.Thread(target=analyser_20_premieres_secondes, args=(mint,), daemon=True).start()
    threading.Thread(target=analyser_metriques_etendues, args=(mint,), daemon=True).start()

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
            if not profile or profile.get("chainId") != CHAIN_ID_SOLANA:
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


BOOSTED_ENDPOINTS = (
    "https://api.dexscreener.com/token-boosts/latest/v1",
    "https://api.dexscreener.com/token-boosts/top/v1",
)


def check_boosted_tokens():
    mints_vus_ce_cycle = set()

    for url in BOOSTED_ENDPOINTS:
        try:
            response = requests.get(url, timeout=10)
            print(f"[debug_boosts] {url} status={response.status_code}")
            if response.status_code != 200:
                continue

            data = response.json()
            boosts = data if isinstance(data, list) else data.get("data", [])
            print(f"[debug_boosts] {len(boosts)} tokens boostés reçus depuis {url}")

            for boost in boosts:
                if not boost or boost.get("chainId") != CHAIN_ID_SOLANA:
                    continue

                mint = boost.get("tokenAddress")
                if not mint or mint in mints_vus_ce_cycle:
                    continue
                mints_vus_ce_cycle.add(mint)

                if mint in seen_mints or mint in pending_mints or mint in active_tokens:
                    continue

                pair = fetch_pair_data(mint)
                source_url = boost.get("url", f"https://dexscreener.com/solana/{mint}")

                if not pair or pair.get("dexId") not in DEX_MIGRES:
                    pending_mints[mint] = time.time()
                    continue

                essayer_alerter(mint, pair, source_url)

        except Exception as e:
            print(f"Erreur lors de la vérification des tokens boostés ({url}) : {e}")


PENDING_CHECK_INTERVAL = 10
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

# --- Suivi ATH des signaux à 30min / 1h (sans polling continu) ---
# Un token dont au moins un signal s'est déclenché est mis de côté ici à
# 30 min (voir monitor_ath), avec les bougies minute déjà téléchargées
# (ohlcv_30m). Une bougie minute couvrant la 1ère heure est récupérée
# séparément une fois 60 min écoulées (finaliser_signaux, un seul appel
# minute léger), et c'est à ce moment-là que la ligne est écrite dans
# signaux_log.csv (log_resultats_signaux / evaluer_et_logger_signaux_croises).
#
# CORRECTIF (2026-08-19) — SUIVI 24H DÉSACTIVÉ TEMPORAIREMENT (sur
# demande, pour pouvoir tester/corriger les bugs d'ATH sans attendre
# 24h à chaque itération) : SIGNAUX_24H_ENABLED=False. fetch_ohlcv_hour
# et SIGNAUX_24H_DELAY restent définis pour réactivation future — il
# suffira de repasser SIGNAUX_24H_ENABLED à True et de relire
# ohlcv_24h dans finaliser_signaux() pour retrouver le comportement
# précédent (log différé de la ligne signaux_log jusqu'à 24h, avec les
# 3 horizons 30min/1h/24h).
SIGNAUX_24H_ENABLED = False
SIGNAUX_24H_DELAY = 24 * 3600

SIGNAUX_1H_DELAY = 3600
SIGNAUX_1H_MINUTES_LIMIT = 70  # marge après 60 min
signaux_pending_finalisation = {}

# CORRECTIF (2026-08-19) : suivi séparé pour la mise à jour de
# token_log.csv à 1h (en plus de son écriture initiale à 30min) — voir
# finaliser_token_log_1h(). Léger : ne stocke que ce qui est nécessaire
# pour refaire un calcul d'ATH (pas de copie complète de data comme pour
# signaux_pending_finalisation).
TOKEN_LOG_1H_DELAY = 3600
TOKEN_LOG_1H_MINUTES_LIMIT = 70
token_log_pending_1h = {}


def monitor_ath():
    global _last_price_check
    current_time = time.time()
    tokens_to_remove = []

    do_price_check = (current_time - _last_price_check) >= PRICE_CHECK_INTERVAL
    if do_price_check:
        _last_price_check = current_time

    for mint, data in list(active_tokens.items()):
        try:
            elapsed = current_time - data["start_time"]

            if do_price_check:
                try:
                    pair0 = fetch_pair_data(mint)
                    if pair0:
                        current_mc = pair0.get("marketCap", 0) or pair0.get("fdv", 0)
                        print(f"[monitor_ath] {data['symbol']} ({mint}) MC actuel=${current_mc:,.0f} (max enregistré=${data['max_price']:,.0f})")
                        if current_mc and current_mc > data["max_price"]:
                            active_tokens[mint]["max_price"] = current_mc
                            active_tokens[mint]["max_price_time"] = round(elapsed)
                        if current_mc and current_mc < active_tokens[mint].get("min_price", current_mc):
                            active_tokens[mint]["min_price"] = current_mc
                            active_tokens[mint]["min_price_time"] = round(elapsed)

                        boost_detecte_maj, nb_boosts_maj = extraire_infos_boost(pair0)
                        if boost_detecte_maj:
                            active_tokens[mint]["boost_detecte"] = True
                            active_tokens[mint]["nombre_boosts_actifs"] = max(
                                nb_boosts_maj, active_tokens[mint].get("nombre_boosts_actifs", 0) or 0
                            )
                        # Historique des boosts : on ajoute un point à CHAQUE
                        # cycle de prix (~toutes les 120s), que le nombre ait
                        # changé ou non — nécessaire pour determiner_instant_combo
                        # afin de savoir QUAND un seuil de boosts a été franchi.
                        active_tokens[mint].setdefault("boosts_history", []).append(
                            (round(elapsed), active_tokens[mint].get("nombre_boosts_actifs", 0) or 0)
                        )

                        profil_detecte_maj, site_maj, twitter_maj, telegram_maj = extraire_infos_profil(pair0)
                        if profil_detecte_maj:
                            active_tokens[mint]["profil_dexscreener"] = True
                            active_tokens[mint]["site_web"] = active_tokens[mint].get("site_web") or site_maj
                            active_tokens[mint]["twitter"] = active_tokens[mint].get("twitter") or twitter_maj
                            active_tokens[mint]["telegram"] = active_tokens[mint].get("telegram") or telegram_maj

                        # CORRECTIF (2026-08-17) : capture, dès qu'une des
                        # conditions supplémentaires devient vraie, l'état des
                        # champs VARIABLE_AVANT_KEYS à CET instant précis.
                        # Réutilise determiner_instant_combo/data déjà en
                        # mémoire — zéro appel API supplémentaire, on se
                        # greffe sur ce cycle qui tourne déjà toutes les
                        # PRICE_CHECK_INTERVAL secondes. Ne fait rien tant
                        # qu'aucun signal de base n'est ouvert (pas la peine
                        # de suivre les conditions d'un token qu'on ne
                        # croisera jamais).
                        if active_tokens[mint].get("positions"):
                            cond_snaps = active_tokens[mint].setdefault("conditions_variable_snapshots", {})
                            for cond_cle, _cond_nom, conditions in SIGNAUX_SUPPLEMENTAIRES:
                                if cond_cle in cond_snaps:
                                    continue
                                instant_cond = determiner_instant_combo(active_tokens[mint], conditions)
                                if instant_cond is not None:
                                    cond_snaps[cond_cle] = {
                                        "instant": instant_cond,
                                        **_construire_snapshot_variable(mint),
                                    }

                        if current_mc:
                            gerer_simulation_position(mint, current_mc, elapsed)
                    else:
                        print(f"[monitor_ath] {data['symbol']} ({mint}) aucune pair Solana retournée par DexScreener")
                except Exception as e:
                    print(f"Erreur monitor_ath (price_check) pour {mint} : {e}")

            if elapsed >= 1800:
                initial_mc = data["initial_mc"] or 1.0
                pool_address = data.get("pool_address")

                elapsed_minutes = max(int(elapsed / 60) + 5, 10)
                ohlcv_list = fetch_ohlcv_minute(pool_address, elapsed_minutes)

                true_ath_mc = get_true_ath_mc(
                    pool_address, initial_mc, data.get("initial_price"), data["start_time"], ohlcv_list=ohlcv_list,
                )
                print(f"[monitor_ath] {data['symbol']} ({mint}) GeckoTerminal ATH mc={true_ath_mc}")
                max_mc = max(active_tokens[mint]["max_price"], true_ath_mc or 0)

                temps_pic = active_tokens[mint].get("max_price_time", 0)
                if ohlcv_list and true_ath_mc and true_ath_mc >= active_tokens[mint]["max_price"]:
                    try:
                        meilleure_bougie = max((c for c in ohlcv_list if len(c) >= 3), key=lambda c: c[2], default=None)
                        if meilleure_bougie:
                            temps_pic = max(round(meilleure_bougie[0] - data["start_time"]), 0)
                    except (TypeError, ValueError):
                        pass

                multiplicateur = max_mc / initial_mc
                dex_url = data.get("dex_url", f"https://dexscreener.com/solana/{mint}")

                if data.get("signal_valide") and data.get("resultat_pct_simule") is None:
                    entree = data.get("prix_entree_simule") or initial_mc
                    active_tokens[mint]["resultat_pct_simule"] = round((max_mc / entree - 1) * 100, 2)
                    if data.get("position_statut") in ("ouverte", "tp1", "trailing"):
                        active_tokens[mint]["position_statut"] = "expire_30min"
                data = active_tokens[mint]

                positions_declenchees = data.get("positions") or {}
                if positions_declenchees:
                    print(
                        f"[monitor_ath] Suivi 30 min terminé pour : {data['symbol']} — "
                        f"x{multiplicateur:,.2f} — {len(positions_declenchees)} signal(aux) validé(s) "
                        f"— en attente du calcul ATH 1h (voir finaliser_signaux)"
                    )
                else:
                    print(f"[monitor_ath] Suivi 30 min terminé pour : {data['symbol']} — x{multiplicateur:,.2f} (aucun signal validé, pas d'alerte)")

                entry_stats = data.get("entry_stats", {})

                min_price = data.get("min_price", initial_mc)
                max_drawdown_before_peak = round((min_price / initial_mc - 1) * 100, 2) if initial_mc else None

                liquidite_usd = data.get("liquidity_usd")
                pool_age_seconds = data.get("pool_age_seconds")
                pool_age_minutes = round(pool_age_seconds / 60, 2) if pool_age_seconds is not None else None
                is_golden_window = (pool_age_seconds is not None and pool_age_seconds <= GOLDEN_WINDOW_MAX_SECONDS)
                ratio_liquidite_mc = round(_safe_div(liquidite_usd, initial_mc), 4)

                simulations_sortie = simuler_strategies_sortie(ohlcv_list, data.get("initial_price"))
                simulations_avancees = simuler_strategies_avancees(ohlcv_list, data.get("initial_price"), data["start_time"])

                time_to_max_drawdown, minutes_ecoulees_dd = calculer_timing_drawdown(
                    ohlcv_list, data.get("initial_price"), data["start_time"],
                    min_price, data.get("min_price_time", 0),
                )
                vitesse_chute_pct_par_min = None
                if max_drawdown_before_peak is not None and minutes_ecoulees_dd and minutes_ecoulees_dd > 0:
                    vitesse_chute_pct_par_min = round(max_drawdown_before_peak / minutes_ecoulees_dd, 2)
                elif max_drawdown_before_peak == 0:
                    vitesse_chute_pct_par_min = 0.0

                row_rapport = {
                    "horodatage": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "mint": mint,
                    "symbole": data["symbol"],
                    "dex": data.get("dex"),
                    "mc_initial": initial_mc,
                    "mc_max": max_mc,
                    "multiplicateur": round(multiplicateur, 3),
                    "liquidite_usd": liquidite_usd,
                    "ratio_liquidite": entry_stats.get("liquidity_ratio"),
                    "score_rugcheck": data.get("rugcheck_score"),
                    "alertes_rugcheck": data.get("rugcheck_flags"),
                    "pct_top10_holders": data.get("top10_pct"),
                    "insiders_detectes": data.get("insiders_detected"),
                    "nombre_holders": data.get("total_holders"),
                    "bundle_detecte": data.get("bundle_detected"),
                    "pool_age_seconds": pool_age_seconds,
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
                    "boost_detecte": data.get("boost_detecte", False),
                    "nombre_boosts_actifs": data.get("nombre_boosts_actifs", 0),
                    "profil_dexscreener": data.get("profil_dexscreener", False),
                    "site_web": data.get("site_web"),
                    "twitter": data.get("twitter"),
                    "telegram": data.get("telegram"),
                    "tx_velocity_5s": data.get("tx_velocity_5s"),
                    "buy_ratio_5s": data.get("buy_ratio_5s"),
                    "avg_order_size_sol": entry_stats.get("avg_order_size_sol"),
                    "unique_buyers_count": None,
                    "buy_ratio_diag": data.get("buy_ratio_diag"),
                    "signal_valide": data.get("signal_valide"),
                    "buy_ratio_source": data.get("buy_ratio_source"),
                    "position_statut": data.get("position_statut"),
                    "resultat_pct_simule": data.get("resultat_pct_simule"),
                    "time_to_2x": data.get("time_to_2x"),
                    "time_to_3x": data.get("time_to_3x"),
                    "max_drawdown_before_peak": max_drawdown_before_peak,
                    "achats_10s": data.get("achats_10s"),
                    "ventes_10s": data.get("ventes_10s"),
                    "volume_m1": data.get("volume_m1"),
                    "ratio_volume_m1_m5": data.get("ratio_volume_m1_m5"),
                    "price_change_m1": data.get("price_change_m1"),
                    "price_change_m3": data.get("price_change_m3"),
                    "buy_ratio_1m": data.get("buy_ratio_1m"),
                    "achats_m1": data.get("achats_m1"),
                    "ratio_achats_m1_m5": data.get("ratio_achats_m1_m5"),
                    "buy_tx_ratio_m5": data.get("buy_tx_ratio_m5"),
                    "mult_10s": data.get("mult_10s"),
                    "mult_30s": data.get("mult_30s"),
                    "mult_1m": data.get("mult_1m"),
                    "ratio_liquidite_mc": ratio_liquidite_mc,
                    "sell_ratio_1m": data.get("sell_ratio_1m"),
                    "max_tx_per_second": data.get("max_tx_per_second"),
                    "pool_age_minutes": pool_age_minutes,
                    "is_golden_window": is_golden_window,
                    "time_to_peak": temps_pic,
                    "time_to_max_drawdown": time_to_max_drawdown,
                    "vitesse_chute_pct_par_min": vitesse_chute_pct_par_min,
                    **simulations_sortie,
                    **simulations_avancees,
                }
                log_resultat_csv(row_rapport)

                # CORRECTIF (2026-08-19) : mémorise ce qu'il faut pour
                # recalculer l'ATH à 1h de ce token (demande explicite :
                # appliquer la même méthode d'ATH corrigée sur
                # token_log.csv, aux 2 horizons 30min ET 1h — voir
                # finaliser_token_log_1h()). max_price/max_price_time
                # inclut déjà le filet de sécurité live (0-180s +
                # polling 120s) grâce aux correctifs ci-dessus.
                token_log_pending_1h[mint] = {
                    "horodatage": row_rapport["horodatage"],
                    "start_time": data["start_time"],
                    "pool_address": pool_address,
                    "initial_mc": initial_mc,
                    "initial_price": data.get("initial_price"),
                    "max_price": active_tokens[mint]["max_price"],
                    "max_price_time": active_tokens[mint].get("max_price_time", 0),
                }

                # Le multiplicateur ATH par signal se calcule maintenant à
                # 30min (avec ohlcv_list déjà téléchargé ici) et 1h (voir
                # finaliser_signaux) — le suivi 24h est désactivé pour le
                # moment (SIGNAUX_24H_ENABLED). On stocke une copie
                # COMPLÈTE de data (pas un sous-ensemble) car
                # evaluer_et_logger_signaux_croises, via
                # determiner_instant_combo, a besoin de tous les champs
                # (price_change_m3, top10_pct, boosts_history,
                # ratio_liquidite_mc, serie_metriques, samples_180s,
                # max_price/max_price_time, etc.) pour recalculer les
                # mêmes instants de validation qu'en direct, et pour le
                # filet de sécurité du suivi live dans
                # calculer_ath_depuis_entree.
                if data.get("positions"):
                    snapshot = dict(data)
                    snapshot["row_rapport"] = row_rapport
                    snapshot["ohlcv_30m"] = ohlcv_list  # déjà fetché ci-dessus, aucun appel en plus
                    snapshot["ohlcv_1h"] = None  # rempli par finaliser_signaux()
                    signaux_pending_finalisation[mint] = snapshot

                # Combos personnalisés (log séparé, inchangé — reste sur
                # la fenêtre de 30 min comme avant)
                evaluer_et_logger_combos_personnalises(mint, data, ohlcv_list)

                tokens_to_remove.append(mint)

        except Exception as e:
            print(f"[monitor_ath] ERREUR lors de la finalisation de {mint} : {e}")
            traceback.print_exc()
            tokens_to_remove.append(mint)

    for mint in tokens_to_remove:
        active_tokens.pop(mint, None)


def finaliser_signaux():
    """À appeler à chaque tour de boucle principale. Dès que
    SIGNAUX_1H_DELAY (1h) s'est écoulé depuis le démarrage d'un token en
    attente de finalisation (mint présent dans
    signaux_pending_finalisation), récupère les bougies MINUTE couvrant
    sa 1ère heure de vie, puis écrit IMMÉDIATEMENT la/les ligne(s) dans
    signaux_log.csv (log_resultats_signaux + evaluer_et_logger_signaux_croises)
    avec les horizons 30min (déjà en mémoire) et 1h.

    CORRECTIF (2026-08-19) — suivi 24h désactivé temporairement
    (SIGNAUX_24H_ENABLED=False) : auparavant, il fallait attendre 24h
    supplémentaires (voir l'ancienne finaliser_signaux_24h) avant de
    voir la moindre ligne dans signaux_log.csv, ce qui rendait le
    débogage des bugs d'ATH beaucoup trop lent. Le log est désormais
    écrit dès l'horizon 1h. Un seul appel GeckoTerminal léger par
    token, une seule fois."""
    maintenant = time.time()
    mints_a_retirer = []

    for mint, data in list(signaux_pending_finalisation.items()):
        if maintenant - data["start_time"] < SIGNAUX_1H_DELAY:
            continue

        ohlcv_1h = fetch_ohlcv_minute(data.get("pool_address"), SIGNAUX_1H_MINUTES_LIMIT)
        if not ohlcv_1h:
            print(f"[signaux_1h] {data.get('symbol')} ({mint}) — OHLCV 1h indisponible, réessai au prochain cycle")
            continue

        row_rapport = data.get("row_rapport")
        ohlcv_30m = data.get("ohlcv_30m")

        # 12 signaux "purs" : entrée + multiplicateur ATH depuis
        # l'entrée réelle de chaque signal, calculé à 30min/1h (+ filet
        # de sécurité sur le suivi live, voir calculer_ath_depuis_entree).
        log_resultats_signaux(mint, data, row_rapport, ohlcv_30m, ohlcv_1h)

        # Croisement des 12 signaux avec les conditions supplémentaires,
        # instant retenu = max(instant signal, instant condition),
        # inchangé — mêmes 2 horizons.
        evaluer_et_logger_signaux_croises(mint, data, ohlcv_30m, ohlcv_1h, row_rapport)

        print(f"[signaux_1h] {data.get('symbol')} ({mint}) — ATH 30min/1h calculés et journalisés")
        mints_a_retirer.append(mint)

    for mint in mints_a_retirer:
        signaux_pending_finalisation.pop(mint, None)


def finaliser_token_log_1h():
    """CORRECTIF (2026-08-19), demande explicite : applique la même
    méthode d'ATH corrigée (filet de sécurité sur le suivi live, voir
    get_true_ath_mc + les correctifs sur max_price ci-dessus) à
    token_log.csv, PAS SEULEMENT à 30min mais aussi à 1h. Une heure
    après le début du suivi d'un token déjà clôturé dans token_log.csv,
    refait un appel GeckoTerminal léger sur une fenêtre plus large, puis
    met à jour EN PLACE la ligne déjà écrite (colonnes mc_max_1h /
    multiplicateur_1h / horodatage_maj_1h) au lieu d'en créer une
    nouvelle. La ligne est identifiée par (mint, horodatage) — suffisant
    tant qu'un même mint n'est pas suivi deux fois dans le même
    processus (déjà garanti par tokens_traites / seen_mints)."""
    if pd is None:
        return

    maintenant = time.time()
    mints_a_retirer = []

    for mint, info in list(token_log_pending_1h.items()):
        if maintenant - info["start_time"] < TOKEN_LOG_1H_DELAY:
            continue

        ohlcv_1h = fetch_ohlcv_minute(info["pool_address"], TOKEN_LOG_1H_MINUTES_LIMIT)
        if not ohlcv_1h:
            print(f"[token_log_1h] {mint} — OHLCV 1h indisponible, réessai au prochain cycle")
            continue

        true_ath_mc_1h = get_true_ath_mc(
            info["pool_address"], info["initial_mc"], info["initial_price"], info["start_time"],
            ohlcv_list=ohlcv_1h,
        )
        mc_max_1h = max(info.get("max_price") or 0, true_ath_mc_1h or 0, info.get("initial_mc") or 0)
        multiplicateur_1h = round(mc_max_1h / info["initial_mc"], 3) if info.get("initial_mc") else None

        try:
            df = pd.read_csv(LOG_FILE)
            masque = (df["mint"] == mint) & (df["horodatage"] == info["horodatage"])
            if masque.any():
                df.loc[masque, "mc_max_1h"] = mc_max_1h
                df.loc[masque, "multiplicateur_1h"] = multiplicateur_1h
                df.loc[masque, "horodatage_maj_1h"] = time.strftime("%Y-%m-%d %H:%M:%S")
                df.to_csv(LOG_FILE, index=False, encoding="utf-8")
                print(f"[token_log_1h] {mint} — ligne mise à jour avec ATH 1h (x{multiplicateur_1h})")
            else:
                print(f"[token_log_1h] {mint} — ligne d'origine introuvable dans {LOG_FILE}, mise à jour ignorée")
        except Exception as e:
            print(f"[token_log_1h] erreur mise à jour {LOG_FILE} pour {mint} : {e}")

        mints_a_retirer.append(mint)

    for mint in mints_a_retirer:
        token_log_pending_1h.pop(mint, None)


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

DEXTOOLS_CHANNEL_CHECK_INTERVAL = 60
DEXTOOLS_PRICE_CHECK_INTERVAL = 300
DEXTOOLS_TRACK_DURATION = 24 * 3600

dextools_tracked = {}
dextools_seen_posts = set()

_dextools_last_channel_check = 0
_dextools_last_price_check = 0

CA_FROM_LINK_RE = re.compile(
    r'(?:dexscreener\.com/solana/|solscan\.io/token/|birdeye\.so/token/|pump\.fun/(?:coin/)?)'
    r'([1-9A-HJ-NP-Za-km-z]{32,44})'
)

AUTRES_CHAINES_KEYWORDS_RE = re.compile(
    r'\b(ethereum|erc-?20|etherscan\.io|bscscan\.com|binance smart chain|bnb chain|'
    r'polygonscan\.com|arbiscan\.io|basescan\.org|uniswap|pancakeswap)\b',
    re.IGNORECASE,
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
    if AUTRES_CHAINES_KEYWORDS_RE.search(message_txt):
        return None
    m = CA_FROM_LINK_RE.search(message_html)
    if m:
        return m.group(1)
    return None


def extraire_infos_alerte_dextools(message_html):
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
    page_html = fetch_dextools_channel_html()
    if not page_html:
        return

    for match in POST_BLOCK_RE.finditer(page_html):
        post_id = match.group("post_id")
        if post_id in dextools_seen_posts:
            continue
        dextools_seen_posts.add(post_id)

        bloc = page_html[match.start():match.start() + 6000]
        text_match = MESSAGE_TEXT_RE.search(bloc)
        message_html = text_match.group("text") if text_match else ""
        message_txt = _dextools_nettoyer_html(message_html)

        mint = extraire_mint_dextools(bloc, message_txt)
        if not mint:
            print(f"[dextools] post {post_id} — pas de lien Solana explicite (autre chaîne ou non identifiable), ignoré")
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
    global _dextools_last_channel_check
    now = time.time()
    if (now - _dextools_last_channel_check) < DEXTOOLS_CHANNEL_CHECK_INTERVAL:
        return
    _dextools_last_channel_check = now
    check_dextools_channel()


def demarrer_suivi_dextools(mint, infos_alerte, post_id, alert_time, alert_dt):
    pair = fetch_pair_data(mint)
    if not pair:
        print(f"[dextools] {mint} — aucune pair Solana confirmée via DexScreener, suivi ignoré")
        return

    mc_initial = pair.get("marketCap", 0) or pair.get("fdv", 0) or None
    prix_initial = _to_float(pair.get("priceUsd"))
    liquidite_usd = (pair.get("liquidity") or {}).get("usd")
    pool_address = pair.get("pairAddress")

    _rug_ok, score_rugcheck, flags_rugcheck, top10_pct, insiders_detected, lp_locked_pct, total_holders, bundle_detected = rugcheck_verdict(mint)

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

    print(f"[dextools] nouveau suivi démarré pour {mint} (post {post_id}, MC initial={mc_initial}) — confirmé Solana")


def get_ath_24h_via_gecko(pool_address):
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


BOOSTED_CHECK_INTERVAL = 30
_last_boosted_check = 0


def check_boosted_tokens_throttled():
    global _last_boosted_check
    now = time.time()
    if (now - _last_boosted_check) < BOOSTED_CHECK_INTERVAL:
        return
    _last_boosted_check = now
    check_boosted_tokens()


if __name__ == "__main__":
    print("Bot de surveillance démarré...")
    while True:
        try:
            check_telegram_commands()
            check_new_solana_tokens()
            check_boosted_tokens_throttled()
            check_pending_tokens()
            monitor_ath()
            finaliser_signaux()
            finaliser_token_log_1h()
            check_dextools_channel_throttled()
            monitor_dextools_ath()
        except Exception as e:
            print(f"[boucle_principale] ERREUR non gérée : {e}")
            traceback.print_exc()
        time.sleep(10)
