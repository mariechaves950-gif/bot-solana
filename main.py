import os
import re
import csv
import html
import time
import threading
import traceback
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
            send_telegram_document(SIGNAUX_LOG_FILE, caption="🎯 Historique des signaux 1/2/3/4 déclenchés (trailing stop simulé)")
        elif text in ("/csv_dextools", "/log_dextools"):
            send_telegram_document(DEXTOOLS_LOG_FILE, caption="📊 Historique canal DexToolsPublic (multiplicateur alerte → ATH 24h)")
        elif text in ("/comparatif", "/strategies", "/rapport_strategies"):
            generer_et_envoyer_rapport_comparatif()
        elif text in ("/propositions", "/filtres"):
            generer_et_envoyer_rapport_propositions()
        elif text == "/help":
            send_telegram_message(
                "Commandes disponibles :\n"
                "/csv — télécharger le fichier de log complet (tous les tokens suivis)\n"
                "/csv_signaux — télécharger l'historique des signaux 1/2/3/4 déclenchés, "
                "avec le résultat des 3 trailing stops comparés (-25% / -30% / -40%)\n"
                "/csv_dextools — télécharger le suivi du canal DexToolsPublic\n"
                "/comparatif — comparatif ROI moyen / médian / taux de succès de toutes les stratégies "
                "(A à G + anciennes), calculé uniquement sur les tokens ayant validé le signal d'entrée\n"
                "/propositions — compare le filtre actuel à 3 propositions de critères de sélection plus "
                "souples (price_change_m3, ventes_m5, max_drawdown_before_peak), avec le nombre de tokens "
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
        if mint in active_tokens:
            active_tokens[mint]["low_second_20s"] = low_second
            active_tokens[mint]["low_mult_20s"] = round(low_mult, 3)
            if low_mc < active_tokens[mint].get("min_price", low_mc):
                active_tokens[mint]["min_price"] = low_mc
                active_tokens[mint]["min_price_time"] = low_second
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

        # --- Signal 3 (entrée à 30s) : buy_ratio_20s vient d'être calculé.
        # Si l'âge du token dépasse déjà 30s (peu probable ici mais possible
        # en cas de retard), on tente l'évaluation immédiatement ; sinon
        # c'est analyser_metriques_etendues() qui la déclenchera à t=30s.
        evaluer_signal3_si_pret(mint)
        # --- Signal LP light (entrée à 30s, sans filtre qualité) : même
        # logique de retry que le Signal 3 ci-dessus.
        evaluer_signal_lp_light_si_pret(mint)

    print(
        f"[analyse_20s] {data['symbol']} ({mint}) — "
        f"buy_ratio_2s={buy_ratio_2s} buy_ratio_5s={buy_ratio_5s} buy_ratio_10s={buy_ratio_10s} "
        f"buy_ratio_20s={buy_ratio_20s} achats_bruts_2s={achats_bruts_2s} buy_ratio_diag={buy_ratio_diag}"
    )


METRIQUES_ETENDUES_DUREE = 180
# Intervalle allongé de 5s -> 10s pour réduire le volume d'appels API
# DexScreener (fetch_pair_data) pendant la phase d'analyse étendue de
# chaque token suivi — même raison que ANALYSE_20S_SAMPLE_INTERVAL.
METRIQUES_ETENDUES_INTERVAL = 10


# ============================================================
# --- SIGNAUX 1, 2, 3, 4 : entrée simulée + trailing stop (SANS alerte Telegram) ---
# ============================================================
# Règles communes à tous les signaux : pas de TP plafonné — le stop suit le
# prix (trailing) et remonte à chaque nouveau plus haut. On compare 3
# distances de trailing en parallèle sur chaque signal : -25% / -30% / -40%.
#
# NOTE (désactivation temporaire des alertes Telegram, pour limiter les
# appels API) : ouvrir_position_signal() n'envoie plus AUCUN message
# Telegram. Toute la logique de simulation (ouverture de position,
# trailing stop, journalisation CSV) reste intacte. Pour réactiver les
# alertes, il suffit de dé-commenter le bloc d'envoi à la fin de la
# fonction ci-dessous.
SIGNAUX_SL_PCT = -0.25
SIGNAUX_MAX_HOLD_MINUTES = 30
SIGNAUX_MISE_USD = MISE_SIMULATION_USD  # mise fictive $100, comme les autres simulations

SIGNAUX_TRAILING_VARIANTS = {
    "trail25": -0.25,
    "trail30": -0.30,
    "trail40": -0.40,
}

# Signal 1 : price_change_m3 ÷ avg_order_size_sol >= 24.2 — entrée à 3 min
SIGNAL1_MC_MIN = 10000
SIGNAL1_BUY_RATIO_20S_MIN = 0.55
SIGNAL1_SEUIL = 24.2

# Signal 2 : score_rugcheck × price_change_m3 >= 15.3 — entrée à 3 min
SIGNAL2_MC_MIN = 20000
SIGNAL2_BUY_RATIO_20S_MIN = 0.55
SIGNAL2_SEUIL = 15.3

# Signal 3 : lp_locked_pct ÷ mult_30s <= 95.6 — entrée à 30 secondes
# (filtres qualité : mc_initial >= 40 000$ + buy_ratio_20s >= 0.63)
SIGNAL3_MC_MIN = 40000
SIGNAL3_BUY_RATIO_20S_MIN = 0.63
SIGNAL3_SEUIL = 95.6

# Signal LP light : MÊME formule que Signal 3 (lp_locked_pct ÷ mult_30s
# <= 95.6, entrée à 30 secondes), mais SANS AUCUN filtre qualité
# (ni mc_initial, ni buy_ratio_20s). Signal indépendant : peut se
# déclencher même quand Signal 3 est rejeté par ses filtres.
SIGNAL_LP_LIGHT_SEUIL = 95.6

# Signal 4 — Momentum M3+M1 : entrée à 3 min, dès que price_change_m3 ET
# price_change_m1 sont tous deux disponibles (calculés en fin de
# analyser_metriques_etendues). Issu de l'analyse du dataset signaux_log :
# c'est la combinaison qui, sur 97 signaux, fait passer le taux de
# réussite (≥x2) de 22% (sans filtre) à 50%, et le multiplicateur moyen de
# 1,84 à 2,75. Aucun filtre qualité additionnel (mc_initial, buy_ratio) :
# signal indépendant, purement momentum, comme Signal LP light.
SIGNAL4_PRICE_CHANGE_M3_MIN = 12
SIGNAL4_PRICE_CHANGE_M1_MIN = 10

# Anti-doublon : (mint, cle_signal) déjà écrit dans SIGNAUX_LOG_FILE
signaux_traites = set()


def _lien_dexscreener(mint):
    return f"https://dexscreener.com/solana/{mint}"


def _lien_axiom(mint):
    return f"https://axiom.trade/meme/{mint}"


def _echapper_markdown(texte):
    """Échappe les caractères qui cassent le parse_mode='Markdown' de
    Telegram (_ * ` [) quand ils apparaissent dans du texte dynamique
    (symbole de token, dex...) inséré au milieu d'un message formaté.
    Sans ça, un symbole ou un texte contenant un nombre impair de ces
    caractères fait échouer l'envoi ENTIER du message, silencieusement
    (erreur 400 "can't parse entities", visible seulement dans les logs)."""
    if texte is None:
        return texte
    texte = str(texte)
    for car in ("_", "*", "`", "["):
        texte = texte.replace(car, f"\\{car}")
    return texte


def ouvrir_position_signal(mint, cle, nom_signal, formule_txt, valeur_calculee, seuil_txt, current_mc):
    """Ouvre une position simulée pour un signal donné (si pas déjà ouverte).

    ALERTES TELEGRAM DÉSACTIVÉES (temporairement, pour limiter les appels
    API) : la position est ouverte et suivie normalement (trailing stop,
    journalisation CSV) mais plus aucun message n'est envoyé sur Telegram
    ici. Pour réactiver, dé-commenter le bloc "ENVOI TELEGRAM" en bas de
    la fonction.

    Chaque position suit 3 variantes de trailing stop en parallèle
    (-25% / -30% / -40% depuis le plus haut observé), actives dès l'entrée
    (pas de seuil d'activation, pas de take-profit plafonné)."""
    data = active_tokens.get(mint)
    if not data or not current_mc:
        return

    positions = data.setdefault("positions", {})
    if cle in positions:
        return  # déjà ouverte, garde-fou anti-doublon

    entry_price = current_mc

    trailing = {}
    for var_cle, pct in SIGNAUX_TRAILING_VARIANTS.items():
        trailing[var_cle] = {
            "peak": entry_price,
            "sl": entry_price * (1 + pct),
            "statut": "ouverte",
            "resultat_pct": None,
            "resultat_usd": None,
        }

    positions[cle] = {
        "nom": nom_signal,
        "entry_price": entry_price,
        "entry_time": time.time(),
        "entry_elapsed_s": round(time.time() - data["start_time"]),
        "formule_txt": formule_txt,
        "valeur_calculee": valeur_calculee,
        "seuil_txt": seuil_txt,
        "trailing": trailing,
    }

    print(f"[{cle}] {data['symbol']} ({mint}) — signal déclenché, position ouverte à ${entry_price:,.0f} (trailing 25/30/40%) — [alerte Telegram désactivée]")

    # --- ENVOI TELEGRAM (désactivé) ---
    # variantes_txt = " / ".join(
    #     f"{int(abs(pct) * 100)}%" for pct in SIGNAUX_TRAILING_VARIANTS.values()
    # )
    #
    # pool_age_txt = "N/A"
    # pool_age_detection = data.get("pool_age_seconds")
    # entry_elapsed_s = round(time.time() - data["start_time"])
    # if pool_age_detection is not None:
    #     age_total_s = pool_age_detection + entry_elapsed_s
    #     minutes, secondes = divmod(age_total_s, 60)
    #     pool_age_txt = f"{minutes}m{secondes:02d}s"
    #
    # lignes = [
    #     f"🎯 *{nom_signal} déclenché !*",
    #     "",
    #     f"🪙 Token : {_echapper_markdown(data['symbol'])}",
    #     f"🏦 DEX : {_echapper_markdown(data.get('dex'))}",
    #     f"🕒 Âge du pool à l'alerte : {pool_age_txt}",
    #     f"📊 Formule : `{formule_txt}` = {valeur_calculee} (seuil {seuil_txt})",
    #     f"💰 MC entrée (simulée) : ${entry_price:,.0f}",
    #     f"📈 Trailing stop simulé (3 variantes comparées) : {variantes_txt} depuis le plus haut",
    #     "",
    #     "📋 *Adresse du token (appuyer pour copier)* :",
    #     f"`{mint}`",
    #     "",
    #     f"🔗 [DexScreener]({_lien_dexscreener(mint)})",
    #     f"⚡ [Trader sur Axiom]({_lien_axiom(mint)})",
    # ]
    # envoye = send_telegram_message("\n".join(lignes))
    # if envoye:
    #     print(f"[{cle}] {data['symbol']} ({mint}) — !!! ALERTE TELEGRAM ENVOYÉE AVEC SUCCÈS !!!")
    # else:
    #     print(f"[{cle}] {data['symbol']} ({mint}) — !!! ÉCHEC ENVOI ALERTE TELEGRAM, voir erreur juste au-dessus !!!")


def evaluer_signal3_si_pret(mint):
    """Signal 3 : lp_locked_pct ÷ mult_30s <= 95.6, entrée à 30s, AVEC
    filtres qualité (mc_initial >= 40 000$ + buy_ratio_20s >= 0.63).
    Peut être appelée plusieurs fois (depuis analyse_20s puis
    metriques_etendues) ; ne s'exécute réellement qu'une fois toutes les
    données nécessaires disponibles, et ne se déclenche qu'une seule fois
    par token."""
    data = active_tokens.get(mint)
    if not data or data.get("signal3_evalue"):
        return

    buy_ratio_20s = data.get("buy_ratio_20s")
    mult_30s = data.get("mult_30s")
    lp_locked_pct = data.get("lp_locked_pct")
    mc_initial = data.get("initial_mc")

    # Données pas encore toutes disponibles -> on retentera au prochain appel
    if buy_ratio_20s is None or not mult_30s or lp_locked_pct is None:
        return

    data["signal3_evalue"] = True

    if not mc_initial or mc_initial < SIGNAL3_MC_MIN:
        print(f"[signal3] {data['symbol']} ({mint}) rejeté — mc_initial=${mc_initial}")
        return
    if buy_ratio_20s < SIGNAL3_BUY_RATIO_20S_MIN:
        print(f"[signal3] {data['symbol']} ({mint}) rejeté — buy_ratio_20s={buy_ratio_20s}")
        return

    valeur = round(lp_locked_pct / mult_30s, 3)
    if valeur > SIGNAL3_SEUIL:
        print(f"[signal3] {data['symbol']} ({mint}) rejeté — lp_locked_pct/mult_30s={valeur}")
        return

    pair = fetch_pair_data(mint)
    current_mc = (pair.get("marketCap", 0) or pair.get("fdv", 0)) if pair else None
    if not current_mc:
        current_mc = (data.get("initial_mc") or 1.0) * mult_30s

    ouvrir_position_signal(
        mint, "signal3", "Signal 3",
        "lp_locked_pct ÷ mult_30s", valeur, f"≤ {SIGNAL3_SEUIL}",
        current_mc,
    )


def evaluer_signal_lp_light_si_pret(mint):
    """Signal LP light : lp_locked_pct ÷ mult_30s <= 95.6, entrée à 30s,
    SANS AUCUN filtre qualité (ni mc_initial, ni buy_ratio_20s) —
    uniquement la condition brute. Signal indépendant de Signal 3 : les
    deux peuvent se déclencher (ou pas) chacun de leur côté sur le même
    token, puisqu'ils ouvrent des positions simulées distinctes
    ("signal3" vs "signal_lp_light")."""
    data = active_tokens.get(mint)
    if not data or data.get("signal_lp_light_evalue"):
        return

    mult_30s = data.get("mult_30s")
    lp_locked_pct = data.get("lp_locked_pct")

    # Données pas encore disponibles -> on retentera au prochain appel
    if not mult_30s or lp_locked_pct is None:
        return

    data["signal_lp_light_evalue"] = True

    valeur = round(lp_locked_pct / mult_30s, 3)
    if valeur > SIGNAL_LP_LIGHT_SEUIL:
        print(f"[signal_lp_light] {data['symbol']} ({mint}) rejeté — lp_locked_pct/mult_30s={valeur}")
        return

    pair = fetch_pair_data(mint)
    current_mc = (pair.get("marketCap", 0) or pair.get("fdv", 0)) if pair else None
    if not current_mc:
        current_mc = (data.get("initial_mc") or 1.0) * mult_30s

    ouvrir_position_signal(
        mint, "signal_lp_light", "Signal LP light (sans filtre)",
        "lp_locked_pct ÷ mult_30s", valeur, f"≤ {SIGNAL_LP_LIGHT_SEUIL}",
        current_mc,
    )


def evaluer_signal4_si_pret(mint, mc_reference=None):
    """Signal 4 — Momentum M3+M1 : price_change_m3 >= 12 ET
    price_change_m1 >= 10, entrée à 3 min, SANS AUCUN filtre qualité
    (comme Signal LP light — signal indépendant, purement momentum).
    Peut être appelée plusieurs fois ; ne s'exécute réellement qu'une fois
    que price_change_m3 ET price_change_m1 sont tous deux disponibles
    (ils ne sont calculés qu'à la fin de analyser_metriques_etendues, une
    fois la fenêtre des 180s écoulée), et ne se déclenche qu'une seule
    fois par token."""
    data = active_tokens.get(mint)
    if not data or data.get("signal4_evalue"):
        return

    price_change_m3 = data.get("price_change_m3")
    price_change_m1 = data.get("price_change_m1")

    # Données pas encore toutes disponibles -> on retentera au prochain appel
    if price_change_m3 is None or price_change_m1 is None:
        return

    data["signal4_evalue"] = True

    if price_change_m3 < SIGNAL4_PRICE_CHANGE_M3_MIN or price_change_m1 < SIGNAL4_PRICE_CHANGE_M1_MIN:
        print(
            f"[signal4] {data['symbol']} ({mint}) rejeté — "
            f"price_change_m3={price_change_m3} price_change_m1={price_change_m1}"
        )
        return

    current_mc = mc_reference
    if not current_mc:
        pair = fetch_pair_data(mint)
        current_mc = (pair.get("marketCap", 0) or pair.get("fdv", 0)) if pair else None
    if not current_mc:
        current_mc = data.get("initial_mc") or 1.0

    valeur_txt = f"m3={price_change_m3}% / m1={price_change_m1}%"

    ouvrir_position_signal(
        mint, "signal4", "Signal 4 — Momentum M3+M1",
        "price_change_m3 & price_change_m1", valeur_txt,
        f"m3 ≥ {SIGNAL4_PRICE_CHANGE_M3_MIN}% et m1 ≥ {SIGNAL4_PRICE_CHANGE_M1_MIN}%",
        current_mc,
    )


def evaluer_signaux_1_et_2(mint, price_change_m3, mc_reference):
    """Signaux 1 et 2, tous deux évalués à 3 minutes. Indépendants l'un de
    l'autre : les deux peuvent se déclencher sur le même token."""
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

    # --- Signal 1 : price_change_m3 ÷ avg_order_size_sol >= 24.2 ---
    if (
        mc_initial and mc_initial >= SIGNAL1_MC_MIN
        and buy_ratio_20s is not None and buy_ratio_20s >= SIGNAL1_BUY_RATIO_20S_MIN
        and avg_order_size_sol
    ):
        valeur1 = round(price_change_m3 / avg_order_size_sol, 3)
        if valeur1 >= SIGNAL1_SEUIL:
            ouvrir_position_signal(
                mint, "signal1", "Signal 1",
                "price_change_m3 ÷ avg_order_size_sol", valeur1, f"≥ {SIGNAL1_SEUIL}",
                current_mc,
            )
        else:
            print(f"[signal1] {data['symbol']} ({mint}) rejeté — valeur={valeur1}")
    else:
        print(
            f"[signal1] {data['symbol']} ({mint}) filtres non remplis "
            f"(mc_initial={mc_initial}, buy_ratio_20s={buy_ratio_20s}, avg_order_size_sol={avg_order_size_sol})"
        )

    # --- Signal 2 : score_rugcheck × price_change_m3 >= 15.3 ---
    if (
        mc_initial and mc_initial >= SIGNAL2_MC_MIN
        and buy_ratio_20s is not None and buy_ratio_20s >= SIGNAL2_BUY_RATIO_20S_MIN
        and score_rugcheck is not None
    ):
        valeur2 = round(score_rugcheck * price_change_m3, 3)
        if valeur2 >= SIGNAL2_SEUIL:
            ouvrir_position_signal(
                mint, "signal2", "Signal 2",
                "score_rugcheck × price_change_m3", valeur2, f"≥ {SIGNAL2_SEUIL}",
                current_mc,
            )
        else:
            print(f"[signal2] {data['symbol']} ({mint}) rejeté — valeur={valeur2}")
    else:
        print(
            f"[signal2] {data['symbol']} ({mint}) filtres non remplis "
            f"(mc_initial={mc_initial}, buy_ratio_20s={buy_ratio_20s}, score_rugcheck={score_rugcheck})"
        )


def gerer_positions_signaux(mint, current_mc):
    """Appelée à chaque cycle de prix (comme gerer_simulation_position) :
    met à jour le pic de chaque variante de trailing et clôture celles dont
    le prix repasse sous leur stop glissant (peu importe le pic, pas de TP)."""
    data = active_tokens.get(mint)
    if not data or not current_mc:
        return
    positions = data.get("positions") or {}
    for cle, pos in positions.items():
        entry_price = pos["entry_price"]
        for var_cle, pct in SIGNAUX_TRAILING_VARIANTS.items():
            tv = pos["trailing"][var_cle]
            if tv["statut"] != "ouverte":
                continue
            if current_mc > tv["peak"]:
                tv["peak"] = current_mc
                tv["sl"] = tv["peak"] * (1 + pct)
            if current_mc <= tv["sl"]:
                tv["statut"] = "trailing_stop"
                tv["resultat_pct"] = round((tv["sl"] / entry_price - 1) * 100, 2)
                tv["resultat_usd"] = round(SIGNAUX_MISE_USD * (tv["resultat_pct"] / 100), 2)
                print(
                    f"[{cle}/{var_cle}] {data['symbol']} ({mint}) — trailing stop touché, "
                    f"position clôturée ({tv['resultat_pct']:+.1f}%)"
                )


def cloturer_positions_signaux_expirees(mint, max_mc):
    """Appelée à l'expiration des 30 minutes de suivi du token : toute
    variante de trailing encore ouverte est clôturée au meilleur prix
    observé (pic de polling ou ATH réel via GeckoTerminal, le plus élevé
    des deux)."""
    data = active_tokens.get(mint)
    if not data:
        return
    positions = data.get("positions") or {}
    for cle, pos in positions.items():
        entry_price = pos["entry_price"]
        for var_cle in SIGNAUX_TRAILING_VARIANTS:
            tv = pos["trailing"][var_cle]
            if tv["statut"] != "ouverte":
                continue
            sortie = max(tv.get("peak", entry_price), max_mc or 0)
            tv["statut"] = "expire_30min"
            tv["resultat_pct"] = round((sortie / entry_price - 1) * 100, 2)
            tv["resultat_usd"] = round(SIGNAUX_MISE_USD * (tv["resultat_pct"] / 100), 2)
            print(
                f"[{cle}/{var_cle}] {data['symbol']} ({mint}) — expiration 30min, "
                f"résultat {tv['resultat_pct']:+.1f}%"
            )


SIGNAUX_LOG_FILE = "signaux_log.csv"

# --- Colonnes existantes : NE JAMAIS RIEN CHANGER ICI (ordre, noms) ---
SIGNAUX_LOG_FIELDS_EXISTANTES = [
    "horodatage", "mint", "symbole", "signal",
    "formule", "valeur_calculee", "seuil",
    "mc_initial_token", "mc_entree", "entree_a_s",
    "trail25_statut", "trail25_resultat_pct", "trail25_resultat_usd",
    "trail30_statut", "trail30_resultat_pct", "trail30_resultat_usd",
    "trail40_statut", "trail40_resultat_pct", "trail40_resultat_usd",
    "buy_ratio_20s", "score_rugcheck", "avg_order_size_sol",
    "lp_locked_pct", "mult_30s",
    "lien_dexscreener", "lien_axiom",
]

# --- Nouvelles colonnes ajoutées à la suite, préfixées pour rester
# explicites (rien de ce qui précède n'est modifié) :
#   avant__*  -> connues AU MOMENT du signal, utilisables comme variables
#                explicatives pour analyser ce qui prédit un bon trail25/30/40.
#   apres__*  -> calculées a posteriori sur toute la fenêtre de 30 min
#                (mc_max, simulations, time_to_peak, etc.). Ce sont des
#                résultats, pas des prédicteurs : utiles pour comparer des
#                stratégies de sortie, jamais pour "expliquer" le signal.
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


def log_resultats_signaux(mint, data, max_mc, row_rapport=None):
    """Écrit une ligne dans SIGNAUX_LOG_FILE pour chaque signal qui s'est
    déclenché sur ce token (signal1, signal2, signal3, signal_lp_light,
    signal4), avec le résultat des 3 variantes de trailing stop comparées
    (-25% / -30% / -40%). Les colonnes lp_locked_pct et mult_30s sont des
    métriques au niveau du token, déjà journalisées pour chaque ligne
    (utile en particulier pour signal3 / signal_lp_light).

    Les 26 colonnes existantes (SIGNAUX_LOG_FIELDS_EXISTANTES) restent
    inchangées. Si row_rapport est fourni (le dict déjà construit pour
    token_log.csv au même instant), les colonnes avant__/apres__ du
    rapport sont ajoutées à la SUITE, sur la même ligne."""
    positions = data.get("positions") or {}
    entry_stats = data.get("entry_stats", {})
    for cle, pos in positions.items():
        cle_unique = (mint, cle)
        if cle_unique in signaux_traites:
            continue
        signaux_traites.add(cle_unique)

        trailing = pos.get("trailing", {})
        t25 = trailing.get("trail25", {})
        t30 = trailing.get("trail30", {})
        t40 = trailing.get("trail40", {})

        row_signal = {
            "horodatage": time.strftime("%Y-%m-%d %H:%M:%S"),
            "mint": mint,
            "symbole": data.get("symbol"),
            "signal": pos.get("nom"),
            "formule": pos.get("formule_txt"),
            "valeur_calculee": pos.get("valeur_calculee"),
            "seuil": pos.get("seuil_txt"),
            "mc_initial_token": data.get("initial_mc"),
            "mc_entree": pos.get("entry_price"),
            "entree_a_s": pos.get("entry_elapsed_s"),
            "trail25_statut": t25.get("statut"),
            "trail25_resultat_pct": t25.get("resultat_pct"),
            "trail25_resultat_usd": t25.get("resultat_usd"),
            "trail30_statut": t30.get("statut"),
            "trail30_resultat_pct": t30.get("resultat_pct"),
            "trail30_resultat_usd": t30.get("resultat_usd"),
            "trail40_statut": t40.get("statut"),
            "trail40_resultat_pct": t40.get("resultat_pct"),
            "trail40_resultat_usd": t40.get("resultat_usd"),
            "buy_ratio_20s": data.get("buy_ratio_20s"),
            "score_rugcheck": data.get("rugcheck_score"),
            "avg_order_size_sol": entry_stats.get("avg_order_size_sol"),
            "lp_locked_pct": data.get("lp_locked_pct"),
            "mult_30s": data.get("mult_30s"),
            "lien_dexscreener": data.get("dex_url"),
            "lien_axiom": _lien_axiom(mint),
        }

        if row_rapport is not None:
            row_signal["horodatage_fin_suivi"] = row_rapport.get("horodatage")
            for k in AVANT_KEYS:
                row_signal[f"avant__{k}"] = row_rapport.get(k)
            for k in APRES_KEYS:
                row_signal[f"apres__{k}"] = row_rapport.get(k)

        log_resultat_signal_csv(row_signal)
    if positions:
        print(f"[signaux_log] {len(positions)} position(s) journalisée(s) pour {mint}")


def analyser_metriques_etendues(mint):
    data = active_tokens.get(mint)
    if not data:
        return
    initial_mc = data.get("initial_mc") or 1.0
    initial_price = data.get("initial_price")
    start = data["start_time"]

    samples = []

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

        # --- Signal 3 / Signal LP light (entrée à 30s) : dès qu'on atteint
        # ~30s d'âge, on calcule mult_30s en direct (mc actuel / mc initial)
        # et on tente l'évaluation des deux signaux (ne se déclenchent que
        # si toutes les données nécessaires sont là).
        if elapsed >= 30 and mint in active_tokens and not active_tokens[mint].get("mult_30s"):
            if mc:
                active_tokens[mint]["mult_30s"] = round(mc / initial_mc, 4)
            evaluer_signal3_si_pret(mint)
            evaluer_signal_lp_light_si_pret(mint)

        # --- Signaux 1 & 2 (entrée à 3min) : dès que la fenêtre des 3
        # minutes (180s) est atteinte, on évalue immédiatement, sans
        # attendre la fin de la boucle de 180s de collecte de métriques.
        if elapsed >= 180 and initial_price and price_usd and mint in active_tokens and not active_tokens[mint].get("signaux_1_2_evalues"):
            price_change_m3_instant = round((price_usd / initial_price - 1) * 100, 2)
            evaluer_signaux_1_et_2(mint, price_change_m3_instant, mc)

        prochain_t += METRIQUES_ETENDUES_INTERVAL
        if mint not in active_tokens:
            return

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
            # mult_30s a déjà pu être défini en direct plus haut dans la
            # boucle ; on ne l'écrase que s'il n'existe pas encore.
            "mult_30s": active_tokens[mint].get("mult_30s") or mult_30s,
            "mult_1m": mult_1m,
            "max_tx_per_second": round(max_tx_par_seconde, 2),
        })

    # Filet de sécurité : si les fenêtres 30s / 180s n'ont pas déclenché
    # l'évaluation en cours de route (ex: donnée absente à ce moment
    # précis), on l'exécute ici avec les valeurs finales calculées.
    evaluer_signal3_si_pret(mint)
    evaluer_signal_lp_light_si_pret(mint)
    mc_180 = _valeur_au_plus_proche(180, 1)
    evaluer_signaux_1_et_2(mint, price_change_m3, mc_180)
    # --- Signal 4 (entrée à 3min, momentum M3+M1) : price_change_m1 et
    # price_change_m3 viennent d'être écrits ci-dessus dans active_tokens,
    # c'est donc ici (et uniquement ici) qu'il peut être évalué pour la
    # première fois avec des données complètes.
    evaluer_signal4_si_pret(mint, mc_180)

    print(
        f"[metriques_etendues] {data.get('symbol')} ({mint}) — "
        f"mult_10s={mult_10s} mult_30s={active_tokens.get(mint, {}).get('mult_30s')} mult_1m={mult_1m} "
        f"buy_ratio_1m={buy_ratio_1m} price_change_m1={price_change_m1}"
    )


def essayer_alerter(mint, pair, source_url):
    # Garde-fou final avant toute alerte : on ne traite jamais une paire
    # qui ne serait pas explicitement sur Solana, même si elle a été
    # transmise par erreur par une source amont.
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

    # --- Filtre liquidité (MIN_LIQUIDITY_USD / MIN_LIQUIDITY_RATIO) SUPPRIMÉ :
    # demande explicite = suivre TOUS les tokens fraîchement migrés, sans
    # pré-filtre autre que RugCheck (score ≤ 20 + LP locked) plus bas.
    # passe_les_filtres() n'est plus appelée ici (elle reste définie plus
    # haut dans le fichier si jamais tu veux la réactiver plus tard).

    txns = pair.get("txns") or {}
    volume = pair.get("volume") or {}
    txns_m5 = txns.get("m5") or {}
    txns_h1 = txns.get("h1") or {}
    price_change_m5 = (pair.get("priceChange") or {}).get("m5")

    tot_m5 = (txns_m5.get("buys") or 0) + (txns_m5.get("sells") or 0)
    tot_h1 = (txns_h1.get("buys") or 0) + (txns_h1.get("sells") or 0)
    tx_accel = round((tot_m5 * 12) / tot_h1, 3) if tot_h1 > 0 else None

    # --- Filtre trigger (price_change_m5 / tx_accel) SUPPRIMÉ : ces valeurs
    # continuent d'être CALCULÉES ci-dessus (elles servent au logging CSV,
    # au calcul de l'ancien champ signal_valide, et aux Signaux 1/2/4), mais
    # elles ne bloquent plus l'entrée en suivi d'un token.

    # --- FILTRE D'ÂGE DU POOL SUPPRIMÉ : pool_age_seconds continue d'être
    # calculé (utile pour le CSV et pour is_golden_window), mais ne rejette
    # plus aucun token, quel que soit son âge au moment de la migration.
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

    flags_txt = ", ".join(rug_flags) if rug_flags else "Aucun"
    top10_txt = f"{top10_pct}%" if top10_pct is not None else "N/A"
    holders_txt = f"{total_holders}" if total_holders is not None else "N/A"
    bundle_txt = "⚠️ Oui" if bundle_detected else "Non"
    boost_txt = f"⚡ Oui ({nombre_boosts_actifs})" if boost_detecte else "Non"
    profil_txt = "✅ Oui" if a_un_profil else "Non"
    # Note : aucune alerte Telegram n'est envoyée ici. Le token passe en
    # suivi silencieux ; les signaux (evaluer_signal3_si_pret /
    # evaluer_signal_lp_light_si_pret / evaluer_signaux_1_et_2 /
    # evaluer_signal4_si_pret) ouvrent bien des positions simulées quand
    # leur critère est validé, mais n'envoient plus non plus de message
    # Telegram (voir ouvrir_position_signal, alertes commentées).
    print(f"Suivi démarré (silencieux) pour : {symbol} ({mint}) — RugCheck score={rug_score} top10={top10_pct}%")

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
        # --- État des signaux ---
        "signal3_evalue": False,
        "signal_lp_light_evalue": False,
        "signaux_1_2_evalues": False,
        "signal4_evalue": False,
        "positions": {},  # cle_signal ("signal1"/"signal2"/"signal3"/"signal_lp_light"/"signal4") -> dict de position simulée
    }
    seen_mints.add(mint)

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


# ============================================================
# --- TOKENS BOOSTÉS DEXSCREENER (quel que soit leur âge) ---
# ============================================================
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


def monitor_ath():
    global _last_price_check
    current_time = time.time()
    tokens_to_remove = []

    do_price_check = (current_time - _last_price_check) >= PRICE_CHECK_INTERVAL
    if do_price_check:
        _last_price_check = current_time

    for mint, data in list(active_tokens.items()):
        # --- CORRECTIF : tout le traitement par token est maintenant
        # protégé par un try/except. Avant ce correctif, une exception
        # levée pendant la finalisation d'UN SEUL token (calcul, division,
        # f-string sur une valeur inattendue, etc.) faisait planter tout
        # le process Python. Sur Railway, cela provoque un redémarrage du
        # service : active_tokens (en mémoire, jamais persisté) repart à
        # zéro, et TOUS les tokens en cours de suivi à ce moment-là
        # perdent leur suivi ET leur rapport 30 min, sans qu'aucune trace
        # ne soit visible ailleurs que dans les logs Railway. Avec ce
        # correctif, une erreur sur un token est loguée et ce token est
        # simplement retiré du suivi, sans affecter les autres tokens ni
        # la boucle principale du bot. ---
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

                        profil_detecte_maj, site_maj, twitter_maj, telegram_maj = extraire_infos_profil(pair0)
                        if profil_detecte_maj:
                            active_tokens[mint]["profil_dexscreener"] = True
                            active_tokens[mint]["site_web"] = active_tokens[mint].get("site_web") or site_maj
                            active_tokens[mint]["twitter"] = active_tokens[mint].get("twitter") or twitter_maj
                            active_tokens[mint]["telegram"] = active_tokens[mint].get("telegram") or telegram_maj

                        if current_mc:
                            gerer_simulation_position(mint, current_mc, elapsed)
                            gerer_positions_signaux(mint, current_mc)
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

                # --- Clôture des positions simulées des signaux (si encore
                # ouvertes à 30min, on les considère expirées au meilleur MC
                # observé depuis leur entrée, comme pour resultat_pct_simule) ---
                cloturer_positions_signaux_expirees(mint, max_mc)

                multiplicateur = max_mc / initial_mc
                dex_url = data.get("dex_url", f"https://dexscreener.com/solana/{mint}")

                if data.get("signal_valide") and data.get("resultat_pct_simule") is None:
                    entree = data.get("prix_entree_simule") or initial_mc
                    active_tokens[mint]["resultat_pct_simule"] = round((max_mc / entree - 1) * 100, 2)
                    if data.get("position_statut") in ("ouverte", "tp1", "trailing"):
                        active_tokens[mint]["position_statut"] = "expire_30min"
                data = active_tokens[mint]

                # --- RAPPORT 30 MIN : ALERTE TELEGRAM DÉSACTIVÉE ---
                # Auparavant, si au moins un des 5 signaux avait été
                # déclenché sur ce token (positions_declenchees non vide),
                # un message récapitulatif était envoyé sur Telegram. Pour
                # limiter les appels API, cet envoi est désactivé : on se
                # contente désormais de logger l'info en console. Tout le
                # calcul (multiplicateur, résultats trailing, etc.) reste
                # inchangé et continue d'être écrit dans les CSV plus bas.
                # Pour réactiver, dé-commenter le bloc ci-dessous.
                positions_declenchees = data.get("positions") or {}
                if positions_declenchees:
                    print(
                        f"[monitor_ath] Suivi 30 min terminé pour : {data['symbol']} — "
                        f"x{multiplicateur:,.2f} — {len(positions_declenchees)} signal(aux) validé(s) "
                        f"— [alerte Telegram désactivée, voir CSV]"
                    )
                    # boost_txt_rapport = (
                    #     f"Oui ({data.get('nombre_boosts_actifs', 0)})"
                    #     if data.get("boost_detecte") else "Non"
                    # )
                    #
                    # lignes_signaux = []
                    # for cle_pos, pos in positions_declenchees.items():
                    #     trailing = pos.get("trailing", {})
                    #     t25 = trailing.get("trail25", {})
                    #     t30 = trailing.get("trail30", {})
                    #     t40 = trailing.get("trail40", {})
                    #
                    #     def _fmt(tv):
                    #         pct = tv.get("resultat_pct")
                    #         return f"{pct:+.1f}%" if pct is not None else "N/A"
                    #
                    #     lignes_signaux.append(
                    #         f"🎯 *{pos.get('nom')}* (entrée à {pos.get('entry_elapsed_s')}s, "
                    #         f"MC ${pos.get('entry_price', 0):,.0f})\n"
                    #         f"   ↳ Trailing 25% : {_fmt(t25)} ({t25.get('statut')}) | "
                    #         f"30% : {_fmt(t30)} ({t30.get('statut')}) | "
                    #         f"40% : {_fmt(t40)} ({t40.get('statut')})"
                    #     )
                    #
                    # msg_rapport = (
                    #     f"📋 *Rapport 30 min — signal(aux) validé(s)*\n\n"
                    #     f"🪙 Token : {_echapper_markdown(data['symbol'])}\n"
                    #     f"💰 Market Cap initial (à la migration) : ${initial_mc:,.0f}\n"
                    #     f"🏆 Market Cap max atteint : ${max_mc:,.0f}\n"
                    #     f"✖️ Multiplicateur : x{multiplicateur:,.2f}\n"
                    #     f"🚀 Boosté : {boost_txt_rapport}\n\n"
                    #     + "\n\n".join(lignes_signaux) +
                    #     f"\n\n🔗 [Voir sur DexScreener]({dex_url})\n"
                    #     f"⚡ [Trader sur Axiom](https://axiom.trade/meme/{mint})"
                    # )
                    #
                    # LIMITE_TELEGRAM = 4000
                    # if len(msg_rapport) > LIMITE_TELEGRAM:
                    #     msg_rapport = msg_rapport[:LIMITE_TELEGRAM] + "\n\n… (message tronqué, voir /csv_signaux pour le détail complet)"
                    #
                    # envoye_rapport = send_telegram_message(msg_rapport)
                    # if envoye_rapport:
                    #     print(f"[monitor_ath] Rapport 30 min envoyé (signal validé) pour : {data['symbol']} — x{multiplicateur:,.2f}")
                    # else:
                    #     print(f"[monitor_ath] !!! ÉCHEC ENVOI du rapport 30 min pour {data['symbol']} ({mint}), voir erreur juste au-dessus !!!")
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

                # --- Nouveau tableau CSV : uniquement les signaux déclenchés
                # (1 ligne par signal validé sur ce token), avec les résultats
                # des 3 trailing stops comparés (-25% / -30% / -40%). Une
                # version enrichie (avant__*/apres__*) est écrite en parallèle
                # dans signaux_log_enrichi.csv, sans jamais toucher à
                # signaux_log.csv. ---
                log_resultats_signaux(mint, data, max_mc, row_rapport)

                tokens_to_remove.append(mint)

        except Exception as e:
            # Ne jamais laisser une erreur sur UN token faire planter tout
            # le process (et donc perdre le suivi de TOUS les autres
            # tokens en cours). On log en détail (avec la traceback) et on
            # retire ce token du suivi pour ne pas boucler indéfiniment
            # dessus.
            print(f"[monitor_ath] ERREUR lors de la finalisation de {mint} : {e}")
            traceback.print_exc()
            tokens_to_remove.append(mint)

    for mint in tokens_to_remove:
        active_tokens.pop(mint, None)


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

# --- Extraction du mint : UNIQUEMENT via des liens explicitement Solana ---
# Le canal @DexToolsPublic publie des tokens multi-chaînes (ETH, BSC, Base,
# Solana, etc.). Pour ne jamais capter un token d'une autre chaîne, on
# n'extrait un mint QUE s'il apparaît dans un lien qui pointe explicitement
# vers un explorateur/aggrégateur Solana. On abandonne le fallback "adresse
# base58 générique" qui pouvait matcher n'importe quel texte du post.
CA_FROM_LINK_RE = re.compile(
    r'(?:dexscreener\.com/solana/|solscan\.io/token/|birdeye\.so/token/|pump\.fun/(?:coin/)?)'
    r'([1-9A-HJ-NP-Za-km-z]{32,44})'
)

# Marqueurs texte qui indiquent que le post concerne une AUTRE chaîne que
# Solana (ETH/BSC/Base/Polygon/Arbitrum/etc.) : si l'un d'eux apparaît dans
# le message et qu'aucun lien Solana explicite n'a été trouvé, on rejette.
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
    """Retourne un mint UNIQUEMENT s'il provient d'un lien explicitement
    Solana. Retourne None si le post référence une autre chaîne, ou si
    aucun lien Solana explicite n'est trouvé (plus de fallback générique)."""
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
    # Confirmation finale via DexScreener : fetch_pair_data() ne renvoie
    # désormais QUE des paires chainId == "solana" (cf. plus haut). Si
    # aucune pair Solana n'est trouvée pour ce mint, on abandonne le
    # suivi plutôt que de tracker un token à l'aveugle.
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
        # --- CORRECTIF : la boucle principale est maintenant protégée par
        # un try/except global. Avant, une exception non prévue dans
        # n'importe laquelle des fonctions appelées ici faisait planter
        # tout le process -> redémarrage Railway -> perte de active_tokens
        # et de dextools_tracked (en mémoire uniquement), donc perte de
        # suivi et de rapports 30 min pour tous les tokens en cours. ---
        try:
            check_telegram_commands()
            check_new_solana_tokens()
            check_boosted_tokens_throttled()
            check_pending_tokens()
            monitor_ath()
            check_dextools_channel_throttled()
            monitor_dextools_ath()
        except Exception as e:
            print(f"[boucle_principale] ERREUR non gérée : {e}")
            traceback.print_exc()
        time.sleep(10)

