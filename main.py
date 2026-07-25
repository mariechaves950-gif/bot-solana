import os
import re
import csv
import html
import time
import threading
import requests
from datetime import datetime

# ============================================================
# SCRIPT 100% INDÉPENDANT — ne touche à rien du bot existant.
#
# Objectif : lire TOUS les posts du canal Telegram public
# t.me/DexToolsPublic (sans aucun filtre — on prend tout ce qui est
# posté), extraire les infos affichées dans le message d'alerte
# (Pair Age, variation 24h, volume, buys/sells, boosts...), puis
# suivre le token pendant 24h pour calculer le multiplicateur entre
# le market cap au moment de l'alerte et l'ATH atteint sur ces 24h.
# Le tout est loggé dans un CSV séparé, avec la date/heure de
# l'alerte et une estimation de la date/heure de l'ATH.
#
# À lancer comme un service séparé (process séparé de bot.py),
# par ex. un second service Railway avec sa propre variable
# TELEGRAM_TOKEN/CHAT_ID (peuvent être les mêmes que le bot existant).
# ============================================================

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    raise RuntimeError("TELEGRAM_TOKEN ou CHAT_ID manquant dans les variables d'environnement")

# --- Canal source ---
DEXTOOLS_CHANNEL = "DexToolsPublic"
DEXTOOLS_CHANNEL_URL = f"https://t.me/s/{DEXTOOLS_CHANNEL}"

# --- Fichier CSV dédié (complètement séparé du CSV du bot existant) ---
DEXTOOLS_LOG_FILE = "dextools_channel_log.csv"

# --- Timing ---
DEXTOOLS_CHANNEL_CHECK_INTERVAL = 60     # secondes entre 2 lectures du canal
DEXTOOLS_PRICE_CHECK_INTERVAL = 300      # secondes entre 2 vérifications de prix (24h de suivi -> pas besoin d'aller vite)
DEXTOOLS_TRACK_DURATION = 24 * 3600      # durée totale de suivi avant de clôturer et logger le CSV

dextools_tracked = {}        # mint -> données de suivi en cours
dextools_seen_posts = set()  # ids de posts déjà traités (anti-doublon)

_dextools_last_price_check = 0

_dextools_update_offset = 0  # pour la commande Telegram /csv_dextools


# ============================================================
# --- FONCTIONS RÉUTILISÉES / ADAPTÉES DU BOT EXISTANT ---
# (copiées ici pour garder ce script totalement indépendant)
# ============================================================

def _to_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
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
            print(f"[dextools] Erreur Telegram ({r.status_code}) : {r.text}")
    except Exception as e:
        print(f"[dextools] Erreur d'envoi Telegram : {e}")


def send_telegram_document(filepath, caption=None):
    if not os.path.isfile(filepath):
        send_telegram_message(f"⚠️ Aucun fichier `{filepath}` pour le moment (aucun token n'a encore terminé ses 24h de suivi).")
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
                print(f"[dextools] Erreur envoi document Telegram ({r.status_code}) : {r.text}")
    except Exception as e:
        print(f"[dextools] Erreur d'envoi du document Telegram : {e}")


def check_telegram_commands():
    """Commande dédiée /csv_dextools pour récupérer ce CSV précis (offset séparé de celui du bot principal)."""
    global _dextools_update_offset
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    params = {"offset": _dextools_update_offset + 1, "timeout": 0}
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code != 200:
            return
        updates = res.json().get("result", [])
    except Exception as e:
        print(f"[dextools][check_telegram_commands] erreur : {e}")
        return

    for update in updates:
        _dextools_update_offset = max(_dextools_update_offset, update.get("update_id", 0))
        message = update.get("message") or {}
        chat_id = str((message.get("chat") or {}).get("id", ""))
        text = (message.get("text") or "").strip().lower()

        if chat_id != str(CHAT_ID):
            continue

        if text in ("/csv_dextools", "/log_dextools"):
            send_telegram_document(DEXTOOLS_LOG_FILE, caption="📊 Historique canal DexToolsPublic (multiplicateur alerte → ATH 24h)")


def fetch_pair_data(mint):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        res = requests.get(url, timeout=5)
        if res.status_code != 200:
            return None
        pairs = res.json().get("pairs")
        return pairs[0] if pairs else None
    except Exception as e:
        print(f"[dextools][fetch_pair_data] erreur pour {mint} : {e}")
        return None


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
    telegram_link = None
    for s in socials:
        if not isinstance(s, dict):
            continue
        type_social = (s.get("type") or "").lower()
        if type_social in ("twitter", "x") and not twitter:
            twitter = s.get("url")
        elif type_social == "telegram" and not telegram_link:
            telegram_link = s.get("url")

    return a_un_profil, site_web, twitter, telegram_link


RUGCHECK_MAX_SCORE = 20  # gardé uniquement à titre informatif dans le CSV, ne filtre RIEN ici


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
    """Version purement informative : ne rejette jamais rien, sert juste à remplir des colonnes du CSV."""
    try:
        url = f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
        res = requests.get(url, timeout=8)
        if res.status_code != 200:
            return None, [], None, 0, None, None, False

        data = res.json()
        score = data.get("score_normalised")
        risks = data.get("risks") or []
        flags = [r.get("name", "?") for r in risks if r.get("level") in ("warn", "danger")]
        lp_locked = data.get("lpLockedPct", 0)

        top10_pct, insiders_detected = _extraire_concentration_holders(data)
        total_holders, bundle_detected = _extraire_holders_et_bundle(data)

        return score, flags, top10_pct, insiders_detected, lp_locked, total_holders, bundle_detected
    except Exception as e:
        print(f"[dextools][rugcheck] erreur pour {mint} : {e}")
        return None, [], None, 0, None, None, False


def get_ath_24h_via_gecko(pool_address):
    """
    Récupère jusqu'à 24 bougies horaires GeckoTerminal et retourne
    (plus_haut_prix_usd, timestamp_unix_de_la_bougie) ou (None, None).
    Best-effort : si le pool n'existe pas encore sur GeckoTerminal (token
    trop récent) ou que l'API échoue, on retombe sur le suivi par polling.
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


# ============================================================
# --- PARSING DU CANAL TELEGRAM PUBLIC (t.me/s/DexToolsPublic) ---
# ============================================================
# On utilise la page "preview" publique de Telegram (pas besoin de
# bot ajouté au canal, ni d'API_ID/API_HASH). Format HTML stable mais
# non documenté officiellement -> à vérifier si Telegram change son
# widget. Aucune dépendance externe (pas de bs4) : parsing par regex.

POST_BLOCK_RE = re.compile(
    r'data-post="' + re.escape(DEXTOOLS_CHANNEL) + r'/(?P<post_id>\d+)".*?'
    r'<time[^>]*datetime="(?P<datetime>[^"]+)"',
    re.DOTALL,
)

MESSAGE_TEXT_RE = re.compile(
    r'<div class="tgme_widget_message_text[^"]*"[^>]*>(?P<text>.*?)</div>',
    re.DOTALL,
)

# Adresse Solana (base58, ni 0/O/I/l) — best-effort pour repérer la CA
SOLANA_ADDRESS_RE = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')

# Liens vers un explorer/aggrégateur contenant directement le mint
CA_FROM_LINK_RE = re.compile(
    r'(?:dexscreener\.com/solana/|solscan\.io/token/|birdeye\.so/token/|pump\.fun/(?:coin/)?)'
    r'([1-9A-HJ-NP-Za-km-z]{32,44})'
)

TICKER_RE = re.compile(r'\$([A-Za-z0-9]{2,15})\b')
PAIR_AGE_RE = re.compile(r'Pair Age:\s*([^\n📈🔥💧👥🛡️🚩🕵️🚀🌐🔗⚡️👶📊]+)', re.IGNORECASE)
CHANGE_24H_RE = re.compile(r'24h:\s*([+\-0-9.,]+%)\s*\|\s*V:\s*\$?([0-9.,a-zA-Z]+)', re.IGNORECASE)
BUYS_SELLS_RE = re.compile(r'Buys:\s*([0-9.,a-zA-Z]+)\s*\|\s*Sells:\s*([0-9.,a-zA-Z]+)', re.IGNORECASE)
BOOSTS_RE = re.compile(r'Boosts?:\s*([0-9]+)', re.IGNORECASE)


def _nettoyer_html(fragment_html):
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


def extraire_mint(message_html, message_txt):
    """Cherche d'abord une CA dans un lien connu (plus fiable), sinon retombe sur un pattern base58 brut."""
    m = CA_FROM_LINK_RE.search(message_html)
    if m:
        return m.group(1)
    candidats = SOLANA_ADDRESS_RE.findall(message_txt)
    return candidats[0] if candidats else None


def extraire_infos_alerte(message_html):
    """
    Extrait les infos affichées dans le message d'alerte du canal, au format
    donné en exemple : Pair Age / 24h % / Volume / Buys / Sells / Boosts.
    Tout est best-effort et stocké tel quel (texte brut) : le canal peut
    changer son format, chaque champ manque proprement (None) sans planter.
    """
    texte = _nettoyer_html(message_html)

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
    """Lit le canal, repère les nouveaux posts, démarre le suivi 24h pour chaque nouveau token trouvé (sans exception)."""
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
        message_txt = _nettoyer_html(message_html)

        mint = extraire_mint(bloc, message_txt)
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

        infos_alerte = extraire_infos_alerte(message_html)
        demarrer_suivi(mint, infos_alerte, post_id, alert_time, alert_dt)


def demarrer_suivi(mint, infos_alerte, post_id, alert_time, alert_dt):
    """Enrichit le token avec les données dispo au moment de l'alerte et l'ajoute au suivi 24h."""

    pair = fetch_pair_data(mint)
    mc_initial, prix_initial, liquidite_usd, pool_address = None, None, None, None
    if pair:
        mc_initial = pair.get("marketCap", 0) or pair.get("fdv", 0) or None
        prix_initial = _to_float(pair.get("priceUsd"))
        liquidite_usd = (pair.get("liquidity") or {}).get("usd")
        pool_address = pair.get("pairAddress")

    score_rugcheck, flags_rugcheck, top10_pct, insiders_detected, lp_locked_pct, total_holders, bundle_detected = rugcheck_verdict(mint)

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


# ============================================================
# --- SUIVI DE L'ATH SUR 24H + CLÔTURE / LOG CSV ---
# ============================================================

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
        cloturer_suivi(mint)


def cloturer_suivi(mint):
    data = dextools_tracked.get(mint)
    if not data:
        return

    mc_initial = data.get("mc_initial") or 1.0

    gecko_high, gecko_ts = get_ath_24h_via_gecko(data.get("pool_address"))
    mc_ath_gecko, gecko_dt_str = None, None
    if gecko_high and data.get("prix_initial"):
        mc_ath_gecko = mc_initial * (gecko_high / data["prix_initial"])
        try:
            gecko_dt_str = datetime.fromtimestamp(gecko_ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            gecko_dt_str = None

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


if __name__ == "__main__":
    print("Suivi du canal Telegram DexToolsPublic démarré (indépendant du bot principal)...")
    while True:
        check_telegram_commands()
        check_dextools_channel()
        monitor_dextools_ath()
        time.sleep(DEXTOOLS_CHANNEL_CHECK_INTERVAL)
