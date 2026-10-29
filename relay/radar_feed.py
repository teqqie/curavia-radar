#!/usr/bin/env python3
"""CuraVia Radar-Feed: holt taeglich Bekanntmachungen aus dem Datenservice
Oeffentlicher Einkauf (oeffentlichevergabe.de, Open-Data-Export) und TED (API v3),
filtert auf Personenbefoerderung rund um 61381 Friedrichsdorf und schreibt
kleine JSON-Dateien nach data/. Nur Standardbibliothek."""
import datetime as dt, hashlib, io, json, os, re, sys, time, urllib.request, urllib.error, zipfile
import xml.etree.ElementTree as ET

DATA = os.environ.get("RADAR_DATA", "data")
UA = "CuraVia-Radar/1.2 (GitHub Actions; taeglicher Abruf offener Vergabedaten)"
BACKFILL_DAYS = int(os.environ.get("NACHERFASSEN_TAGE") or 0)  # einmalige Nacherfassung (manueller Start)
HOME = (50.260667, 8.6276263)  # 61381 Friedrichsdorf
PLZ_GEO_URL = "https://raw.githubusercontent.com/WZBSocialScienceCenter/plz_geocoord/master/plz_geocoord.csv"
# PLZ der Prioritaet-1-Kreise (Hochtaunuskreis, Wetteraukreis)
P1_PLZ = {"61348", "61350", "61352", "61381", "61389", "61440", "61449", "61462", "61476", "61479", "61250", "61267",
          "61273", "61276", "61279", "61118", "61169", "61184", "61191", "61194", "61197", "61200", "61203", "61206",
          "61209", "61231", "61239", "63654", "63667", "63683", "63688", "63691", "63695", "63697", "63699", "35510", "35516"}
TODAY = dt.date.today()
MAX_CATCHUP_DAYS = 10
FIRST_RUN_DAYS = 14

# ---- Fachliche Filter -------------------------------------------------------
REL_CPV = ("6010", "6011", "6012", "6013", "6014", "6017", "85311200")
BROAD_CPV = ("60000000",)
KW = re.compile(r"bef(ö|oe)rder|fahrdienst|krankenfahrt|sch(ü|ue)ler(verkehr|spezial|fahr)|patient|rollstuhl|"
                r"behindert|tagespflege|werkstatt.{0,20}fahr|senioren.{0,10}fahr|mietwagen|taxi|shuttle|"
                r"bedarfsverkehr|on-demand|rufbus|anruf-?sammeltaxi|personenverkehr|fahrleistung", re.I)
EXCLUDE = re.compile(r"\bpost|brief|umzug|spedition|schnee|kunst|luftverkehr|g(ü|ue)ter|abfall|m(ü|ue)ll|"
                     r"fahrradverleih|mietfahrr|carsharing|leasing|lieferung von", re.I)

# ungefaehre Kreismitte (fuer Bekanntmachungen ohne PLZ des Leistungsorts)
NUTS_CENTER = {
    "DE718": (50.30, 8.55), "DE71E": (50.35, 8.90), "DE712": (50.11, 8.68), "DE71A": (50.08, 8.45), "DE713": (50.10, 8.77),
    "DE71C": (50.02, 8.80), "DE719": (50.20, 9.20), "DE721": (50.55, 8.75), "DE723": (50.43, 8.20), "DE722": (50.60, 8.35),
    "DE725": (50.60, 9.25), "DE714": (50.08, 8.24), "DE71D": (50.12, 8.05), "DE711": (49.87, 8.65), "DE716": (49.85, 8.80),
    "DE717": (49.92, 8.45), "DE724": (50.80, 8.70), "DEB35": (49.99, 8.25), "DEB3J": (49.92, 8.05), "DE261": (49.97, 9.15),
    "DE264": (50.00, 9.20), "DEB1B": (50.28, 7.85), "DE269": (49.75, 9.25),
}
NUTS_COARSE = {"DE7", "DE71", "DE72", "DE73", "DEU"}
NUTS_PRIO = {**{k: 3 for k in NUTS_CENTER}, **{k: 3 for k in NUTS_COARSE if k != "DEU"}}  # Vorfilter fuer TED
NUTS_NAME = {"DE718": "Hochtaunuskreis", "DE71E": "Wetteraukreis", "DE712": "Frankfurt am Main", "DE71A": "Main-Taunus-Kreis",
             "DE713": "Offenbach (Stadt)", "DE71C": "Kreis Offenbach", "DE719": "Main-Kinzig-Kreis", "DE721": "Landkreis Gießen",
             "DE723": "Limburg-Weilburg", "DE722": "Lahn-Dill-Kreis", "DE725": "Vogelsbergkreis", "DE714": "Wiesbaden",
             "DE71D": "Rheingau-Taunus-Kreis", "DE711": "Darmstadt", "DE716": "Darmstadt-Dieburg", "DE717": "Groß-Gerau",
             "DE724": "Marburg-Biedenkopf", "DEB35": "Mainz", "DEB3J": "Mainz-Bingen", "DE261": "Aschaffenburg (Stadt)",
             "DE264": "Landkreis Aschaffenburg", "DEB1B": "Rhein-Lahn-Kreis", "DE7": "Hessen", "DE71": "Südhessen", "DE72": "Mittelhessen"}


RAIL = re.compile(r"schienenpersonen|\bspnv\b|s-bahn|eisenbahn|schienenverkehr|zugverkehr|regionalbahn|"
                  r"schienentransport|schienenbef(ö|oe)rder|triebwagen", re.I)


def is_rail(cpvs, text):
    if re.search(r"schienenersatz", text or "", re.I):
        return False  # SEV wird mit Bussen gefahren -> Strasse
    return any(str(c).startswith(("6020", "6021", "6022", "6023")) for c in cpvs) or bool(RAIL.search(text or ""))


def in_area(regions, plzs):
    """grober Vorfilter; die genaue Entfernung prueft locate()"""
    return any(r in NUTS_CENTER or r in NUTS_COARSE for r in regions) or \
        any(re.fullmatch(r"(35|55|56|6[0-5])\d{3}", p or "") for p in plzs)


_GEO = None


def plz_geo():
    global _GEO
    if _GEO is None:
        _GEO = {}
        try:
            code, body = http(PLZ_GEO_URL, tries=3, timeout=60)
            if code == 200:
                for line in body.decode("utf-8").splitlines()[1:]:
                    p = line.split(",")
                    if len(p) == 3:
                        _GEO[p[0]] = (float(p[1]), float(p[2]))
        except Exception:
            pass
    return _GEO


def km(a, b):
    import math
    la1, lo1, la2, lo2 = map(math.radians, (*a, *b))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(h))


def locate(e, buyer_plz=None):
    """Setzt entfernung_km (Luftlinie ab 61381 Friedrichsdorf), ort_genauigkeit und prioritaet.
    Gibt False zurueck, wenn der Leistungsort sicher ausserhalb von 50 km liegt."""
    geo = plz_geo()
    plzs = [p for p in e.get("plz", []) if p]
    dists = [km(HOME, geo[p]) for p in plzs if p in geo]
    exact = [r for r in e.get("nuts", []) if r in NUTS_CENTER]
    if dists:
        d, acc = min(dists), "PLZ des Leistungsorts"
    elif exact:
        d, acc = min(km(HOME, NUTS_CENTER[r]) for r in exact), "Kreismitte (ungefähr)"
    elif buyer_plz and buyer_plz in geo:
        d, acc = km(HOME, geo[buyer_plz]), "nur Adresse des Auftraggebers – Leistungsort prüfen"
    else:
        d, acc = None, "Leistungsort unklar – prüfen"
    p1 = any(p in P1_PLZ for p in plzs) or any(r in ("DE718", "DE71E") for r in exact)
    if d is not None and d > 50 and not p1:
        # Kreismitte ist ungenau: grosse Kreise am Rand bis 60 km zulassen
        if acc.startswith("Kreismitte") and d <= 60:
            acc += ", Kreis reicht ggf. in den 50-km-Umkreis"
        else:
            return False
    e["entfernung_km"] = round(d) if d is not None else None
    e["ort_genauigkeit"] = acc
    e["prioritaet"] = 1 if p1 else (2 if d is not None and d <= 30 else 3)
    return True


def relevant(cpvs, text):
    cpv_hit = any(str(c).startswith(REL_CPV) for c in cpvs)
    kw_hit = bool(KW.search(text or ""))
    broad = any(str(c).startswith(BROAD_CPV) for c in cpvs)
    if EXCLUDE.search(text or "") and not kw_hit:
        return False
    return cpv_hit or (kw_hit and (broad or any(str(c).startswith(("60", "85", "98")) for c in cpvs)))


def norm_key(title, buyer):
    t = re.sub(r"[^a-z0-9äöüß]+", " ", (title or "").lower()).strip()[:90]
    # lange Titel sind eindeutig genug (Auftraggeber-Namen unterscheiden sich zwischen Portalen)
    b = "" if len(t) >= 30 else re.sub(r"[^a-z0-9äöüß]+", " ", (buyer or "").lower()).strip()[:12]
    return hashlib.sha1(f"{t}|{b}".encode()).hexdigest()[:16]


# ---- HTTP -------------------------------------------------------------------
def http(url, data=None, headers=None, tries=5, timeout=90):
    last = None
    for a in range(tries):
        try:
            req = urllib.request.Request(url, data=data, headers={"User-Agent": UA, **(headers or {})})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(8 * (a + 1)); continue
            return e.code, e.read() if hasattr(e, "read") else b""
        except Exception as e:  # Netzwerkfehler
            last = e; time.sleep(8 * (a + 1))
    raise RuntimeError(f"{url}: {last}")


def load(name, default):
    try:
        with open(os.path.join(DATA, name), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save(name, obj):
    os.makedirs(DATA, exist_ok=True)
    with open(os.path.join(DATA, name), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)


def day_list(start, end):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


# ---- Datenservice -----------------------------------------------------------
def ocds_entries(zbytes, day):
    out = []
    with zipfile.ZipFile(io.BytesIO(zbytes)) as z:
        for name in z.namelist():
            try:
                j = json.loads(z.read(name).decode("utf-8"))
            except Exception:
                continue
            for rel in j.get("releases", []):
                tags = rel.get("tag") or []
                if any(t in ("award", "awardUpdate", "contract", "contractUpdate", "contractTermination") for t in tags):
                    continue
                t = rel.get("tender") or {}
                items = t.get("items") or []
                cpvs = sorted({c for i in items for c in [(i.get("classification") or {}).get("id")] +
                               [a.get("id") for a in (i.get("additionalClassifications") or [])] if c})
                lots = t.get("lots") or []
                text = " ".join([t.get("title") or "", t.get("description") or ""] + [(l.get("title") or "") for l in lots])
                if not relevant(cpvs, text):
                    continue
                regions = sorted({(i.get("deliveryAddress") or {}).get("region") for i in items} - {None})
                plzs = sorted({(i.get("deliveryAddress") or {}).get("postalCode") for i in items} - {None})
                locs = sorted({(i.get("deliveryAddress") or {}).get("locality") for i in items} - {None})
                # PLZ steckt manchmal in locality ("61169 Friedberg")
                plzs += [m for l in locs for m in re.findall(r"\b\d{5}\b", l or "")]
                buyer = rel.get("buyer") or {}
                # Sind weder PLZ noch NUTS-Code des Leistungsorts vorhanden, darf
                # die PLZ des Auftraggebers als unsicherer Ersatz verwendet werden.
                # Nicht durch den zweiten Vorfilter erneut aussortieren!
                buyer_plz = ((buyer.get("address") or {}).get("postalCode") or "")
                if not in_area(regions, plzs):
                    if regions or plzs or not in_area([], [buyer_plz]):
                        continue
                ends = sorted({x for x in [(t.get("tenderPeriod") or {}).get("endDate")] +
                               [(l.get("tenderPeriod") or {}).get("endDate") for l in lots] if x})
                val = (t.get("value") or {}).get("amount")
                lvals = [(l.get("value") or {}).get("amount") for l in lots if (l.get("value") or {}).get("amount")]
                uri = j.get("uri") or ""
                nid = rel.get("id")
                m = re.search(r"/notices/([^?]+)\?.*noticeVersion=([^&]+)", uri)
                kind = "Vorinformation" if "planning" in tags else ("Änderung" if any("Update" in x or "amendment" in x for x in tags) else "Ausschreibung")
                out.append({
                    "quelle": "Datenservice", "id": f"DS-{nid}", "titel": t.get("title"), "auftraggeber": buyer.get("name"),
                    "art": kind, "verfahren": t.get("procurementMethodDetails") or t.get("procurementMethod"),
                    "nuts": regions, "plz": sorted(set(p for p in plzs if p)), "orte": locs,
                    "region": ", ".join(NUTS_NAME.get(r, r) for r in regions) or ", ".join(locs),
                    "prioritaet": None, "cpv": cpvs[:6], "frist": ends[0][:10] if ends else "",
                    "frist_quelle": "OCDS" if ends else "", "wert": val or (sum(lvals) if lvals else None),
                    "lose": len(lots), "veroeffentlicht": day,
                    "link": next((d.get("url") for d in (t.get("documents") or []) if d.get("url")), None)
                            or f"https://oeffentlichevergabe.de/ui/de/search/details?noticeId={nid}",
                    "datenservice_api": f"https://oeffentlichevergabe.de/api/notices/{m.group(1)}?format=ocds&noticeVersion={m.group(2)}" if m else uri,
                    "beschreibung": (t.get("description") or "")[:600],
                    "_buyer_plz": (buyer.get("address") or {}).get("postalCode"),
                    "bereich": "Schiene" if is_rail(cpvs, text) else "Straße",
                    "_eforms": f"https://oeffentlichevergabe.de/api/notices/{m.group(1)}?format=eforms&noticeVersion={m.group(2)}" if m else None,
                })
    return out


NS = {"cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
      "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"}


def eforms_enrich(e):
    """Frist/Wert aus der eForms-Fassung nachladen, falls OCDS sie nicht enthaelt (best effort)."""
    url = e.pop("_eforms", None)
    if e.get("frist") or not url:
        return
    try:
        st, body = http(url, tries=2, timeout=40)
        if st != 200 or not body.strip().startswith(b"<"):
            return
        root = ET.fromstring(body)
        for path, label in ((".//cac:TenderSubmissionDeadlinePeriod/cbc:EndDate", "Angebotsfrist"),
                            (".//cac:ParticipationRequestReceptionPeriod/cbc:EndDate", "Teilnahmefrist")):
            vals = sorted({x.text[:10] for x in root.findall(path, NS) if x.text})
            if vals:
                e["frist"], e["frist_quelle"] = vals[0], f"eForms ({label})"
                break
        if not e.get("wert"):
            v = [x.text for x in root.findall(".//cbc:EstimatedOverallContractAmount", NS) if x.text]
            if v:
                e["wert"] = float(v[0])
    except Exception:
        pass


def run_datenservice(status):
    st = status.get("datenservice", {})
    last = st.get("last_ok_day")
    yesterday = TODAY - dt.timedelta(days=1)
    start = (dt.date.fromisoformat(last) + dt.timedelta(days=1)) if last else TODAY - dt.timedelta(days=FIRST_RUN_DAYS)
    start = max(start, yesterday - dt.timedelta(days=MAX_CATCHUP_DAYS))
    if BACKFILL_DAYS:
        start = TODAY - dt.timedelta(days=BACKFILL_DAYS)
    pending = sorted(set(st.get("failed_days", [])) | {d.isoformat() for d in day_list(start, yesterday)})
    entries, ok, failed, waiting, unavailable, scanned = [], [], [], [], [], 0
    for day in pending:
        try:
            code, body = http(f"https://oeffentlichevergabe.de/api/notice-exports?pubDay={day}&format=ocds.zip")
            if code == 200 and body[:2] == b"PK":
                entries += ocds_entries(body, day)
                with zipfile.ZipFile(io.BytesIO(body)) as z:
                    scanned += len(z.namelist())
                ok.append(day)
            elif code in (400, 404) and day >= (TODAY - dt.timedelta(days=2)).isoformat():
                waiting.append(day)  # Export noch nicht erstellt -> beim naechsten Lauf erneut
            elif code in (400, 404):
                # Bei der Nacherfassung ist ein fehlender Export KEIN erfolgreich
                # gepruefter Tag (z. B. ausserhalb der Archiv-Aufbewahrung).
                unavailable.append(day)
            else:
                failed.append(f"{day} (HTTP {code})")
        except Exception as ex:
            failed.append(f"{day} ({str(ex)[:80]})")
        time.sleep(2)
    entries = [e for e in entries if locate(e, e.pop("_buyer_plz", None))]
    for e in entries:
        eforms_enrich(e)
        time.sleep(1)
    retry = [f.split(" ")[0] for f in failed] + waiting
    status["datenservice"] = {
        "status": "erfolgreich" if ok and not failed and not unavailable else ("teilweise" if ok else ("wartet auf Export" if waiting and not failed and not unavailable else "nicht erreichbar")),
        "geprueft_tage": ok, "fehlgeschlagen": failed, "noch_nicht_verfuegbar": waiting,
        "nicht_verfuegbar_tage": unavailable,
        "failed_days": retry[-MAX_CATCHUP_DAYS:],
        "last_ok_day": max(ok + ([last] if last else [])) if (ok or last) else None,
        "bekanntmachungen_gelesen": scanned, "treffer": len(entries),
    }
    return entries


# ---- TED --------------------------------------------------------------------
TED_NUTS = " ".join(k for k in NUTS_PRIO)
TED_CPV = "60000000 60100000 60112000 60120000 60130000 60140000 60170000 60171000 60172000 85311200"
TED_FIELDS = ["publication-number", "notice-title", "buyer-name", "place-of-performance", "deadline-receipt-tender-date-lot",
              "publication-date", "notice-type", "buyer-city", "classification-cpv", "estimated-value-lot", "estimated-value-proc"]
TED_FIELDS_EXTRA = TED_FIELDS + ["place-of-performance-city-lot", "place-of-performance-post-code-lot", "deadline-receipt-request-date-lot"]


def first_lang(v):
    if isinstance(v, dict):
        for k in ("deu", "eng"):
            if v.get(k):
                return v[k][0] if isinstance(v[k], list) else v[k]
        for x in v.values():
            return x[0] if isinstance(x, list) else x
    if isinstance(v, list):
        return v[0] if v else None
    return v


def run_ted(status):
    st = status.get("ted", {})
    last = st.get("last_ok_day")
    since = (dt.date.fromisoformat(last) - dt.timedelta(days=2)) if last else TODAY - dt.timedelta(days=FIRST_RUN_DAYS)
    if BACKFILL_DAYS:
        since = TODAY - dt.timedelta(days=BACKFILL_DAYS)
    fields = TED_FIELDS_EXTRA
    q = (f"classification-cpv IN ({TED_CPV}) AND place-of-performance IN ({TED_NUTS}) "
         f"AND publication-date>={since.strftime('%Y%m%d')}")
    out, total = [], 0
    try:
        for page in range(1, 5):
            body = json.dumps({"query": q, "fields": fields, "limit": 250, "page": page}).encode()
            code, raw = http("https://api.ted.europa.eu/v3/notices/search", data=body, headers={"Content-Type": "application/json"})
            if code == 400 and fields is TED_FIELDS_EXTRA:  # Zusatzfelder nicht unterstuetzt -> Standardfelder
                fields = TED_FIELDS
                body = json.dumps({"query": q, "fields": fields, "limit": 250, "page": page}).encode()
                code, raw = http("https://api.ted.europa.eu/v3/notices/search", data=body, headers={"Content-Type": "application/json"})
            if code != 200:
                raise RuntimeError(f"HTTP {code}: {raw[:200]!r}")
            j = json.loads(raw)
            total = j.get("totalNoticeCount", 0)
            for n in j.get("notices", []):
                nt = n.get("notice-type") or ""
                if nt.startswith("can") or nt in ("veat",):
                    continue
                title = first_lang(n.get("notice-title")) or ""
                cpvs = n.get("classification-cpv") or []
                if not relevant(cpvs, title):
                    continue
                regs = sorted(set(r for r in (n.get("place-of-performance") or []) if r and r != "DEU"))
                tplz = sorted({str(p) for p in (n.get("place-of-performance-post-code-lot") or []) if p})
                tcity = sorted({str(first_lang(c)) for c in (n.get("place-of-performance-city-lot") or []) if c})
                if not in_area(regs, tplz):
                    continue
                dl = sorted({d[:10] for d in (n.get("deadline-receipt-tender-date-lot") or []) + (n.get("deadline-receipt-request-date-lot") or []) if d})
                val = first_lang(n.get("estimated-value-proc")) or first_lang(n.get("estimated-value-lot"))
                pub = n.get("publication-number")
                out.append({
                    "quelle": "TED", "id": f"TED-{pub}", "titel": (title.split(" – ", 2)[2] if title.startswith("Deutschland – ") and title.count(" – ") >= 2 else title),
                    "auftraggeber": first_lang(n.get("buyer-name")), "art": "Vorinformation" if nt.startswith("pin") else "Ausschreibung",
                    "verfahren": nt, "nuts": regs, "plz": tplz, "orte": tcity or ([first_lang(n.get("buyer-city"))] if n.get("buyer-city") else []),
                    "region": ", ".join(NUTS_NAME.get(r, r) for r in regs), "prioritaet": None, "cpv": cpvs[:6],
                    "bereich": "Schiene" if is_rail(cpvs, title) else "Straße",
                    "frist": dl[0] if dl else "", "frist_quelle": "TED" if dl else "", "wert": float(val) if val not in (None, "") else None,
                    "lose": None, "veroeffentlicht": (n.get("publication-date") or "")[:10],
                    "link": f"https://ted.europa.eu/de/notice/{pub}/html", "beschreibung": "",
                })
            if page * 250 >= total:
                break
        out = [e for e in out if locate(e)]
        status["ted"] = {"status": "erfolgreich", "seit": since.isoformat(), "bekanntmachungen_gelesen": total,
                         "treffer": len(out), "last_ok_day": TODAY.isoformat()}
    except Exception as ex:
        status["ted"] = {**st, "status": "nicht erreichbar", "fehler": str(ex)[:300], "seit": since.isoformat()}
    return out


# ---- Zusammenfuehren, Delta -------------------------------------------------
def fingerprint(e):
    return hashlib.sha1(json.dumps([e.get("titel"), e.get("frist"), e.get("art"), e.get("wert")], ensure_ascii=False).encode()).hexdigest()[:12]


def merge(entries, seen):
    by_key = {}
    for e in entries:
        k = norm_key(e["titel"], e["auftraggeber"])
        e["key"] = k
        if k in by_key:  # Duplikat aus zweiter Quelle zusammenfuehren
            a = by_key[k]
            a["quellen"] = sorted(set(a["quellen"]) | {e["quelle"]})
            for f in ("frist", "wert", "link", "beschreibung", "lose"):
                if not a.get(f) and e.get(f):
                    a[f] = e[f]
            a["prioritaet"] = min(a["prioritaet"] or 3, e["prioritaet"] or 3)
            for f in ("entfernung_km", "ort_genauigkeit", "plz"):
                if e.get("ort_genauigkeit", "").startswith("PLZ") and not (a.get("ort_genauigkeit") or "").startswith("PLZ"):
                    a[f] = e.get(f)
            a.setdefault("weitere_ids", []).append(e["id"])
        else:
            e["quellen"] = [e["quelle"]]
            by_key[k] = e
    delta = []
    for k, e in by_key.items():
        fp = fingerprint(e)
        old = seen.get(k)
        if not old:
            e["delta"] = "neu"; delta.append(e)
            seen[k] = {"fp": fp, "erstmals": TODAY.isoformat(), "frist": e.get("frist"), "art": e.get("art"), "titel": e.get("titel")}
        elif old.get("fp") != fp:
            changes = []
            if old.get("frist") != e.get("frist"):
                changes.append(f"Frist geändert: {old.get('frist') or 'k. A.'} → {e.get('frist') or 'k. A.'}")
            if old.get("art") != e.get("art"):
                changes.append(f"{old.get('art')} → {e.get('art')}")
            e["delta"] = "geändert"; e["aenderung"] = "; ".join(changes) or "Angaben geändert"
            delta.append(e)
            seen[k].update({"fp": fp, "frist": e.get("frist"), "art": e.get("art")})
        seen[k]["zuletzt"] = TODAY.isoformat()
    # seen klein halten (180 Tage)
    cutoff = (TODAY - dt.timedelta(days=180)).isoformat()
    for k in [k for k, v in seen.items() if v.get("zuletzt", v.get("erstmals", "")) < cutoff]:
        del seen[k]
    delta.sort(key=lambda e: (e["prioritaet"], e.get("frist") or "9999"))
    return delta


def main():
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    status = load("status.json", {})
    seen = load("seen.json", {})
    status.pop("fehler", None)
    ds = run_datenservice(status)
    ted = run_ted(status)
    for src in ("datenservice", "ted"):
        s = status.get(src, {})
        s["lauf"] = now
        if s.get("status") in ("erfolgreich", "teilweise", "wartet auf Export"):
            s["letzte_erfolgreiche_aktualisierung"] = now
        status[src] = s
    entries = []
    for e in ds + ted:
        e.pop("_buyer_plz", None); e.pop("_eforms", None)
        e["frist_status"] = ("abgelaufen" if e.get("frist") and e["frist"] < TODAY.isoformat()
                             else "offen" if e.get("frist") else "unbekannt")
        if BACKFILL_DAYS:  # Nacherfassung: nur noch offene Verfahren und aktuelle Vorinformationen
            age = (TODAY - dt.date.fromisoformat(e["veroeffentlicht"][:10])).days if e.get("veroeffentlicht") else 0
            if e["frist_status"] == "abgelaufen":
                continue
            if e["frist_status"] == "unbekannt" and e.get("art") != "Vorinformation" and age > 60:
                continue
            if e.get("art") == "Vorinformation" and age > 180:
                continue
        entries.append(e)
    delta = merge(entries, seen)
    status["nacherfassung_tage"] = BACKFILL_DAYS or None
    # Genauigkeit der Umkreissuche nachvollziehbar machen: Wenn die PLZ-Datei
    # nicht geladen wurde, bleibt nur die ungenauere NUTS-Kreismitte.
    status["plz_geodaten"] = {"status": "erfolgreich" if plz_geo() else "nicht erreichbar (nur ungefaehre Ortspruefung)",
                            "anzahl_postleitzahlen": len(plz_geo())}
    ok_sources = [s for s in ("datenservice", "ted") if status[s].get("status") in ("erfolgreich", "teilweise", "wartet auf Export")]
    status["erstellt"] = now
    status["gesamtstatus"] = ("erfolgreich" if len(ok_sources) == 2 else "teilweise" if ok_sources else "fehlgeschlagen")
    status["gueltig"] = bool(ok_sources)
    status["hinweis"] = ("delta.json enthält nur Treffer der Quellen mit Status erfolgreich/teilweise. "
                         "Quellen mit 'nicht erreichbar' wurden in diesem Lauf NICHT geprüft.")
    status["delta_anzahl"] = len(delta)
    save("status.json", status)
    save("seen.json", seen)
    save("delta.json", {"erstellt": now, "gueltig": bool(ok_sources), "gepruefte_quellen": ok_sources, "eintraege": delta})
    if ok_sources:  # Verlauf nur bei gueltigem Lauf fortschreiben, damit ein verpasster Claude-Lauf nichts verliert
        hist = load("delta_verlauf.json", [])
        hist = ([{"erstellt": now, "gepruefte_quellen": ok_sources, "eintraege": delta}] + hist)[:14]
        save("delta_verlauf.json", hist)
    print(json.dumps(status, ensure_ascii=False, indent=1))
    sys.exit(0 if ok_sources else 1)


if __name__ == "__main__":
    main()
