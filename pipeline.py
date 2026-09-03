#!/usr/bin/env python3
"""data.gouv.fr weekly dataset watch: download, classify, build a digest.

Each run downloads data.gouv.fr's own weekly bulk catalog export (one CSV
row per dataset) and classifies every dataset into a domain/activity
bucket. "New" and "updated" datasets are derived straight from data.gouv.fr's
own created_at / last_modified timestamps, filtered to a trailing window
(default 8 days) - this needs no state carried over from the previous run,
which matters because the scheduled cloud agent that runs this weekly starts
from a fresh sandbox each time with nothing persisted locally. The only
thing that needs to survive between runs is the digest itself, and that is
written straight to the Artifact's `db` capability (collection "digests",
one small JSON document per run) rather than kept as a local file.

Usage:
    python pipeline.py --download              # fetch a fresh catalog.csv
    python pipeline.py                          # reuse existing catalog.csv
    python pipeline.py --digest-json out.json   # also write the digest as JSON
                                                   (this is what gets pushed to
                                                   the Artifact's db each week)
"""
import argparse
import csv
import json
import re
import sys
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

CATALOG_URL = "https://www.data.gouv.fr/api/1/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3"
HERE = Path(__file__).parent
CATALOG_CSV = HERE / "catalog.csv"
REPORTS_DIR = HERE / "reports"
RECENT_WINDOW_DAYS = 8  # slightly over a week, to absorb schedule jitter

csv.field_size_limit(10_000_000)

# --- Taxonomy: domain -> keyword/tag matchers (whole-word or phrase) ------
# Every entry is matched as a whole word/phrase (regex \b...\b), never a raw
# substring - short fragments like "eau" or "plu" used to false-match inside
# "nouveaux" / "plus" and badly skewed the classification.

TAXONOMY = {
    "Sante": ["sante", "medical", "medicale", "hopital", "covid", "sanitaire", "handicap", "medicament", "epidemio", "soins?"],
    "Environnement & Energie": ["environnement", "biodiversite", "climat", "climatique", "energie", "energetique", "pollution", "dechets?", "eaux?", "hydraulique", "foret", "forestier", "ecologie", "ecologique", "carbone", "renouvelable", "nucleaire", "risques? naturels?", "inondations?"],
    "Transport & Mobilite": ["transports?", "mobilite", "routiers?", "circulation", "ferroviaire", "aerien", "aeroport", "velos?", "trafic", "stationnement", "autoroutes?", "voirie"],
    "Education & Recherche": ["education", "scolaire", "universite", "universitaire", "recherche", "enseignement", "etudiants?", "ecoles?", "formation"],
    "Economie & Finances": ["economie", "economique", "finances?", "financier", "budget", "budgetaire", "impots?", "fiscal", "fiscalite", "entreprises?", "commerce", "industrie", "industriel", "pib", "marche"],
    "Emploi & Travail": ["emploi", "chomage", "travail", "salaires?", "recrutement", "insertion professionnelle"],
    "Justice & Securite": ["justice", "securite", "police", "delinquance", "tribunal", "prisons?", "incendies?", "secours", "penal"],
    "Agriculture & Alimentation": ["agriculture", "agricole", "alimentation", "alimentaire", "elevage", "peche", "viticulture", "foncier agricole"],
    "Logement & Urbanisme": ["logements?", "urbanisme", "immobilier", "habitat", "cadastre", "construction", "plu\\b", "amenagement du territoire"],
    "Culture & Patrimoine": ["culture", "culturel", "patrimoine", "musees?", "bibliotheques?", "spectacles?", "tourisme", "touristique", "monuments?"],
    "Numerique": ["numerique", "open ?data", "algorithmes?", "logiciels?", "intelligence artificielle", "cybersecurite", "informatique", "application mobile", "site internet"],
    "International": ["international", "diplomatique", "cooperation internationale", "etranger", "visas?", "immigration", "asile"],
    "Collectivites & Administration": ["collectivites?", "communes?", "departements?", "regions?", "administration", "administratif", "elus?", "elections?", "mairies?", "prefecture", "intercommunalite"],
}
FALLBACK_DOMAIN = "Autre / Non classe"

_COMPILED_TAXONOMY = {
    domain: [re.compile(r"\b" + kw + r"\b") for kw in keywords]
    for domain, keywords in TAXONOMY.items()
}

# --- Sub-taxonomy: domain -> subcategory -> keyword/tag matchers ----------
# Same whole-word matching approach, scoped to a domain that's already been
# assigned - so "sante" doesn't need repeating inside every health subcategory.

SUBTAXONOMY = {
    "Sante": {
        "Etablissements & soins": ["hopital", "hopitaux", "clinique", "etablissement de sante", "soins?", "medecins?", "chirurgie"],
        "Epidemiologie & maladies": ["epidemio", "covid", "maladies?", "cancer", "pandemie"],
        "Handicap & medico-social": ["handicap", "medico-social", "ehpad", "dependance"],
        "Medicaments & produits de sante": ["medicament", "pharmaceutique", "vaccins?", "dispositifs? medicaux?"],
    },
    "Environnement & Energie": {
        "Biodiversite & ecologie": ["biodiversite", "ecologie", "ecologique", "faune", "flore", "especes?"],
        "Climat & risques naturels": ["climat", "climatique", "risques? naturels?", "inondations?", "secheresse", "carbone"],
        "Dechets & recyclage": ["dechets?", "recyclage", "tri selectif", "collecte"],
        "Energie & reseaux": ["energie", "energetique", "renouvelable", "nucleaire", "electricite", "gaz"],
        "Eau & hydrologie": ["eaux?", "hydraulique", "hydrologie", "assainissement", "rivieres?"],
        "Forets": ["foret", "forestier", "boisement"],
    },
    "Transport & Mobilite": {
        "Reseaux routiers": ["routiers?", "voirie", "autoroutes?"],
        "Transport en commun": ["transports? en commun", "bus", "tramway", "metro", "ferroviaire", "train"],
        "Mobilites actives": ["velos?", "pietons?", "mobilite douce"],
        "Trafic & stationnement": ["trafic", "circulation", "stationnement", "parking"],
        "Aerien & maritime": ["aerien", "aeroport", "maritime", "portuaire"],
    },
    "Education & Recherche": {
        "Etablissements scolaires": ["ecoles?", "scolaire", "college", "lycee"],
        "Enseignement superieur": ["universite", "universitaire", "grandes ecoles"],
        "Recherche": ["recherche", "scientifique", "laboratoire"],
        "Formation professionnelle": ["formation", "apprentissage", "alternance"],
    },
    "Economie & Finances": {
        "Finances publiques & budget": ["budget", "budgetaire", "finances publiques", "depenses publiques"],
        "Fiscalite": ["impots?", "fiscal", "fiscalite", "taxes?"],
        "Entreprises & commerce": ["entreprises?", "commerce", "industrie", "industriel", "pme"],
        "Marches publics": ["marches? publics?", "appels? d'offres?", "boamp"],
    },
    "Emploi & Travail": {
        "Marche du travail": ["chomage", "emploi", "marche du travail"],
        "Conditions de travail": ["salaires?", "conditions de travail", "convention collective"],
        "Insertion professionnelle": ["insertion professionnelle", "recrutement", "alternance"],
    },
    "Justice & Securite": {
        "Securite & police": ["securite", "police", "delinquance"],
        "Justice": ["justice", "tribunal", "penal", "juridique"],
        "Secours & incendie": ["incendies?", "secours", "pompiers?"],
    },
    "Agriculture & Alimentation": {
        "Production agricole": ["agriculture", "agricole", "cultures?", "foncier agricole"],
        "Elevage & peche": ["elevage", "peche", "aquaculture"],
        "Alimentation": ["alimentation", "alimentaire", "nutrition"],
        "Viticulture": ["viticulture", "vignes?", "vin"],
    },
    "Logement & Urbanisme": {
        "Cadastre & foncier": ["cadastre", "foncier", "parcelles?"],
        "Construction & habitat": ["construction", "habitat", "logements?"],
        "Amenagement du territoire": ["amenagement du territoire", "urbanisme", "plu\\b"],
        "Immobilier": ["immobilier"],
    },
    "Culture & Patrimoine": {
        "Patrimoine & monuments": ["patrimoine", "monuments?"],
        "Bibliotheques & musees": ["bibliotheques?", "musees?"],
        "Tourisme": ["tourisme", "touristique"],
        "Spectacles & evenements": ["spectacles?", "evenements? culturels?", "festival"],
    },
    "Numerique": {
        "Open data & plateformes": ["open ?data", "plateforme numerique"],
        "Intelligence artificielle": ["intelligence artificielle", "algorithmes?"],
        "Cybersecurite": ["cybersecurite"],
        "Applications & services numeriques": ["application mobile", "site internet", "logiciels?", "informatique"],
    },
    "International": {
        "Cooperation & diplomatie": ["cooperation internationale", "diplomatique"],
        "Immigration & asile": ["immigration", "asile", "visas?"],
    },
    "Collectivites & Administration": {
        "Communes & intercommunalites": ["communes?", "intercommunalite", "mairies?"],
        "Regions & departements": ["regions?", "departements?"],
        "Elections & elus": ["elus?", "elections?"],
        "Administration publique": ["administration", "administratif", "prefecture"],
    },
}
FALLBACK_SUBCATEGORY = "Autre"

# Doc-id-safe slug per domain, used when splitting per-domain tag-links into
# separate small Artifact db documents (see write-up in main()).
DOMAIN_SLUGS = {
    "Sante": "sante",
    "Environnement & Energie": "environnement-energie",
    "Transport & Mobilite": "transport-mobilite",
    "Education & Recherche": "education-recherche",
    "Economie & Finances": "economie-finances",
    "Emploi & Travail": "emploi-travail",
    "Justice & Securite": "justice-securite",
    "Agriculture & Alimentation": "agriculture-alimentation",
    "Logement & Urbanisme": "logement-urbanisme",
    "Culture & Patrimoine": "culture-patrimoine",
    "Numerique": "numerique",
    "International": "international",
    "Collectivites & Administration": "collectivites-administration",
}

# Platform/metadata boilerplate tags that show up almost everywhere regardless of
# subject matter - excluded from the tag-based third level since they carry no
# distinguishing signal (e.g. "donnees-ouvertes" tops nearly every subcategory).
TAG_STOPWORDS = {
    "donnees-ouvertes", "opendata", "open-data", "inspire", "passerelle-inspire",
    "geoscientific-information", "hvd", "france",
}

_COMPILED_SUBTAXONOMY = {
    domain: {
        sub: [re.compile(r"\b" + kw + r"\b") for kw in keywords]
        for sub, keywords in subs.items()
    }
    for domain, subs in SUBTAXONOMY.items()
}


def normalize(text):
    if not text:
        return ""
    import unicodedata
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return text.lower()


def classify_domain(tags_norm, text_norm):
    for domain, patterns in _COMPILED_TAXONOMY.items():
        if any(p.search(tags_norm) for p in patterns):
            return domain
    for domain, patterns in _COMPILED_TAXONOMY.items():
        if any(p.search(text_norm) for p in patterns):
            return domain
    return FALLBACK_DOMAIN


def classify_sub(domain, tags_norm, text_norm):
    subs = _COMPILED_SUBTAXONOMY.get(domain)
    if not subs:
        return None
    for sub, patterns in subs.items():
        if any(p.search(tags_norm) for p in patterns):
            return sub
    for sub, patterns in subs.items():
        if any(p.search(text_norm) for p in patterns):
            return sub
    return FALLBACK_SUBCATEGORY


# --- Download ---------------------------------------------------------------

def download_catalog(dest: Path):
    print(f"Downloading catalog to {dest} ...", file=sys.stderr)
    urllib.request.urlretrieve(CATALOG_URL, dest)
    print(f"Done: {dest.stat().st_size / 1e6:.1f} MB", file=sys.stderr)


# --- Parse + classify ---------------------------------------------------------------

def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_catalog(path: Path):
    records = []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            tags = [t for t in (row.get("tags") or "").split(",") if t]
            tags_norm = normalize(" ".join(tags))
            text_norm = normalize(f"{row.get('title', '')} {row.get('description_short', '')}")
            domain = classify_domain(tags_norm, text_norm)
            sub_domain = classify_sub(domain, tags_norm, text_norm)
            records.append({
                "id": row["id"],
                "title": row.get("title", ""),
                "organization": row.get("organization", ""),
                "url": row.get("url", ""),
                "license": row.get("license", ""),
                "created_at": parse_dt(row.get("created_at")),
                "last_modified": parse_dt(row.get("last_modified")),
                "archived": (row.get("archived") == "True"),
                "resources_formats": [x for x in re.split(r"[,;]", row.get("resources_formats") or "") if x],
                "domain": domain,
                "sub_domain": sub_domain,
                "tags": tags,
            })
    return records


# --- Digest ---------------------------------------------------------------

def build_digest(records, now, window_days=RECENT_WINDOW_DAYS, cap=40):
    window_start = now - timedelta(days=window_days)

    new_items = [r for r in records if r["created_at"] and r["created_at"] >= window_start]
    updated_items = [
        r for r in records
        if r["last_modified"] and r["last_modified"] >= window_start
        and not (r["created_at"] and r["created_at"] >= window_start)
    ]
    archived_items = [
        r for r in records
        if r["archived"] and r["last_modified"] and r["last_modified"] >= window_start
    ]

    domain_counts = Counter(r["domain"] for r in records)
    org_counts = Counter(r["organization"] or "(sans organisation)" for r in records)
    license_counts = Counter(r["license"] or "(non renseigne)" for r in records)
    format_counts = Counter(f.upper() for r in records for f in r["resources_formats"])

    subdomain_counts = {}
    for domain in SUBTAXONOMY:
        subs = Counter(r["sub_domain"] for r in records if r["domain"] == domain and r["sub_domain"])
        if subs:
            subdomain_counts[domain] = dict(subs.most_common())

    def is_recent(r):
        # Same trailing window as new_items/updated_items above, so the "recent"
        # marker on tags/links means the same thing as "new"/"mis a jour" elsewhere.
        return (
            (r["created_at"] and r["created_at"] >= window_start)
            or (r["last_modified"] and r["last_modified"] >= window_start)
        )

    # Third level: the real tags publishers attached, within each domain/subcategory -
    # data-driven rather than another hand-picked keyword layer, so it stays accurate
    # to what's actually in the catalog and needs no manual upkeep as tag usage shifts.
    subtag_counters = defaultdict(lambda: defaultdict(Counter))
    subtag_recent_sets = defaultdict(lambda: defaultdict(set))
    for r in records:
        if r["sub_domain"]:
            counter = subtag_counters[r["domain"]][r["sub_domain"]]
            recent_set = subtag_recent_sets[r["domain"]][r["sub_domain"]] if is_recent(r) else None
            for t in r["tags"]:
                t_clean = t.strip().lower()
                if t_clean and t_clean not in TAG_STOPWORDS:
                    counter[t_clean] += 1
                    if recent_set is not None:
                        recent_set.add(t_clean)

    subtag_counts = {}
    subtag_recent = {}
    for domain, subs in subtag_counters.items():
        for sub, counter in subs.items():
            top = {t: c for t, c in counter.most_common(10) if c >= 2}
            if top:
                subtag_counts.setdefault(domain, {})[sub] = top
                recent_here = sorted(subtag_recent_sets[domain][sub] & top.keys())
                if recent_here:
                    subtag_recent.setdefault(domain, {})[sub] = recent_here

    # Dataset links per (domain, subcategory, tag), for the tags that made the cut
    # above - collected in one more pass rather than a per-tag scan of all records.
    qualifying_tags = {
        (domain, sub, t)
        for domain, subs in subtag_counts.items()
        for sub, tags in subs.items()
        for t in tags
    }
    RAW_CAP = 60
    subtag_item_pool = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for r in records:
        if not r["sub_domain"]:
            continue
        domain, sub = r["domain"], r["sub_domain"]
        for t in r["tags"]:
            t_clean = t.strip().lower()
            key = (domain, sub, t_clean)
            if key in qualifying_tags:
                bucket = subtag_item_pool[domain][sub][t_clean]
                if len(bucket) < RAW_CAP:
                    bucket.append(r)

    LINKS_CAP = 12
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    subtag_links = {}
    for domain, subs in subtag_item_pool.items():
        for sub, tags in subs.items():
            for t, items in tags.items():
                items_sorted = sorted(
                    items, key=lambda r: r["last_modified"] or r["created_at"] or epoch, reverse=True
                )
                links = [
                    {
                        "title": r["title"], "url": r["url"], "organization": r["organization"],
                        "recent": is_recent(r),
                    }
                    for r in items_sorted[:LINKS_CAP]
                ]
                subtag_links.setdefault(domain, {}).setdefault(sub, {})[t] = links

    def brief(r):
        return {
            "title": r["title"], "url": r["url"], "domain": r["domain"],
            "sub_domain": r["sub_domain"], "organization": r["organization"],
        }

    new_items.sort(key=lambda r: r["created_at"], reverse=True)
    updated_items.sort(key=lambda r: r["last_modified"], reverse=True)

    return {
        "date": now.strftime("%Y-%m-%d"),
        "generated_at": now.isoformat(),
        "window_days": window_days,
        "total": len(records),
        "new_count": len(new_items),
        "updated_count": len(updated_items),
        "archived_count": len(archived_items),
        "domain_counts": dict(domain_counts.most_common()),
        "subdomain_counts": subdomain_counts,
        "subtag_counts": subtag_counts,
        "subtag_recent": subtag_recent,
        "subtag_links": subtag_links,
        "org_counts_top": dict(org_counts.most_common(15)),
        "license_counts_top": dict(license_counts.most_common(10)),
        "format_counts_top": dict(format_counts.most_common(10)),
        "new_items": [brief(r) for r in new_items[:cap]],
        "updated_items": [brief(r) for r in updated_items[:cap]],
    }


# --- Markdown rendering (local readability only) ---------------------------

def render_markdown(digest):
    lines = [f"# data.gouv.fr — rapport du {digest['date']}", ""]
    lines.append(f"- Total datasets: **{digest['total']}**")
    lines.append(f"- Nouveaux (<= {digest['window_days']}j): **{digest['new_count']}**")
    lines.append(f"- Mis a jour (<= {digest['window_days']}j): **{digest['updated_count']}**")
    lines.append(f"- Archives (<= {digest['window_days']}j): **{digest['archived_count']}**")
    lines.append("")

    lines.append("## Repartition par domaine")
    for domain, count in digest["domain_counts"].items():
        lines.append(f"- {domain}: {count}")
        for sub, sub_count in digest.get("subdomain_counts", {}).get(domain, {}).items():
            lines.append(f"  - {sub}: {sub_count}")
            tags = digest.get("subtag_counts", {}).get(domain, {}).get(sub, {})
            if tags:
                tag_str = ", ".join(f"{t} ({c})" for t, c in tags.items())
                lines.append(f"    - tags: {tag_str}")
    lines.append("")

    lines.append("## Top organisations")
    for org, count in digest["org_counts_top"].items():
        lines.append(f"- {org}: {count}")
    lines.append("")

    lines.append("## Licences")
    for lic, count in digest["license_counts_top"].items():
        lines.append(f"- {lic}: {count}")
    lines.append("")

    if digest["new_items"]:
        lines.append(f"## Nouveaux datasets (max {len(digest['new_items'])} affiches)")
        for r in digest["new_items"]:
            lines.append(f"- [{r['title']}]({r['url']}) — {r['domain']} — {r['organization']}")
        lines.append("")

    if digest["updated_items"]:
        lines.append(f"## Datasets mis a jour (max {len(digest['updated_items'])} affiches)")
        for r in digest["updated_items"]:
            lines.append(f"- [{r['title']}]({r['url']}) — {r['domain']} — {r['organization']}")
        lines.append("")

    return "\n".join(lines)


# --- Main ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--download", action="store_true", help="fetch a fresh catalog.csv")
    parser.add_argument("--digest-json", help="also write the digest as JSON to this path")
    args = parser.parse_args()

    if args.download or not CATALOG_CSV.exists():
        download_catalog(CATALOG_CSV)

    print("Parsing + classifying ...", file=sys.stderr)
    records = parse_catalog(CATALOG_CSV)
    print(f"Parsed {len(records)} datasets", file=sys.stderr)

    now = datetime.now(timezone.utc)
    digest = build_digest(records, now)

    REPORTS_DIR.mkdir(exist_ok=True)
    report_path = REPORTS_DIR / f"report_{digest['date']}.md"
    report_path.write_text(render_markdown(digest), encoding="utf-8")
    print(f"Report written to {report_path}", file=sys.stderr)

    # subtag_links is dropped from the digest itself and written as a separate
    # file, split by domain slug: at ~1MB+ total it would blow past the Artifact
    # db's 256 KiB per-document cap if kept in the (one-document) digest. Split
    # per domain it comes to well under that per document, and the dashboard
    # fetches a domain's doc lazily, only once a tag chip in it is clicked.
    subtag_links = digest.pop("subtag_links", {})
    tag_links_by_slug = {
        DOMAIN_SLUGS[domain]: {"domain": domain, "subs": subs}
        for domain, subs in subtag_links.items()
        if domain in DOMAIN_SLUGS
    }

    if args.digest_json:
        digest_path = Path(args.digest_json)
        digest_path.write_text(json.dumps(digest, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Digest JSON written to {digest_path}", file=sys.stderr)

        links_path = digest_path.parent / "tag_links.json"
        links_path.write_text(json.dumps(tag_links_by_slug, ensure_ascii=False, indent=2), encoding="utf-8")
        sizes = {slug: len(json.dumps(v, ensure_ascii=False)) for slug, v in tag_links_by_slug.items()}
        print(f"Tag links JSON written to {links_path} (largest domain doc: {max(sizes.values(), default=0)} bytes)", file=sys.stderr)

    print(json.dumps({k: v for k, v in digest.items() if k not in ("new_items", "updated_items")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
