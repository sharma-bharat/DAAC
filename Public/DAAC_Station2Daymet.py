#!/usr/bin/env python3
"""
DAAC_Station2Daymet.py

by Bharat Sharma based on the original scripts by Michele Thornton

=========================================================================
Single-script workflow: Environment and Climate Change Canada (ECCC) daily
climate station observations  ->  Daymet station-input format.

Replaces the 2025 multi-script workflow:
    ca_https_urllists_2025_prov.sh   (build per-province URL lists)
    download_2025_stndata.sh         (wget loop)
    processCAstns.py                 (CSV -> .stndata)
    id_files_for_stnList_writefile.py(list transformed files)
    create_stnList.py                (build stnList_<year>.csv)
plus the manual `cat`, `mv`, and BAD/good sorting steps.

Original:  Michele Thornton, ORNL
This version is Consolidated: single-entry pipeline, resumable, year-agnostic.

-------------------------------------------------------------------------
USAGE
-------------------------------------------------------------------------
  # everything, current data year, into ./<year>/
  python DAAC_Station2Daymet.py --year 2025 --root ./main_dir

  # only re-run the transform + stnList (source CSVs already downloaded)
  python DAAC_Station2Daymet.py --year 2025 --root ./main_dir --stages transform,stnlist

  # quick smoke test: 5 stations from two provinces
  python DAAC_Station2Daymet.py --year 2025 --root ./main_dir --provinces AB,BC --limit 5

-------------------------------------------------------------------------
OUTPUT TREE  (main_dir/<year>/)
-------------------------------------------------------------------------
  urls/<PROV>_<year>_URLlist.txt      per-province lists (kept for reference)
  urls/ALLPROV_<year>_URLlist.txt     concatenated list
  climate_station_list.csv            ECCC master list (elevation source)
  source_stn_data/*.csv               downloaded daily station CSVs
  transformed/good/*.stndata          stations that pass the QC gate
  transformed/bad/*.stndata           stations that fail (kept, not deleted)
  stnList_<year>.csv                  lon,lat,elev,CAE0<id>,name  (GOOD only)
  qc_report_<year>.csv                per-station QC audit trail
  logs/run_<timestamp>.log            full run log
=========================================================================
"""

from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

# ------------------------------------------------------------------ config
BASE_URL = "https://dd.weather.gc.ca/today/climate/observations"
DAILY_URL = f"{BASE_URL}/daily/csv"
STATION_LIST_URL = f"{BASE_URL}/climate_station_list.csv"

PROVINCES = [
    "AB",
    "BC",
    "MB",
    "NB",
    "NL",
    "NS",
    "NT",
    "NU",
    "ON",
    "PE",
    "QC",
    "SK",
    "YT",
]

STN_ID_PREFIX = "CAE0"  # ORNL convention for ECCC direct-access stations
MISSING = -9999.0  # Daymet nodata
TRACE_PRECIP_MM = 0.001  # trace flag 'T' -> same value used for GHCND
USER_AGENT = "ORNL-Daymet-CA-ingest/1.0 (+daymet.ornl.gov)"

# climate_daily_<PROV>_<CLIMATEID>_<YEAR>_P1D.csv
FNAME_RE = re.compile(
    r"^climate_daily_(?P<prov>[A-Z]{2})_(?P<climate_id>[^_]+)_(?P<year>\d{4})_P1D\.csv$"
)

log = logging.getLogger("ca2daymet")

# functions- initial, get data


def http_get(url: str, retries: int = 4, timeout: int = 60) -> bytes:
    """GET with exponential backoff. Replaces the wget loop."""
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            TimeoutError,
            OSError,
        ) as exc:
            last = exc
            if isinstance(exc, urllib.error.HTTPError) and exc.code == 404:
                break
            time.sleep(2**attempt)
    raise RuntimeError(f"GET failed for {url}: {last}")


def daymet_calendar(year: int) -> pd.DatetimeIndex:
    """
    Daymet uses a fixed 365-day year: in leap years December 31 is dropped.
    This replaces the manual comment-out/comment-in of the
    `if index < len(df) - 1` line that the 2024/2025 scripts required.
    """
    days = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
    if calendar.isleap(year):
        days = days[:-1]
    assert len(days) == 365, f"{year}: expected 365 Daymet days, got {len(days)}"
    return days


def climate_id_from_filename(path: Path) -> str | None:
    m = FNAME_RE.match(path.name)
    return m.group("climate_id") if m else None


# Step 1 : build URL lists
# ------------------------


def stage_urls(paths: dict, year: int, provinces: list[str], force: bool) -> Path:
    """Parse each province directory index for <year>_P1D.csv links."""
    url_dir = paths["urls"]
    url_dir.mkdir(parents=True, exist_ok=True)
    all_list = url_dir / f"ALLPROV_{year}_URLlist.txt"

    if all_list.exists() and not force:
        n = sum(1 for _ in all_list.open())
        log.info("URL list exists (%d URLs) - reusing. Use --force to rebuild.", n)
        return all_list

    href_re = re.compile(rf'href="([^"]*_{year}_P1D\.csv)"')
    all_urls: list[str] = []

    for prov in provinces:
        base = f"{DAILY_URL}/{prov}/"
        try:
            html = http_get(base).decode("utf-8", errors="ignore")
        except RuntimeError as exc:
            log.error("%s: directory listing failed (%s)", prov, exc)
            continue
        # dedupe but keep order
        names = list(dict.fromkeys(href_re.findall(html)))
        urls = [base + n for n in names]
        (url_dir / f"{prov}_{year}_URLlist.txt").write_text("\n".join(urls) + "\n")
        all_urls.extend(urls)
        log.info("%4d %s", len(urls), prov)

    all_list.write_text("\n".join(all_urls) + "\n")
    log.info("TOTAL %d URLs -> %s", len(all_urls), all_list)
    return all_list


# Step 2 : download station CSVs + master station list
# ----------------------------------------------------


def stage_download(
    paths: dict, year: int, url_file: Path, jobs: int, limit: int | None, force: bool
) -> None:
    src = paths["source"]
    src.mkdir(parents=True, exist_ok=True)

    # master station list (elevation lookup) - always refresh once per run
    stn_list = paths["year"] / "climate_station_list.csv"
    if force or not stn_list.exists():
        stn_list.write_bytes(http_get(STATION_LIST_URL))
        log.info("Downloaded %s", stn_list.name)

    urls = [u.strip() for u in url_file.read_text().splitlines() if u.strip()]
    if limit:
        urls = urls[:limit]

    todo = []
    for u in urls:
        dest = src / u.rsplit("/", 1)[-1]
        if dest.exists() and dest.stat().st_size > 0 and not force:
            continue
        todo.append((u, dest))

    log.info(
        "Download: %d of %d files needed (%d already present)",
        len(todo),
        len(urls),
        len(urls) - len(todo),
    )

    failures: list[str] = []

    def fetch(job):
        u, dest = job
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            tmp.write_bytes(http_get(u))
            tmp.replace(dest)  # atomic -> safe to interrupt and resume
            return None
        except Exception as exc:  # noqa: BLE001
            tmp.unlink(missing_ok=True)
            return f"{u}\t{exc}"

    if todo:
        with ThreadPoolExecutor(max_workers=jobs) as pool:
            for i, res in enumerate(
                as_completed(pool.submit(fetch, j) for j in todo), 1
            ):
                r = res.result()
                if r:
                    failures.append(r)
                if i % 100 == 0:
                    log.info("  ... %d/%d", i, len(todo))

    if failures:
        f = paths["logs"] / f"download_failures_{year}.txt"
        f.write_text("\n".join(failures) + "\n")
        log.warning("%d downloads failed - see %s", len(failures), f)
    log.info("source_stn_data now holds %d CSVs", len(list(src.glob("*.csv"))))


# Step 3 : transform to .stndata and split good / bad
# ---------------------------------------------------


def transform_one(
    csv_path: Path,
    year: int,
    days: pd.DatetimeIndex,
    threshold: int,
    qc_days: pd.DatetimeIndex | None = None,
) -> dict:
    """
    Convert one ECCC daily CSV to Daymet .stndata content.

    Returns a QC record dict; 'lines' holds the output rows.

    Rules (unchanged from processCAstns.py):
      * Data Quality flag not null  -> tmax/tmin/prcp = nodata for that day
      * Total Precip Flag == 'T'    -> precip = 0.001 mm (trace, as in GHCND)
      * precip converted mm -> cm, 4 decimals
      * missing -> -9999
    New:
      * rows are placed on the fixed 365-day Daymet calendar by DATE, not by
        row position, so partial-year files, duplicate rows, out-of-order rows
        and leap years can no longer shift the record off by a day.
      * Climate ID is taken from the filename, so IDs such as 611E001 are
        never coerced to 6110.0 by the CSV parser.
    """
    rec = {
        "source_file": csv_path.name,
        "climate_id": "",
        "station_name": "",
        "lon": np.nan,
        "lat": np.nan,
        "n_rows_source": 0,
        "n_days_expected": len(days),
        "n_days_scored": len(days if qc_days is None else qc_days),
        "missing_tmax": len(days),
        "missing_tmin": len(days),
        "missing_prcp": len(days),
        "n_quality_flagged": 0,
        "n_trace_precip": 0,
        "n_duplicate_dates": 0,
        "status": "BAD",
        "reason": "",
        "lines": None,
    }

    cid = climate_id_from_filename(csv_path)
    if cid is None:
        rec["reason"] = (
            "filename does not match climate_daily_<PROV>_<ID>_<YEAR>_P1D.csv"
        )
        return rec
    rec["climate_id"] = cid

    # Climate ID / Station Name forced to str: prevents 611E001 -> 6110.0
    df = pd.read_csv(
        csv_path,
        encoding="iso-8859-1",
        dtype={"Climate ID": str, "Station Name": str},
        low_memory=False,
    )
    # ECCC headers carry a degree sign; drop it so column names are stable
    df.rename(columns=lambda c: c.replace("\u00b0", "").strip(), inplace=True)

    required = [
        "Date/Time",
        "Data Quality",
        "Max Temp (C)",
        "Min Temp (C)",
        "Total Precip (mm)",
        "Total Precip Flag",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA

    rec["n_rows_source"] = len(df)
    if df.empty:
        rec["reason"] = "empty source file"
        return rec

    for col in ("Longitude (x)", "Latitude (y)", "Station Name"):
        if col in df.columns and df[col].notna().any():
            val = df[col].dropna().iloc[0]
            rec[
                (
                    "lon"
                    if col.startswith("Lon")
                    else "lat" if col.startswith("Lat") else "station_name"
                )
            ] = val

    # ---- QC / value rules (order preserved from the original script) ------
    tri = ["Max Temp (C)", "Min Temp (C)", "Total Precip (mm)"]
    for c in tri:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    flagged = df["Data Quality"].notnull()
    rec["n_quality_flagged"] = int(flagged.sum())
    df.loc[flagged, tri] = np.nan

    trace = df["Total Precip Flag"].astype("string").str.strip().eq("T").fillna(False)
    rec["n_trace_precip"] = int(trace.sum())
    df["Total Precip (mm)"] = np.where(trace, TRACE_PRECIP_MM, df["Total Precip (mm)"])

    # ---- place on the fixed Daymet calendar ------------------------------
    df["date"] = pd.to_datetime(df["Date/Time"], errors="coerce")
    df = df.dropna(subset=["date"])
    rec["n_duplicate_dates"] = int(df["date"].duplicated().sum())
    df = df.drop_duplicates(subset="date", keep="first").set_index("date").sort_index()

    out = df.reindex(days)[tri]

    # missing counts drive the good/bad gate; when --qc-through is used they are
    # counted only over the elapsed part of the year (mid-season runs)
    qwin = out if qc_days is None else out.reindex(qc_days)
    rec["n_days_scored"] = len(qwin)
    rec["missing_tmax"] = int(qwin["Max Temp (C)"].isna().sum())
    rec["missing_tmin"] = int(qwin["Min Temp (C)"].isna().sum())
    rec["missing_prcp"] = int(qwin["Total Precip (mm)"].isna().sum())

    prcp_cm = (out["Total Precip (mm)"] * 0.10).round(4)
    tmax = out["Max Temp (C)"].fillna(MISSING)
    tmin = out["Min Temp (C)"].fillna(MISSING)
    prcp_cm = prcp_cm.fillna(MISSING)

    rec["lines"] = [
        f"{i}\t{a}\t{b}\t{c:.4f}\n"
        for i, (a, b, c) in enumerate(zip(tmax.values, tmin.values, prcp_cm.values))
    ]

    # ---- good / bad gate --------
    if (
        rec["missing_tmax"] >= threshold
        and rec["missing_tmin"] >= threshold
        and rec["missing_prcp"] >= threshold
    ):
        rec["reason"] = (
            f"tmax/tmin/prcp all missing >= {threshold} days "
            f"({rec['missing_tmax']}/{rec['missing_tmin']}/{rec['missing_prcp']})"
        )
    else:
        rec["status"] = "GOOD"
    return rec


def stage_transform(
    paths: dict, year: int, threshold: int, jobs: int, qc_through: str | None = None
) -> pd.DataFrame:
    src = paths["source"]
    good, bad = paths["good"], paths["bad"]
    for d in (good, bad):
        d.mkdir(parents=True, exist_ok=True)

    files = sorted(src.glob("*.csv"))
    if not files:
        log.error("No CSVs in %s - run the download stage first.", src)
        return pd.DataFrame()

    days = daymet_calendar(year)
    qc_days = None
    if qc_through:
        qc_days = days[days <= pd.Timestamp(qc_through)]
        log.info("QC gate scored over %d days (through %s)", len(qc_days), qc_through)
    log.info(
        "Transforming %d files onto the %d-day %d Daymet calendar ...",
        len(files),
        len(days),
        year,
    )

    recs = []

    def work(p: Path) -> dict:
        try:
            return transform_one(p, year, days, threshold, qc_days)
        except Exception as exc:  # noqa: BLE001
            return {
                "source_file": p.name,
                "climate_id": climate_id_from_filename(p) or "",
                "station_name": "",
                "lon": np.nan,
                "lat": np.nan,
                "n_rows_source": 0,
                "n_days_expected": len(days),
                "n_days_scored": len(qc_days if qc_days is not None else days),
                "missing_tmax": len(days),
                "missing_tmin": len(days),
                "missing_prcp": len(days),
                "n_quality_flagged": 0,
                "n_trace_precip": 0,
                "n_duplicate_dates": 0,
                "status": "BAD",
                "reason": f"exception: {exc}",
                "lines": None,
            }

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        for rec in pool.map(work, files):
            recs.append(rec)

    # write out; collision-safe (two provinces can share a Climate ID prefix)
    written: dict[str, str] = {}
    for rec in recs:
        if rec["lines"] is None:
            log.warning("SKIP %s: %s", rec["source_file"], rec["reason"])
            rec.pop("lines", None)
            continue
        name = f"{year}_{STN_ID_PREFIX}{rec['climate_id']}.stndata"
        if name in written:
            log.warning(
                "Duplicate output name %s (%s and %s) - suffixing",
                name,
                written[name],
                rec["source_file"],
            )
            name = f"{year}_{STN_ID_PREFIX}{rec['climate_id']}_dup.stndata"
        written[name] = rec["source_file"]
        dest = (good if rec["status"] == "GOOD" else bad) / name
        with dest.open("w") as fh:
            fh.writelines(rec["lines"])
        rec["stndata_file"] = str(dest.relative_to(paths["year"]))
        rec.pop("lines", None)

    qc = pd.DataFrame(recs)
    qc_path = paths["year"] / f"qc_report_{year}.csv"
    qc.drop(columns=[c for c in ("lines",) if c in qc], errors="ignore").to_csv(
        qc_path, index=False
    )

    n_good = int((qc["status"] == "GOOD").sum())
    log.info("Transform done: %d GOOD -> %s", n_good, paths["good"])
    log.info("                %d BAD  -> %s", len(qc) - n_good, paths["bad"])
    log.info("QC report: %s", qc_path)
    return qc


# Step 4 : stnList from the GOOD stations only
# -------------------------------------------
def stage_stnlist(paths: dict, year: int, qc: pd.DataFrame | None) -> Path:
    """
    stnList_<year>.csv, one line per GOOD station:
        longitude,latitude,elevation,CAE0<ClimateID>,STATION NAME
    Built straight from the QC table - no matched_filenames_dir2.txt step.
    """
    out_path = paths["year"] / f"stnList_{year}.csv"
    qc_path = paths["year"] / f"qc_report_{year}.csv"
    if qc is None or qc.empty:
        if not qc_path.exists():
            log.error("No QC report - run the transform stage first.")
            return out_path
        qc = pd.read_csv(qc_path, dtype={"climate_id": str})

    good = qc[qc["status"] == "GOOD"].copy()

    # elevation lookup from the ECCC master list
    stn_list_file = paths["year"] / "climate_station_list.csv"
    elev, namemap = {}, {}
    if stn_list_file.exists():
        m = pd.read_csv(stn_list_file, dtype={"Climate ID": str}, encoding="iso-8859-1")
        m["Climate ID"] = m["Climate ID"].astype(str).str.strip()
        for _, r in m.iterrows():
            cid = r["Climate ID"]
            if cid and cid not in elev:
                elev[cid] = r.get("Elevation")
                namemap[cid] = r.get("Station Name")
    else:
        log.warning("%s missing - elevations will be blank", stn_list_file.name)

    rows, no_elev = [], []
    for _, r in good.iterrows():
        cid = str(r["climate_id"]).strip()
        e = elev.get(cid)
        if pd.isna(e) if e is not None else True:
            no_elev.append(cid)
            e = ""
        name = r.get("station_name")
        if not isinstance(name, str) or not name.strip():
            name = namemap.get(cid, "")
        rows.append([r["lon"], r["lat"], e, f"{STN_ID_PREFIX}{cid}", str(name).strip()])

    rows.sort(key=lambda x: x[3])
    with out_path.open("w", newline="") as fh:
        csv.writer(fh, lineterminator="\n").writerows(rows)

    log.info("stnList: %d stations -> %s", len(rows), out_path)
    if no_elev:
        f = paths["logs"] / f"missing_elevation_{year}.txt"
        f.write_text("\n".join(no_elev) + "\n")
        log.warning("%d stations had no elevation match - see %s", len(no_elev), f)
    return out_path


# driver
# ------
def build_paths(root: Path, year: int) -> dict:
    y = root / str(year)
    p = {
        "root": root,
        "year": y,
        "urls": y / "urls",
        "source": y / "source_stn_data",
        "good": y / "transformed" / "good",
        "bad": y / "transformed" / "bad",
        "logs": y / "logs",
    }
    for d in p.values():
        d.mkdir(parents=True, exist_ok=True)
    return p


def setup_logging(logdir: Path, verbose: bool) -> None:
    logdir.mkdir(parents=True, exist_ok=True)
    fname = logdir / f"run_{dt.datetime.now():%Y%m%dT%H%M%S}.log"
    fmt = "%(asctime)s %(levelname)-7s %(message)s"
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=fmt,
        handlers=[logging.FileHandler(fname), logging.StreamHandler(sys.stdout)],
    )
    log.info("Log file: %s", fname)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="ECCC daily station observations -> Daymet station inputs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--year", type=int, required=True, help="data year, e.g. 2026")
    ap.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="output root; a <year>/ subdirectory is created under it",
    )
    ap.add_argument(
        "--stages",
        default="all",
        help="comma list of urls,download,transform,stnlist (or 'all')",
    )
    ap.add_argument(
        "--provinces",
        default=",".join(PROVINCES),
        help="comma list of province/territory codes",
    )
    ap.add_argument(
        "--missing-threshold",
        type=int,
        default=180,
        help="a station is BAD when tmax AND tmin AND prcp are each "
        "missing at least this many days",
    )
    ap.add_argument(
        "--qc-through",
        default=None,
        metavar="YYYY-MM-DD",
        help="score the good/bad gate only through this date - use for "
        "mid-season runs so not-yet-observed days are not counted "
        "as missing. Default: score the whole year.",
    )
    ap.add_argument("--jobs", type=int, default=8, help="parallel workers")
    ap.add_argument(
        "--limit", type=int, default=None, help="download at most N stations (testing)"
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="re-download / rebuild even if outputs exist",
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    stages = (
        ["urls", "download", "transform", "stnlist"]
        if args.stages == "all"
        else [s.strip() for s in args.stages.split(",") if s.strip()]
    )
    provinces = [p.strip().upper() for p in args.provinces.split(",") if p.strip()]

    paths = build_paths(args.root.resolve(), args.year)
    setup_logging(paths["logs"], args.verbose)
    log.info("Year %d | root %s | stages %s", args.year, paths["year"], stages)

    url_file = paths["urls"] / f"ALLPROV_{args.year}_URLlist.txt"
    if "urls" in stages:
        url_file = stage_urls(paths, args.year, provinces, args.force)
    if "download" in stages:
        if not url_file.exists():
            log.error("Missing %s - run the urls stage.", url_file)
            return 2
        stage_download(paths, args.year, url_file, args.jobs, args.limit, args.force)

    qc = None
    if "transform" in stages:
        qc = stage_transform(
            paths, args.year, args.missing_threshold, args.jobs, args.qc_through
        )
    if "stnlist" in stages:
        stage_stnlist(paths, args.year, qc)

    log.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
