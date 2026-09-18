
# Instructions for `DAAC_Station2Daymet.py`

by Bharat Sharma based on the original scripts by Michele Thornton

========================================================================= <br>
Single-script workflow: Environment and Climate Change Canada (ECCC) daily
climate station observations  ->  Daymet station-input format.

Replaces the 2025 multi-script workflow:
```
    ca_https_urllists_2025_prov.sh   (build per-province URL lists)
    download_2025_stndata.sh         (wget loop)
    processCAstns.py                 (CSV -> .stndata)
    id_files_for_stnList_writefile.py(list transformed files)
    create_stnList.py                (build stnList_<year>.csv)
plus the manual `cat`, `mv`, and BAD/good sorting steps.
```

Original:  Michele Thornton, ORNL
This version is Consolidated:    single-entry pipeline, resumable, year-agnostic.

-------------------------------------------------------------------------
USAGE
-------------------------------------------------------------------------
  **everything, current data year, into `./<year>/`** <br>
  `python DAAC_Station2Daymet.py --year 2025 --root ./main_dir`

  **only re-run the transform + stnList (source CSVs already downloaded)** <br>
  `python DAAC_Station2Daymet.py --year 2025 --root ./main_dir --stages transform,stnlist`

  **quick smoke test: 5 stations from two provinces** <br>
  `python DAAC_Station2Daymet.py --year 2025 --root ./main_dir --provinces AB,BC --limit 5`

-------------------------------------------------------------------------
**OUTPUT TREE  (`main_dir/<year>/`)**
-------------------------------------------------------------------------
```
  urls/<PROV>_<year>_URLlist.txt      per-province lists (kept for reference)
  urls/ALLPROV_<year>_URLlist.txt     concatenated list
  climate_station_list.csv            ECCC master list (elevation source)
  source_stn_data/*.csv               downloaded daily station CSVs
  transformed/good/*.stndata          stations that pass the QC gate
  transformed/bad/*.stndata           stations that fail (kept, not deleted)
  stnList_<year>.csv                  lon,lat,elev,CAE0<id>,name  (GOOD only)
  qc_report_<year>.csv                per-station QC audit trail
  logs/run_<timestamp>.log            full run log
```
=========================================================================
