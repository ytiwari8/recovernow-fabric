# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d697189c-e81b-4074-89e8-86c1adfee2a6",
# META       "default_lakehouse_name": "funnel_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795",
# META       "known_lakehouses": [
# META         {
# META           "id": "68cab2d5-d6ec-47a8-a3ce-904a41379bf5"
# META         },
# META         {
# META           "id": "d697189c-e81b-4074-89e8-86c1adfee2a6"
# META         },
# META         {
# META           "id": "98a8d990-4a33-4845-b492-8359b66e5259"
# META         },
# META         {
# META           "id": "7d8e32d8-17fe-4c76-bb9a-3c2893720aa6"
# META         },
# META         {
# META           "id": "12e92db1-a5e3-4866-98cd-cb0ffb3d8af4"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "9cec1e09-b29c-ada4-4fe5-a917061e3807",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "57a7170f-c860-4e3b-a410-4fe60692dcb6",
# META       "known_warehouses": [
# META         {
# META           "id": "57a7170f-c860-4e3b-a410-4fe60692dcb6",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "84ce022f-919f-4ccd-ab79-e8d836bf0eec",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "13b17ff3-d8b3-450b-9c75-ffb925dc7b9c",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "dd3c97f5-c109-48ce-8fb2-8c7d4a61014a",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "b6bd712c-d5b7-415d-b9f7-196da7ea150d",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ════════════════════════════════════════════════════════════════════════════
# Business Development — Weekly Admit Source + Points (Longbranch + EDTC)
# ----------------------------------------------------------------------------
# 1. Pulls weekly KIPU admits (admit_facts) for Longbranch + EDTC.
# 2. Name-matches each admit to Zoho (zoho_leads_current) for source/BD attribution.
# 3. Scores each admit via:
#      • Longbranch_KIPU_Mapping.xlsx (program → program_group) — 3-key then 1-key
#      • Insurance Keys + BD Points rubric from 2026_BD_Data.xlsx
#      • Era-aware rubric: ≤ 2026-04-30 → "Through Apr26", ≥ 2026-05-01 → "Starting May 26"
#      • EDTC = flat 2 pts.  NORA = flat 1.5 pts (no gender split).
#      • PHP at "Recovery Center" → Residential; PHP elsewhere → NORA.
# 4. Writes Delta funnel_lakehouse.dbo.bd_admit_source_weekly  (feeds Power BI).
# 5. Writes weekly Excel  Files/BD_Reports/BD_Admit_Source_<week>.xlsx
#       Tab 1: the table (now with Points column)
#       Tab 2: every Zoho field pulled
# 6. Maintains the cumulative workbook
#       Files/BD_Reports/2026_BD_Data.xlsx
#         • appends new MR#s into the 2026 Data EDTC / 2026 Data Longbranch tabs
#         • re-scores every row (backfill) using current rubrics
#         • rebuilds Scorecard tab with both target eras
#
# Default lakehouse: funnel_lakehouse.
# Set SHOW_DISCOVERY = True on the first run to confirm column/value names.
# ════════════════════════════════════════════════════════════════════════════
import os
from datetime import datetime, timezone, timedelta, date
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

RUN_TS = datetime.now(timezone.utc).isoformat()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

SHOW_DISCOVERY = False

# --- sources ---
T_ADMITS = "funnel_lakehouse.dbo.admit_facts"
T_ZOHO   = "zoho_lakehouse.dbo.zoho_leads_current"

# Cross-lakehouse paths to the KIPU program mapping (read in place from kipu_lakehouse).
# Tried in order — OneLake requires workspace+lakehouse to match (all IDs or all names).
KIPU_MAPPING_PATHS = [
    # all IDs (workspace id / kipu_lakehouse id)
    "abfss://fb72ebcf-98cc-4162-85c9-5d2042b8b795@onelake.dfs.fabric.microsoft.com/"
    "68cab2d5-d6ec-47a8-a3ce-904a41379bf5/Files/Mappings/Longbranch_KIPU_Mapping.xlsx",
    # all names
    "abfss://KIPU Dashboard@onelake.dfs.fabric.microsoft.com/"
    "kipu_lakehouse.Lakehouse/Files/Mappings/Longbranch_KIPU_Mapping.xlsx",
]

# BD workbook lives in funnel_lakehouse default Files area
BD_FILE = "/lakehouse/default/Files/BD_Reports/2026 BD Data.xlsx"

# --- outputs ---
T_BD_OUT = "funnel_lakehouse.dbo.bd_admit_source_weekly"
XLSX_DIR = "/lakehouse/default/Files/BD_Reports"

# --- weekly window: edit manual dates, or flip to auto Fri→Thu ---
MANUAL_DATES = True
MANUAL_START = date(2026, 5, 22)
MANUAL_END   = date(2026, 5, 28)

if MANUAL_DATES:
    WEEK_START, WEEK_END = MANUAL_START, MANUAL_END
else:
    _today = datetime.now(timezone.utc).date()
    _off   = (_today.weekday() - 3) % 7
    _off   = 7 if _off == 0 else _off
    WEEK_END   = _today - timedelta(days=_off)
    WEEK_START = WEEK_END - timedelta(days=6)

WEEK_LABEL = f"{WEEK_START.isoformat()}_to_{WEEK_END.isoformat()}"

print(f"Run: {RUN_TS}")
print(f"Window ({'MANUAL' if MANUAL_DATES else 'auto Fri→Thu'}): {WEEK_START} → {WEEK_END}   [{WEEK_LABEL}]")


# ── helpers ──────────────────────────────────────────────────────────────────
def get_ci(df, name, dtype="string"):
    m = {c.lower(): c for c in df.columns}
    return F.col(m[name.lower()]) if name.lower() in m else F.lit(None).cast(dtype)

def name_key(first_c, last_c):
    f = F.regexp_replace(F.lower(F.coalesce(first_c, F.lit(""))), r"[^a-z ]", "")
    l = F.regexp_replace(F.lower(F.coalesce(last_c,  F.lit(""))), r"[^a-z ]", "")
    k = F.trim(F.regexp_replace(F.concat_ws(" ", f, l), r"\s+", " "))
    return F.when(k == "", F.lit(None)).otherwise(k)


# ── DISCOVERY (first run only) ────────────────────────────────────────────────
if SHOW_DISCOVERY:
    _adm = spark.read.table(T_ADMITS)
    print("admit_facts columns:\n ", _adm.columns, "\n")
    _adm.groupBy("facility").count().orderBy(F.col("count").desc()).show(truncate=False)
    _z = spark.read.table(T_ZOHO)
    print("zoho_leads_current columns:\n ", _z.columns)


# ── KIPU admits (Longbranch + EDTC) inside the weekly window ──────────────────
adm_all = spark.read.table(T_ADMITS)
_loc = F.lower(F.coalesce(F.col("location_name"), F.lit("")))
_fac = F.lower(F.coalesce(F.col("facility"),      F.lit("")))
_is_longbranch = _fac.contains("longbranch")
_is_edtc       = _fac.contains("edtc") | _loc.contains("edtc") | _loc.contains("eating disorder")
adm = adm_all.filter(_is_longbranch | _is_edtc)

adm = adm.withColumn(
    "admission_dt",
    F.coalesce(
        F.col("admission_date").cast("date"),
        F.to_date("admission_date", "yyyy-MM-dd"),
        F.to_date("admission_date", "MM-dd-yyyy"),
    ),
)

admits = (
    adm.filter((F.col("admission_dt") >= F.lit(str(WEEK_START))) &
               (F.col("admission_dt") <= F.lit(str(WEEK_END))))
       .withColumn("patient_name",
                   F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name"))))
       .withColumn("adm_id",
                   F.concat_ws("|", F.col("mr_number"), F.col("admission_dt").cast("string")))
       .withColumn("k_name", name_key(F.col("first_name"), F.col("last_name")))
)

n_admits = admits.count()
print(f"Longbranch + EDTC admits in window: {n_admits}")
admits.groupBy("location_name").count().show(truncate=False)


# ── Zoho prep ─────────────────────────────────────────────────────────────────
z = spark.read.table(T_ZOHO)
z_first = get_ci(z, "first_name"); z_last = get_ci(z, "last_name"); z_full = get_ci(z, "full_name")

z2 = (
    z.withColumn("_zf", F.coalesce(z_first, F.split(z_full, " ").getItem(0)))
     .withColumn("_zl", F.coalesce(z_last,  F.element_at(F.split(z_full, " "), -1)))
     .withColumn("z_name",     name_key(F.col("_zf"), F.col("_zl")))
     .withColumn("z_created",  F.to_timestamp(get_ci(z, "created_time")))
     .withColumn("z_modified", F.to_timestamp(get_ci(z, "modified_time")))
     .drop("_zf", "_zl")
)
for c in z.columns:
    z2 = z2.withColumnRenamed(c, "zoho_" + c.lower())

print(f"Zoho leads: {z.count():,}  |  named keys: {z2.filter(F.col('z_name').isNotNull()).count():,}")


# ── name match ────────────────────────────────────────────────────────────────
joined = admits.join(z2, admits["k_name"] == z2["z_name"], "left")
w = Window.partitionBy("adm_id").orderBy(
    F.abs(F.datediff(F.col("z_created"), F.col("admission_dt"))).asc_nulls_last(),
    F.col("z_modified").desc_nulls_last(),
)
best = (
    joined.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1).drop("_rn")
          .withColumn("match_status",
                      F.when(F.col("z_name").isNotNull(), F.lit("matched")).otherwise(F.lit("unmatched")))
          .withColumn("_gap", F.abs(F.datediff(F.col("z_created"), F.col("admission_dt"))))
          .withColumn("match_confidence",
                      F.when(F.col("z_name").isNull(), F.lit("none"))
                       .when(F.col("_gap").isNull(), F.lit("name_only"))
                       .when(F.col("_gap") <= 120, F.lit("high"))
                       .otherwise(F.lit("low")))
          .drop("_gap")
)
print("match status:")
best.groupBy("match_status", "match_confidence").count().orderBy(F.col("count").desc()).show()


# ════════════════════════════════════════════════════════════════════════════
# SCORING — load lookups and score each admit
# ════════════════════════════════════════════════════════════════════════════

def _load_program_mapping():
    """Read program_mapping sheet from kipu_lakehouse/Files/Mappings/."""
    try:
        import notebookutils
    except ImportError:
        import mssparkutils as notebookutils  # type: ignore
    local = "/tmp/Longbranch_KIPU_Mapping.xlsx"
    last_err = None
    for src in KIPU_MAPPING_PATHS:
        try:
            notebookutils.fs.cp(src, "file:" + local)
            pm = pd.read_excel(local, sheet_name="program_mapping", header=1)
            pm.columns = [str(c).strip() for c in pm.columns]
            print(f"  program mapping loaded from: {src.split('@')[1][:60]}…")
            return pm[["Program Name (from Kipu)", "Facility", "Location", "Program Group"]].copy()
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Could not read Longbranch_KIPU_Mapping.xlsx from any path. Last error: {last_err}")

def _load_bd_sheet(sheet, header):
    """Load a sheet from the BD workbook."""
    df = pd.read_excel(BD_FILE, sheet_name=sheet, header=header)
    df.columns = [str(c).strip() for c in df.columns]
    return df

if not os.path.exists(BD_FILE):
    raise FileNotFoundError(
        f"BD workbook not found at {BD_FILE}. "
        "Upload 2026_BD_Data.xlsx to funnel_lakehouse/Files/BD_Reports/ and rerun.")

prog_map_pd    = _load_program_mapping()
ins_keys_pd    = _load_bd_sheet("Insurance Keys", header=3)[["Insurance","Type","Category"]].dropna(subset=["Insurance"])
rubric_pre_pd  = _load_bd_sheet("BD Points Through Apr26", header=3)[["Payor","LOC","Key","Points","Key 2"]]
rubric_post_pd = _load_bd_sheet("BD Points Starting May 26", header=3)[["Payor","LOC","Key","Points","Key 2"]]
targets_pre_pd  = _load_bd_sheet("Targets till April 2026",  header=2)[["BD Admits Tracker","Goal"]].dropna(subset=["BD Admits Tracker"])
targets_post_pd = _load_bd_sheet("Targets after May 2026",   header=1)[["BD Admits Tracker","Goal"]].dropna(subset=["BD Admits Tracker"])

# Some rubric rows have null Payor/LOC (CommercialOP, Drug CourtIOP, EDTC bookkeeping rows). Drop them for the join.
rubric_pre_pd  = rubric_pre_pd.dropna(subset=["Payor","LOC"]).copy()
rubric_post_pd = rubric_post_pd.dropna(subset=["Payor","LOC"]).copy()
# Strip whitespace in lookup columns
for df_ in (rubric_pre_pd, rubric_post_pd):
    df_["Payor"]  = df_["Payor"].astype(str).str.strip()
    df_["LOC"]    = df_["LOC"].astype(str).str.strip()
    df_["Key 2"]  = df_["Key 2"].astype(str).str.strip()
    df_["Points"] = df_["Points"].astype(float)

print(f"Loaded scoring lookups: prog_map={len(prog_map_pd)}  ins_keys={len(ins_keys_pd)}  "
      f"rubric_pre={len(rubric_pre_pd)}  rubric_post={len(rubric_post_pd)}")


# ── scoring functions (single source of truth: pandas) ────────────────────────
def _lookup_payor(insurance, ins_keys):
    if pd.isna(insurance): return "Other"
    s = str(insurance).strip().upper()
    if s in ("", "0", "NAN", "NONE"): return "Other"
    for _, r in ins_keys.iterrows():
        if str(r["Insurance"]).strip().upper() == s:
            return str(r["Category"]).strip()
    return "Other"

def _lookup_program_group(program, facility, location, prog_map):
    if pd.isna(program): return None
    p = str(program).strip().lower()
    f = str(facility or "").strip().lower()
    l = str(location or "").strip().lower()
    pn = prog_map["Program Name (from Kipu)"].fillna("").astype(str).str.strip().str.lower()
    fc = prog_map["Facility"].fillna("").astype(str).str.strip().str.lower()
    lc = prog_map["Location"].fillna("").astype(str).str.strip().str.lower()
    m3 = prog_map[(pn == p) & (fc == f) & (lc == l)]
    if len(m3) > 0:
        return str(m3["Program Group"].iloc[0]).strip()
    m1 = prog_map[pn == p]
    if len(m1) > 0:
        return str(m1["Program Group"].iloc[0]).strip()
    return None

def _resolve_program_bucket(program_group, location):
    if program_group is None or (isinstance(program_group, float) and pd.isna(program_group)):
        return "Other"
    pg = str(program_group).strip()
    loc = str(location or "").lower()
    if pg.upper() == "PHP":
        return "Residential" if "recovery center" in loc else "NORA"
    if pg in ("Male NORA", "Female NORA"):
        return "NORA"
    return pg

def score_admit(facility, program, location, insurance, admission_date,
                prog_map, ins_keys, rubric_pre, rubric_post):
    """Return (payor_category, program_bucket, points, key2_bucket)."""
    payor = _lookup_payor(insurance, ins_keys)
    pg    = _lookup_program_group(program, facility, location, prog_map)
    bucket_loc = _resolve_program_bucket(pg, location)

    # EDTC short-circuit
    if str(facility or "").strip().upper() == "EDTC":
        return payor, bucket_loc, 2.0, "EDTC"

    # NORA flat rule (regardless of payor)
    if bucket_loc == "NORA":
        return payor, "NORA", 1.5, "VA NORA M&F"

    # Era-aware rubric
    try:
        d = pd.to_datetime(admission_date, errors="coerce")
        adm_d = d.date() if pd.notna(d) else None
    except Exception:
        adm_d = None
    use_pre = (adm_d is not None) and (adm_d <= date(2026, 4, 30))
    rubric = rubric_pre if use_pre else rubric_post

    m = rubric[(rubric["Payor"].str.lower() == str(payor).lower()) &
               (rubric["LOC"].str.lower()   == str(bucket_loc).lower())]
    if len(m) > 0:
        return payor, bucket_loc, float(m["Points"].iloc[0]), str(m["Key 2"].iloc[0])
    return payor, bucket_loc, 0.0, "Other"


# ── apply scoring ─────────────────────────────────────────────────────────────
# Score on a SMALL projection only (keeps the 60+ Zoho columns at their stable
# Delta types — round-tripping them through pandas breaks the Delta schema merge).
_ZA = {
    "zoho_referral_source":                        "referral_source",
    "zoho_referring_company":                      "referring_company",
    "zoho_bd_contact_owner":                       "bd_contact",
    "zoho_bd_owner_2_only_for_double_attribution": "bd_contact_2",
    "zoho_marketing_channel":                      "marketing_channel",
}
sel = best.select(
    "adm_id", "mr_number", "patient_name", "admission_dt", "facility",
    "program", "location_name", "insurance_company", "match_status", "match_confidence",
    *[get_ci(best, k).alias(v) for k, v in _ZA.items()],
)
best_pdf = sel.toPandas()
best_pdf["admission_date"] = best_pdf["admission_dt"]   # workbook append expects this name

if len(best_pdf) > 0:
    scored = best_pdf.apply(
        lambda r: pd.Series(score_admit(
            r.get("facility"), r.get("program"), r.get("location_name"),
            r.get("insurance_company"), r.get("admission_dt"),
            prog_map_pd, ins_keys_pd, rubric_pre_pd, rubric_post_pd)),
        axis=1)
    scored.columns = ["payor_category", "program_bucket", "points", "key2_bucket"]
    best_pdf = pd.concat([best_pdf, scored], axis=1)
else:
    for c in ("payor_category", "program_bucket", "key2_bucket"):
        best_pdf[c] = pd.Series(dtype="object")
    best_pdf["points"] = pd.Series(dtype="float64")

print("scoring summary:")
if len(best_pdf):
    print(best_pdf.groupby(["key2_bucket", "program_bucket"]).agg(
        admits=("mr_number", "count"), points=("points", "sum")).to_string())

# join the 4 score columns back onto the full Spark `best` (Zoho types untouched)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
_score_schema = StructType([
    StructField("adm_id",         StringType()),
    StructField("payor_category", StringType()),
    StructField("program_bucket", StringType()),
    StructField("points",         DoubleType()),
    StructField("key2_bucket",    StringType()),
])
_sr = best_pdf[["adm_id", "payor_category", "program_bucket", "points", "key2_bucket"]].copy()
_sr["points"] = pd.to_numeric(_sr["points"], errors="coerce").fillna(0.0).astype(float)
for _c in ["adm_id", "payor_category", "program_bucket", "key2_bucket"]:
    _sr[_c] = _sr[_c].where(pd.notna(_sr[_c]), None).astype(object)
_recs = _sr.to_dict("records")
score_sdf = spark.createDataFrame(_recs, schema=_score_schema) if _recs else spark.createDataFrame([], _score_schema)

best_scored = best.join(score_sdf, on="adm_id", how="left")


# ── wide BD table (core + scoring + matched Zoho + week label) ────────────────
zoho_cols = [c for c in best_scored.columns if c.startswith("zoho_")]
bd = best_scored.select(
    # Tab 1 core (KIPU)
    F.col("patient_name"),
    F.col("location_name").alias("location"),
    F.col("program"),
    F.col("program_bucket"),
    F.col("insurance_company").alias("insurance"),
    F.col("payor_category"),
    F.col("points"),
    F.col("key2_bucket").alias("bucket"),
    # Tab 1 core (Zoho)
    get_ci(best_scored, "zoho_marketing_channel").alias("marketing_channel"),
    get_ci(best_scored, "zoho_referral_source").alias("referral_source"),
    get_ci(best_scored, "zoho_referring_company").alias("referring_company"),
    get_ci(best_scored, "zoho_bd_contact_owner").alias("bd_contact"),
    get_ci(best_scored, "zoho_bd_owner_2_only_for_double_attribution").alias("bd_contact_2"),
    # identity / keys
    F.col("mr_number"),
    F.col("casefile_id"),
    F.col("admission_dt").alias("admission_date"),
    F.col("facility"),
    F.col("match_status"),
    F.col("match_confidence"),
    # week + run metadata
    F.lit(WEEK_LABEL).alias("week_label"),
    F.lit(WEEK_START.isoformat()).cast("date").alias("week_start"),
    F.lit(WEEK_END.isoformat()).cast("date").alias("week_end"),
    F.lit(RUN_TS).cast("timestamp").alias("build_run_ts"),
    # all Zoho detail
    *[F.col(c) for c in zoho_cols],
)
print(f"bd rows: {bd.count():,}  |  zoho detail cols: {len(zoho_cols)}")


# ── Delta write (replace this week's partition; self-heal on schema drift) ────
def _write_weekly(df):
    try:
        exists = spark.catalog.tableExists(T_BD_OUT)
    except Exception:
        exists = False
    if exists:
        try:
            (df.write.format("delta").partitionBy("week_label").option("mergeSchema", "true")
               .mode("overwrite").option("replaceWhere", f"week_label = '{WEEK_LABEL}'")
               .saveAsTable(T_BD_OUT))
            return
        except Exception as e:
            print(f"  weekly replaceWhere failed ({type(e).__name__}) — rebuilding table from scratch")
            spark.sql(f"DROP TABLE IF EXISTS {T_BD_OUT}")
    (df.write.format("delta").partitionBy("week_label").option("overwriteSchema", "true")
       .mode("overwrite").saveAsTable(T_BD_OUT))

_write_weekly(bd)
print(f"Wrote {T_BD_OUT}  (week_label = {WEEK_LABEL})")


# ── weekly Excel ─────────────────────────────────────────────────────────────
pdf = bd.toPandas()
os.makedirs(XLSX_DIR, exist_ok=True)
xlsx_path = f"{XLSX_DIR}/BD_Admit_Source_{WEEK_LABEL}.xlsx"

tab1_cols = ["mr_number","patient_name","admission_date","facility",
             "location","program","program_bucket","insurance","payor_category",
             "points","bucket","marketing_channel","referral_source","referring_company",
             "bd_contact","bd_contact_2"]
tab1 = pdf[tab1_cols].copy()
tab1.columns = ["MR","Patient Name","Admission Date","Facility",
                "Location","Program","Program Bucket","Insurance","Payor Category",
                "Points","Bucket","Marketing Channel","Referral Source","Referring Company",
                "BD Contact","BD Contact 2"]

zoho_detail_cols = [c for c in pdf.columns if c.startswith("zoho_")]
tab2 = pdf[["patient_name","mr_number","admission_date","location","match_status","match_confidence"] + zoho_detail_cols].copy()
def _pretty(c):
    if c.startswith("zoho_"): return "Zoho " + c[len("zoho_"):].replace("_"," ").title()
    return c.replace("_"," ").title().replace("Mr ","MR ")
tab2.columns = [_pretty(c) for c in tab2.columns]

try:
    import xlsxwriter  # noqa: F401
    _engine = "xlsxwriter"
except Exception:
    _engine = "openpyxl"
with pd.ExcelWriter(xlsx_path, engine=_engine) as xw:
    tab1.to_excel(xw, sheet_name="BD Admits",  index=False)
    tab2.to_excel(xw, sheet_name="Zoho Detail", index=False)
print(f"Wrote {xlsx_path}  (engine={_engine})")


# ════════════════════════════════════════════════════════════════════════════
# 2026 BD workbook — append new admits, backfill scoring, rebuild Scorecard
#   pandas + xlsxwriter only (this runtime's openpyxl full-load is broken).
#   Data-tab cell formatting is not preserved; values + all lookup tabs are.
# ════════════════════════════════════════════════════════════════════════════
all_raw = pd.read_excel(BD_FILE, sheet_name=None, header=None)   # every sheet, raw grid
SCORE_HEADERS = ["Payor Category", "Program Bucket", "Points", "Bucket"]

def _find(cols, *patterns, exclude=()):
    for c in cols:
        cl = str(c).strip().lower()
        if any(x in cl for x in exclude):
            continue
        if any(p in cl for p in patterns):
            return c
    return None

def _process_tab(sheet_name, facility):
    raw = all_raw.get(sheet_name)
    if raw is None or raw.shape[0] < 1:
        print(f"  ⚠ '{sheet_name}' missing/empty — skipped")
        return None, 0
    headers = [str(h).strip() if pd.notna(h) else f"col_{i}" for i, h in enumerate(raw.iloc[0].tolist())]
    df = raw.iloc[1:].copy(); df.columns = headers; df = df.reset_index(drop=True)

    c_mr   = _find(headers, "mr")
    c_name = _find(headers, "full name", "name", exclude=("company",))
    c_date = _find(headers, "admission date")
    c_prog = _find(headers, "program", exclude=("bucket",))
    c_loc  = _find(headers, "location")
    c_ins  = _find(headers, "insurance")
    c_rs   = _find(headers, "referral source")
    c_rc   = _find(headers, "referring company")
    c_bd1  = _find(headers, "bd contact 1")
    c_bd2  = _find(headers, "bd contact 2")

    # keep only real data rows
    df = df[df[c_mr].notna() & (df[c_mr].astype(str).str.strip() != "")].reset_index(drop=True)
    existing = set(df[c_mr].astype(str).str.strip())

    # append this week's new admits for this facility (unseen MRs only)
    nw = best_pdf[best_pdf["facility"].astype(str).str.upper() == facility.upper()].copy()
    nw = nw[~nw["mr_number"].astype(str).str.strip().isin(existing)]
    added = 0
    if len(nw):
        src = {c_mr:"mr_number", c_name:"patient_name", c_date:"admission_date",
               c_prog:"program", c_loc:"location_name", c_ins:"insurance_company",
               c_rs:"referral_source", c_rc:"referring_company",
               c_bd1:"bd_contact", c_bd2:"bd_contact_2"}
        recs = []
        for _, r in nw.iterrows():
            rec = {h: None for h in headers}
            for col, key in src.items():
                if col is not None:
                    rec[col] = r.get(key)
            recs.append(rec)
        df = pd.concat([df, pd.DataFrame(recs)], ignore_index=True)
        added = len(recs)

    # score every row (backfill + new)
    if len(df):
        sc = df.apply(lambda row: pd.Series(score_admit(
                facility, row.get(c_prog), row.get(c_loc), row.get(c_ins), row.get(c_date),
                prog_map_pd, ins_keys_pd, rubric_pre_pd, rubric_post_pd)), axis=1)
        sc.columns = SCORE_HEADERS
        for h in SCORE_HEADERS:
            df[h] = sc[h].values
    else:
        for h in SCORE_HEADERS:
            df[h] = pd.Series(dtype="object")

    base_cols = [h for h in headers if h not in SCORE_HEADERS]
    df = df[base_cols + SCORE_HEADERS].copy()
    df["__facility"] = facility
    print(f"  {sheet_name}: +{added} new rows, {len(df)} rows scored")
    return df, added

print("\n── updating 2026 BD workbook ──")
edtc_df, added_e = _process_tab("2026 Data EDTC",       "EDTC")
lb_df,   added_l = _process_tab("2026 Data Longbranch", "Longbranch")


# ── combined view for Scorecard + fact table ─────────────────────────────────
def _norm_cols(df):
    rename = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if   lc.startswith("mr"):                       rename[c] = "MR"
        elif "full name" in lc or lc == "name":         rename[c] = "Full Name"
        elif "admission date" in lc:                    rename[c] = "Admission Date"
        elif "program bucket" in lc:                    rename[c] = "Program Bucket"
        elif "program" in lc and "bucket" not in lc:    rename[c] = "Program"
        elif "location" in lc:                          rename[c] = "Location"
        elif "payor category" in lc:                    rename[c] = "Payor Category"
        elif "insurance" in lc:                         rename[c] = "Insurance"
        elif lc == "points":                            rename[c] = "Points"
        elif lc == "bucket":                            rename[c] = "Bucket"
        elif "referral source" in lc:                   rename[c] = "Referral Source"
        elif "referring company" in lc:                 rename[c] = "Referring Company"
        elif "bd contact 1" in lc:                      rename[c] = "BD Contact 1"
        elif "bd contact 2" in lc:                      rename[c] = "BD Contact 2"
    return df.rename(columns=rename)

parts = [p for p in (edtc_df, lb_df) if p is not None]
combined = pd.concat([_norm_cols(p) for p in parts], ignore_index=True) if parts else pd.DataFrame()
if len(combined):
    combined = combined.dropna(subset=["MR"])
    combined = combined[combined["MR"].astype(str).str.strip() != ""]
    combined["adm_dt"] = pd.to_datetime(combined["Admission Date"], errors="coerce")
    combined["Points"] = pd.to_numeric(combined["Points"], errors="coerce").fillna(0.0)
    combined["Bucket"] = combined["Bucket"].astype(str).str.strip()

pre  = combined[combined["adm_dt"].dt.date <= date(2026, 4, 30)] if len(combined) else combined
post = combined[combined["adm_dt"].dt.date >= date(2026, 5, 1)]  if len(combined) else combined

def _bucket_unit_value(rubric, bucket_name):
    m = rubric[rubric["Key 2"].astype(str).str.strip() == bucket_name]
    return float(m["Points"].iloc[0]) if len(m) else 0.0

def _scorecard_rows(actual_df, targets_df, rubric_df):
    rows = []; T_adm = T_admT = T_pts = T_ptsT = 0.0
    tgt_buckets = targets_df["BD Admits Tracker"].astype(str).str.strip().tolist()
    for _, t in targets_df.iterrows():
        bucket = str(t["BD Admits Tracker"]).strip()
        if bucket.lower() in ("total", "", "nan"): continue
        try:    goal = int(t["Goal"])
        except: goal = 0
        unit = 2.0 if bucket == "EDTC" else _bucket_unit_value(rubric_df, bucket)
        pt_goal = round(goal * unit, 2)
        sub = actual_df[actual_df["Bucket"] == bucket] if len(actual_df) else actual_df
        a_adm = int(len(sub)); a_pts = round(float(sub["Points"].sum()), 2) if len(sub) else 0.0
        rows.append([bucket, a_adm, goal, f"{(a_adm/goal*100):.0f}%" if goal else "—",
                     a_pts, pt_goal, f"{(a_pts/pt_goal*100):.0f}%" if pt_goal else "—"])
        T_adm += a_adm; T_admT += goal; T_pts += a_pts; T_ptsT += pt_goal
    if len(actual_df):
        other = actual_df[~actual_df["Bucket"].isin(tgt_buckets)]
        if len(other):
            rows.append(["Other", int(len(other)), "—", "—", round(float(other["Points"].sum()), 2), "—", "—"])
            T_adm += len(other); T_pts += float(other["Points"].sum())
    rows.append(["TOTAL", int(T_adm), int(T_admT) if T_admT else "—",
                 f"{(T_adm/T_admT*100):.0f}%" if T_admT else "—",
                 round(T_pts, 2), round(T_ptsT, 2) if T_ptsT else "—",
                 f"{(T_pts/T_ptsT*100):.0f}%" if T_ptsT else "—"])
    return rows

pre_rows  = _scorecard_rows(pre,  targets_pre_pd,  rubric_pre_pd)
post_rows = _scorecard_rows(post, targets_post_pd, rubric_post_pd)

sc_cols = ["Bucket", "Admits", "Admit Target", "Admit %", "Points", "Point Target", "Point %"]
sc_grid = []
sc_grid.append(["Through April 2026   (admission_date <= 2026-04-30)"] + [None]*6)
sc_grid.append(sc_cols)
sc_grid += pre_rows
sc_grid.append([None]*7)
sc_grid.append(["From May 2026   (admission_date >= 2026-05-01)"] + [None]*6)
sc_grid.append(sc_cols)
sc_grid += post_rows
scorecard_df = pd.DataFrame(sc_grid)


# ── rewrite the workbook (Scorecard + data tabs + preserved lookup tabs) ──────
DATA_TABS = {"2026 Data EDTC": edtc_df, "2026 Data Longbranch": lb_df}
with pd.ExcelWriter(BD_FILE, engine="xlsxwriter") as xw:
    scorecard_df.to_excel(xw, sheet_name="Scorecard", index=False, header=False)
    for name, df in DATA_TABS.items():
        if df is None:
            all_raw[name].to_excel(xw, sheet_name=name, index=False, header=False)
        else:
            df.drop(columns=["__facility"], errors="ignore").to_excel(xw, sheet_name=name, index=False)
    for name, raw in all_raw.items():
        if name in DATA_TABS or name == "Scorecard":
            continue
        raw.to_excel(xw, sheet_name=name, index=False, header=False)

print(f"Wrote {BD_FILE}")
print(f"  EDTC: +{added_e} new rows | Longbranch: +{added_l} new rows | Scorecard rebuilt")

# ════════════════════════════════════════════════════════════════════════════
# SEMANTIC-MODEL TABLES — full 2026 fact (from workbook) + targets dimension
#   bd_admit_source : one row per admit, ALL of 2026, scored  ← Power BI fact
#   bd_targets      : era × bucket admit/point targets        ← Power BI dim
# ════════════════════════════════════════════════════════════════════════════
T_FACT    = "funnel_lakehouse.dbo.bd_admit_source"
T_TARGETS = "funnel_lakehouse.dbo.bd_targets"

def _norm_more(df):
    rename = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if   "referral source"   in lc: rename[c] = "Referral Source"
        elif "referring company" in lc: rename[c] = "Referring Company"
        elif "bd contact 1"      in lc: rename[c] = "BD Contact 1"
        elif "bd contact 2"      in lc: rename[c] = "BD Contact 2"
    return df.rename(columns=rename)

fact_src = _norm_more(combined).copy()
_adm_dt  = pd.to_datetime(fact_src.get("Admission Date"), errors="coerce")

fact_pdf = pd.DataFrame({
    "mr_number":         fact_src["MR"].astype(str).str.strip(),
    "patient_name":      fact_src.get("Full Name"),
    "admission_date":    _adm_dt.dt.strftime("%Y-%m-%d"),
    "facility":          fact_src.get("__facility"),
    "location":          fact_src.get("Location"),
    "program":           fact_src.get("Program"),
    "program_bucket":    fact_src.get("Program Bucket"),
    "insurance":         fact_src.get("Insurance"),
    "payor_category":    fact_src.get("Payor Category"),
    "points":            pd.to_numeric(fact_src.get("Points"), errors="coerce").fillna(0.0),
    "bucket":            fact_src.get("Bucket"),
    "referral_source":   fact_src.get("Referral Source"),
    "referring_company": fact_src.get("Referring Company"),
    "bd_contact":        fact_src.get("BD Contact 1"),
    "bd_contact_2":      fact_src.get("BD Contact 2"),
})
fact_pdf["period_era"] = _adm_dt.dt.date.apply(
    lambda d: "Through Apr 2026" if (pd.notna(d) and d <= date(2026, 4, 30)) else "From May 2026")
fact_pdf["build_run_ts"] = RUN_TS

# keep only real MR rows; NaN→None so Spark infers cleanly
fact_pdf = fact_pdf[fact_pdf["mr_number"].str.lower().isin(["", "nan", "none"]) == False].copy()
for c in fact_pdf.columns:
    if fact_pdf[c].dtype == object:
        fact_pdf[c] = fact_pdf[c].where(pd.notna(fact_pdf[c]), None)

fact_sdf = (spark.createDataFrame(fact_pdf)
                 .withColumn("admission_date", F.to_date("admission_date"))
                 .withColumn("week_ending",
                             F.expr("date_add(admission_date, pmod(5 - dayofweek(admission_date), 7))"))
                 .withColumn("build_run_ts", F.col("build_run_ts").cast("timestamp")))
(fact_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_FACT))
print(f"Wrote {T_FACT}: {fact_sdf.count()} admits (full 2026)")

# targets dimension (admit + derived point targets) per era
tgt_rows = []
for era, tdf, rdf in [("Through Apr 2026", targets_pre_pd,  rubric_pre_pd),
                      ("From May 2026",    targets_post_pd, rubric_post_pd)]:
    for _, t in tdf.iterrows():
        bucket = str(t["BD Admits Tracker"]).strip()
        if bucket.lower() in ("total", "", "nan"):
            continue
        try:
            goal = int(t["Goal"])
        except Exception:
            continue
        unit = 2.0 if bucket == "EDTC" else _bucket_unit_value(rdf, bucket)
        tgt_rows.append((era, bucket, goal, float(unit), round(goal * unit, 2)))

tgt_pdf = pd.DataFrame(tgt_rows, columns=["era", "bucket", "admit_target", "point_unit", "point_target"])
tgt_sdf = spark.createDataFrame(tgt_pdf)
(tgt_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_TARGETS))
print(f"Wrote {T_TARGETS}: {tgt_sdf.count()} target rows")


# ════════════════════════════════════════════════════════════════════════════
# VALIDATION + run status + email
#   Flags the two source-fixable problems: an insurance string missing from the
#   Insurance Keys tab, or a program missing from the KIPU mapping. Both silently
#   fall to Other/0 points, so we surface them here for a fix-and-rerun loop.
# ════════════════════════════════════════════════════════════════════════════
from pyspark.sql.types import IntegerType, TimestampType

# --- email settings (SendGrid, same path as the CTM daily brief) ---
SEND_EMAIL       = True
KV_URL           = "https://kv-kipu1.vault.azure.net/"
SENDGRID_SECRET  = "SENDGRID-API-KEY"
FROM_EMAIL       = "data@recovernow.com"
TO_EMAIL         = "ytiwari@recovernow.com"

_known_ins = set(ins_keys_pd["Insurance"].astype(str).str.strip().str.upper())
_known_prog = set(prog_map_pd["Program Name (from Kipu)"].fillna("").astype(str).str.strip().str.lower())

def _scan_unmapped(df):
    ins_bad, prog_bad = set(), set()
    if not len(df):
        return [], []
    for v in df.get("Insurance", pd.Series(dtype=object)).dropna().astype(str):
        s = v.strip().upper()
        if s in ("", "0", "NAN", "NONE"):
            continue
        if s not in _known_ins:
            ins_bad.add(v.strip())
    for v in df.get("Program", pd.Series(dtype=object)).dropna().astype(str):
        if v.strip() == "":
            continue
        if v.strip().lower() not in _known_prog:
            prog_bad.add(v.strip())
    return sorted(ins_bad), sorted(prog_bad)

unmapped_ins, unmapped_prog = _scan_unmapped(combined)

n_total      = int(len(combined))
points_total = round(float(combined["Points"].sum()), 2) if len(combined) else 0.0
matched_wk   = int((best_pdf["match_status"] == "matched").sum()) if len(best_pdf) else 0
run_status   = "warnings" if (unmapped_ins or unmapped_prog) else "ok"

# --- printed report (always visible on a manual run) ---
print("\n" + "#"*64)
print(f"# BD RUN — {WEEK_LABEL} — {'⚠ NEEDS ATTENTION' if run_status!='ok' else '✅ OK'}")
print("#"*64)
print(f"  new admits this week : {n_admits}   (Zoho-matched: {matched_wk})")
print(f"  total 2026 admits    : {n_total}")
print(f"  total points         : {points_total}")
if unmapped_ins:
    print(f"\n  ⚠ {len(unmapped_ins)} INSURANCE name(s) not in 'Insurance Keys' tab → scored as Other/0:")
    for x in unmapped_ins:
        print(f"      - {x}")
    print("    FIX: add them to the Insurance Keys tab (with Category), then rerun.")
if unmapped_prog:
    print(f"\n  ⚠ {len(unmapped_prog)} PROGRAM(s) not in Longbranch_KIPU_Mapping → scored as Other/0:")
    for x in unmapped_prog:
        print(f"      - {x}")
    print("    FIX: add them to the program_mapping sheet, then rerun.")
if run_status == "ok":
    print("\n  No mapping gaps — every admit scored against the rubric.")
print("#"*64)

# --- bd_run_status table (one appended row per run) ---
T_STATUS = "funnel_lakehouse.dbo.bd_run_status"
status_schema = StructType([
    StructField("run_ts",                 StringType()),
    StructField("week_label",             StringType()),
    StructField("status",                 StringType()),
    StructField("admits_this_week",       IntegerType()),
    StructField("admits_total",           IntegerType()),
    StructField("points_total",           DoubleType()),
    StructField("zoho_matched_this_week", IntegerType()),
    StructField("n_unmapped_insurances",  IntegerType()),
    StructField("n_unmapped_programs",    IntegerType()),
    StructField("unmapped_insurances",    StringType()),
    StructField("unmapped_programs",      StringType()),
])
status_row = [(RUN_TS, WEEK_LABEL, run_status, int(n_admits), n_total, points_total, matched_wk,
               len(unmapped_ins), len(unmapped_prog),
               "; ".join(unmapped_ins) or None, "; ".join(unmapped_prog) or None)]
(spark.createDataFrame(status_row, schema=status_schema)
      .withColumn("run_ts", F.to_timestamp("run_ts"))
      .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(T_STATUS))
print(f"Wrote {T_STATUS} (status = {run_status})")

# --- email (SendGrid) ---
def _send_email(subject, body_html):
    if not SEND_EMAIL:
        print("  email disabled (SEND_EMAIL=False)")
        return
    try:
        import notebookutils, requests
        sg = notebookutils.credentials.getSecret(KV_URL, SENDGRID_SECRET)
        r = requests.post("https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {sg}", "Content-Type": "application/json"},
            json={"personalizations": [{"to": [{"email": TO_EMAIL}]}],
                  "from": {"email": FROM_EMAIL, "name": "BD Weekly Report"},
                  "subject": subject,
                  "content": [{"type": "text/html", "value": body_html}]}, timeout=20)
        print("  SendGrid:", "sent" if r.status_code in (200, 202) else f"FAILED {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"  email send failed: {e}")

_subj = f"BD Report {WEEK_LABEL} — {'NEEDS ATTENTION' if run_status!='ok' else 'OK'}"
_b = [f"<h3>BD Weekly Run — {WEEK_LABEL}</h3>",
      f"<b>Status:</b> {'&#9888; NEEDS ATTENTION' if run_status!='ok' else '&#9989; OK'}<br>",
      f"<b>New admits this week:</b> {n_admits} (Zoho-matched: {matched_wk})<br>",
      f"<b>Total 2026 admits:</b> {n_total}<br>",
      f"<b>Total points:</b> {points_total}<br><br>"]
if unmapped_ins:
    _b.append("<b style='color:#b00'>Insurance names missing from 'Insurance Keys' (add + rerun):</b><ul>")
    _b += [f"<li>{x}</li>" for x in unmapped_ins]; _b.append("</ul>")
if unmapped_prog:
    _b.append("<b style='color:#b00'>Programs missing from KIPU mapping (add + rerun):</b><ul>")
    _b += [f"<li>{x}</li>" for x in unmapped_prog]; _b.append("</ul>")
if run_status == "ok":
    _b.append("<p>No mapping gaps — every admit scored against the rubric.</p>")
_send_email(_subj, "".join(_b))


# ── sanity summary ───────────────────────────────────────────────────────────
fact = spark.read.table(T_FACT)
print("="*64)
print(f"bd_admit_source (full 2026): {fact.count()} admits  |  "
      f"total points: {fact.agg(F.sum('points')).first()[0] or 0}")
print("="*64)
print("Admits + points by era × bucket:")
fact.groupBy("period_era", "bucket").agg(
        F.count("*").alias("admits"), F.round(F.sum("points"), 2).alias("points")) \
    .orderBy("period_era", F.col("points").desc()).show(40, truncate=False)
print("Targets:")
spark.read.table(T_TARGETS).orderBy("era", F.col("admit_target").desc()).show(40, truncate=False)
print(f"This week ({WEEK_LABEL}): {n_admits} new admits processed → appended to workbook + fact rebuilt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ════════════════════════════════════════════════════════════════════════════
# Refresh bd_admit_source FROM both:
#   - Historical: 2026 Data EDTC + 2026 Data Longbranch tabs in 2026 BD Data.xlsx
#   - Going-forward: BD_Admit_Source_<week>.xlsx weekly snapshots
#
# Both live in Files/BD_Reports/. Same dedupe key (MR, Admission Date).
# Weekly always wins on collision (curation surface beats historical). Within
# weekly files, newest mtime wins. Trusts file values as-is, no re-scoring.
# ════════════════════════════════════════════════════════════════════════════
import os, glob
from datetime import datetime, timezone, date
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

RUN_TS = datetime.now(timezone.utc).isoformat()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# ── config ───────────────────────────────────────────────────────────────────
XLSX_DIR        = "/lakehouse/default/Files/BD_Reports"
HISTORICAL_FILE = "/lakehouse/default/Files/BD_Reports/2026 BD Data.xlsx"
# Tab name in 2026 BD Data.xlsx -> default facility for that tab
HISTORICAL_TABS = {"2026 Data EDTC": "EDTC", "2026 Data Longbranch": "Longbranch"}

T_FACT   = "funnel_lakehouse.dbo.bd_admit_source"
T_STATUS = "funnel_lakehouse.dbo.bd_run_status"

REFRESH_SEMANTIC_MODEL   = False
SEMANTIC_MODEL_NAME      = "Recover Now BD"
SEMANTIC_MODEL_WORKSPACE = None

print(f"Refresh run: {RUN_TS}")


# ── helpers ──────────────────────────────────────────────────────────────────
def _norm_key(s):
    """Lowercase + collapse internal whitespace, so 'Insurance 1   Insurance Company'
    matches 'Insurance 1 Insurance Company'."""
    return " ".join(str(s).strip().lower().split())

def _build_col_lookup(df_raw):
    return {_norm_key(c): c for c in df_raw.columns}

def _col(cols_lower, *names):
    for n in names:
        c = cols_lower.get(_norm_key(n))
        if c:
            return c
    return None

def normalize_frame(df_raw, source_file, source_mtime, source_type, default_facility=None):
    """Convert any input frame (weekly or historical) to the fact schema."""
    cl = _build_col_lookup(df_raw)
    def G(*names):
        c = _col(cl, *names)
        return df_raw[c] if c else pd.Series([None] * len(df_raw))

    facility = G("Facility")
    if default_facility is not None:
        facility = facility.fillna(default_facility)

    return pd.DataFrame({
        "mr_number":         G("MR", "MR Number").astype(str).str.strip(),
        "patient_name":      G("Patient Name", "Full Name", "Name"),
        "admission_date":    pd.to_datetime(G("Admission Date"), errors="coerce").dt.normalize(),
        "facility":          facility,
        "location":          G("Location", "Location 1"),
        "program":           G("Program", "Program 1"),
        "program_bucket":    G("Program Bucket"),
        "insurance":         G("Insurance", "Insurance 1 Insurance Company"),
        "payor_category":    G("Payor Category"),
        "points":            pd.to_numeric(G("Points"), errors="coerce").fillna(0.0),
        "bucket":            G("Bucket"),
        "marketing_channel": G("Marketing Channel"),
        "referral_source":   G("Referral Source"),
        "referring_company": G("Referring Company"),
        "bd_contact":        G("BD Contact", "BD Contact 1"),
        "bd_contact_2":      G("BD Contact 2"),
        "__source_file":     source_file,
        "__source_mtime":    source_mtime,
        "__source_type":     source_type,
    })


# ── read weekly files ─────────────────────────────────────────────────────────
weekly_files = sorted(
    glob.glob(os.path.join(XLSX_DIR, "BD_Admit_Source_*.xlsx")),
    key=lambda p: os.path.getmtime(p),
    reverse=True,
)
weekly_frames = []
skipped = []

print(f"\nWeekly files: {len(weekly_files)}")
for fp in weekly_files:
    try:
        df_raw = pd.read_excel(fp, sheet_name="BD Admits")
    except Exception as e:
        print(f"  ⚠ failed to read {os.path.basename(fp)}: {e}")
        skipped.append((os.path.basename(fp), str(e)))
        continue

    cl = _build_col_lookup(df_raw)
    needed = ["MR", "Admission Date", "Facility"]
    missing = [r for r in needed if _col(cl, r) is None]
    if missing:
        print(f"  ⚠ {os.path.basename(fp)} missing {missing}, skipped")
        skipped.append((os.path.basename(fp), f"missing {missing}"))
        continue

    mtime = datetime.fromtimestamp(os.path.getmtime(fp), timezone.utc)
    frame = normalize_frame(df_raw, os.path.basename(fp), mtime, "weekly")
    weekly_frames.append(frame)
    print(f"  ✓ {os.path.basename(fp)}: {len(frame)} rows")


# ── read historical workbook ──────────────────────────────────────────────────
hist_frames = []
if os.path.exists(HISTORICAL_FILE):
    hist_mtime = datetime.fromtimestamp(os.path.getmtime(HISTORICAL_FILE), timezone.utc)
    print(f"\nHistorical: {os.path.basename(HISTORICAL_FILE)}")
    for tab, default_facility in HISTORICAL_TABS.items():
        try:
            df_raw = pd.read_excel(HISTORICAL_FILE, sheet_name=tab)
        except Exception as e:
            print(f"  ⚠ tab '{tab}' failed: {e}")
            skipped.append((f"{os.path.basename(HISTORICAL_FILE)} :: {tab}", str(e)))
            continue
        cl = _build_col_lookup(df_raw)
        missing = [r for r in ["MR", "Admission Date"] if _col(cl, r) is None]
        if missing:
            print(f"  ⚠ tab '{tab}' missing {missing}, skipped")
            skipped.append((f"{os.path.basename(HISTORICAL_FILE)} :: {tab}", f"missing {missing}"))
            continue
        frame = normalize_frame(df_raw,
                                f"2026 BD Data.xlsx :: {tab}",
                                hist_mtime, "historical",
                                default_facility=default_facility)
        hist_frames.append(frame)
        print(f"  ✓ {tab}: {len(frame)} rows  (default facility: {default_facility})")
else:
    print(f"\nHistorical file not found at {HISTORICAL_FILE}, skipping historical load")


# ── union and dedupe ──────────────────────────────────────────────────────────
all_frames = weekly_frames + hist_frames
if not all_frames:
    raise RuntimeError("No data loaded. Nothing written.")

df = pd.concat(all_frames, ignore_index=True)
print(f"\nUnion total: {len(df)} rows  "
      f"({sum(len(f) for f in weekly_frames)} weekly + "
      f"{sum(len(f) for f in hist_frames)} historical)")

before = len(df)
df = df[~df["mr_number"].str.lower().isin(["", "nan", "none"])]
df = df.dropna(subset=["admission_date"])
print(f"After dropping rows missing MR or admission date: {len(df)} of {before}")

# Dedupe key: (MR, Admission Date). Weekly wins over historical; within weeklies
# newest mtime wins.
df["__pref"] = df["__source_type"].map({"weekly": 0, "historical": 1})
df = df.sort_values(["__pref", "__source_mtime"], ascending=[True, False])

dup_mask = df.duplicated(subset=["mr_number", "admission_date"], keep=False)
if dup_mask.any():
    n_groups = df[dup_mask].groupby(["mr_number", "admission_date"]).ngroups
    cross = df[dup_mask].groupby(["mr_number", "admission_date"])["__source_type"].nunique()
    cross_n = int((cross > 1).sum())
    print(f"\n  {n_groups} (MR, Admission Date) collision(s), {cross_n} cross-source. "
          f"Weekly beats historical, then newest mtime.")

df = df.drop_duplicates(subset=["mr_number", "admission_date"], keep="first")
print(f"After dedupe: {len(df)} rows")
print(f"  by source: {df['__source_type'].value_counts().to_dict()}")


# ── derive period_era + clean up ──────────────────────────────────────────────
df["period_era"] = df["admission_date"].dt.date.apply(
    lambda d: "Through Apr 2026" if (pd.notna(d) and d <= date(2026, 4, 30)) else "From May 2026")
df["build_run_ts"]   = RUN_TS
df["admission_date"] = df["admission_date"].dt.strftime("%Y-%m-%d")

df = df.drop(columns=["__source_file", "__source_mtime", "__source_type", "__pref"])
for c in df.columns:
    if df[c].dtype == object:
        df[c] = df[c].where(pd.notna(df[c]), None)


# ── write fact ────────────────────────────────────────────────────────────────
fact_sdf = (spark.createDataFrame(df)
            .withColumn("admission_date", F.to_date("admission_date"))
            .withColumn("week_ending",
                        F.expr("date_add(admission_date, pmod(5 - dayofweek(admission_date), 7))"))
            .withColumn("build_run_ts", F.col("build_run_ts").cast("timestamp")))
(fact_sdf.write.format("delta")
        .mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(T_FACT))
print(f"\nWrote {T_FACT}: {fact_sdf.count()} admits")


# ── status log ────────────────────────────────────────────────────────────────
status_schema = StructType([
    StructField("run_ts",                 StringType()),
    StructField("week_label",             StringType()),
    StructField("status",                 StringType()),
    StructField("admits_this_week",       IntegerType()),
    StructField("admits_total",           IntegerType()),
    StructField("points_total",           DoubleType()),
    StructField("zoho_matched_this_week", IntegerType()),
    StructField("n_unmapped_insurances",  IntegerType()),
    StructField("n_unmapped_programs",    IntegerType()),
    StructField("unmapped_insurances",    StringType()),
    StructField("unmapped_programs",      StringType()),
])
points_total = round(float(df["points"].sum()), 2)
status_row = [(RUN_TS, f"refresh_{date.today().isoformat()}",
               "refresh_ok" if not skipped else "refresh_warnings",
               0, int(len(df)), points_total, 0, 0, 0,
               None,
               "; ".join(f"{n}: {r}" for n, r in skipped) or None)]
(spark.createDataFrame(status_row, schema=status_schema)
      .withColumn("run_ts", F.to_timestamp("run_ts"))
      .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(T_STATUS))
print(f"Logged refresh to {T_STATUS}")


# ── optional: refresh Power BI semantic model ─────────────────────────────────
if REFRESH_SEMANTIC_MODEL:
    try:
        import sempy.fabric as fabric
        fabric.refresh_dataset(dataset=SEMANTIC_MODEL_NAME,
                               workspace=SEMANTIC_MODEL_WORKSPACE,
                               refresh_type="full")
        print(f"Triggered refresh of '{SEMANTIC_MODEL_NAME}'")
    except Exception as e:
        print(f"  semantic model refresh failed: {e}")


# ── sanity summary ────────────────────────────────────────────────────────────
fact = spark.read.table(T_FACT)
print("="*64)
print(f"bd_admit_source: {fact.count()} admits | total points: {fact.agg(F.sum('points')).first()[0] or 0}")
print("\nAdmits + points by era × bucket:")
fact.groupBy("period_era", "bucket").agg(
        F.count("*").alias("admits"),
        F.round(F.sum("points"), 2).alias("points")) \
    .orderBy("period_era", F.col("points").desc()).show(40, truncate=False)

if skipped:
    print(f"\n⚠ Skipped {len(skipped)} source(s):")
    for n, r in skipped:
        print(f"   {n}: {r}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
