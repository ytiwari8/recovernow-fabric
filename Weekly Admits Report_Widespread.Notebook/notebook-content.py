# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "98a8d990-4a33-4845-b492-8359b66e5259",
# META       "default_lakehouse_name": "dazos_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795",
# META       "known_lakehouses": [
# META         {
# META           "id": "98a8d990-4a33-4845-b492-8359b66e5259"
# META         },
# META         {
# META           "id": "68cab2d5-d6ec-47a8-a3ce-904a41379bf5"
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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
# Daily Admits Attribution Report + KPI Scoring (Widespread) — REVISED
# ============================================================================
# Each "# CELL ******" block maps to one Fabric notebook cell. Paste block-by-
# block, or run as-is in order.
#
# What changed vs. the original (by fix number from the review):
#   1. Facility crosswalk added; matching is facility-scoped at every tier.
#   2. Match on facility + MR#, not MR# alone (blocks the Morris->Reed bind).
#   3. Deterministic tiebreak when an MR#/name has multiple opps
#      (Admitted first, then most recent, then nearest the admit date).
#   4. Fuzzy index bug fixed (cand.index[pos], not iloc[idx]).
#   5. Name source switched from Potential_Name (51/11019 populated) to
#      Account_Name (10968/11019), and name tiers are facility-scoped.
#   6. LEGACY time parser set; Modified_Time parsed once into a real timestamp.
#   7. Scoring config: Hunter Bland in ACTIVE_REPS, 11th Ave North inpatient,
#      strategic-partner logic reads Referral Source (not the empty Contact
#      field) and matches by substring.
#   8. Self-pay + VOB-window are explicit decisions (see SELF_PAY_POINTS and the
#      VOB cell), not silent zeros.
#   +  Dropped columns: Referral Source Contact, Referring Contact,
#      Payment Method, Admitting to LOC, Internal Transfer?, Kipu Insurance,
#      Dazos Insurance, Dazos MR# (if different).
# ============================================================================


# CELL ********************
# ---- Imports + single source read (Fix 6: parse time once) -----------------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta, timezone
import io
import os
import uuid
import base64
import pandas as pd

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

TS_FMT_SPARK  = "MM-dd-yyyy h:mm a"     # Spark to_timestamp pattern
TS_FMT_PANDAS = "%m-%d-%Y %I:%M %p"     # pandas to_datetime pattern


# CELL ********************
# ---- Configuration ---------------------------------------------------------

# ---- Reporting window ------------------------------------------------------
#   - Both set            -> exact [REPORT_START, REPORT_END] range
#   - Only REPORT_START   -> from that date through yesterday
#   - Both None           -> last LOOKBACK_DAYS days
LOOKBACK_DAYS = 7
REPORT_START  = "2026-06-01"   # set to None for rolling window
REPORT_END    = "2026-06-11"   # set to None to run through yesterday
RECIPIENT     = "ytiwari@recovernow.com"
SENDER_EMAIL  = "data@recovernow.com"
SENDER_NAME   = "Recover Now Data"
FUZZY_THRESHOLD = 88           # raised from 80; facility scoping makes this safe

# Self-pay scoring decision (Fix 8). Self Pay has no VOB color, so it scores 0
# under the color tables. Set a flat credit here, or leave 0 to keep current
# behavior. This is a business call, not a bug.
SELF_PAY_POINTS = 0            # e.g. 1 to give cash admits flat credit

# VOB window decision (Fix 8). The source has no "VOB changed on" timestamp, so
# the window keys off Modified_Time, meaning any opp edited in-window for any
# reason is counted. Set True to keep that behavior (with the noise), False to
# disable VOB scoring until a real VOB-change date exists upstream.
SCORE_VOB_EVENTS = True

# Kipu location_name strings to include (exact match in the .isin filter).
WIDESPREAD_FACILITIES = [
    "Tides Edge Recovery Services",
    "11th Ave North",
    "Lotus Wellness",
    "Chattanooga Recovery Center",
    "Graceland Recovery",
    "Green Acres Recovery",
    "Green Acres Wellness",
    "Recover Now Georgia",
]

# Fix 1: Kipu location -> Dazos Treatment_Program. Both sides lowercased.
# This is the crosswalk that makes facility-scoped matching possible. The Kipu
# and Dazos spellings differ, and the mapping is many-to-one (11th Ave North
# and Tides Edge Recovery Services both map to "tides edge recovery").
# COMPLETE THIS from your full facility roster before relying on it.
FACILITY_CROSSWALK = {
    "recover now georgia":          "recover now greater atlanta",
    "tides edge recovery services": "tides edge recovery",
    "11th ave north":               "tides edge recovery",
    "green acres wellness":         "green acres wellness",
    "green acres recovery":         "green acres wellness",
    "lotus wellness":               "lotus wellness",
    "chattanooga recovery center":  "chattanooga recovery center",
    "graceland recovery":           "graceland recovery",
}

# KPI scoring config. Admit scoring keys off Kipu Location; VOB scoring keys off
# Dazos Treatment_Program. Include BOTH spellings so facility_tier() resolves
# regardless of source. (Fix 7: 11th Ave North added.)
INPATIENT_FACILITIES = {
    # Dazos Treatment_Program spellings
    "Lotus Wellness", "Green Acres Wellness", "Green Acres Recovery",
    "Tides Edge Recovery", "Recover Now Greater Atlanta", "RNGA",
    # Kipu location_name spellings
    "Tides Edge Recovery Services", "Recover Now Georgia", "11th Ave North",
}
OUTPATIENT_FACILITIES = {
    "Chattanooga Recovery Center", "Graceland Recovery",
}

# Fix 7: lowercased, matched by substring so "Legacy Healing Center" catches
# "legacy". COMPLETE THIS from your full partner roster.
STRATEGIC_PARTNERS = {
    "tulip hill", "robert alexander center", "legacy", "boca recovery",
    "twin lakes", "willingway", "flyland",
}

# Fix 7: Hunter Bland added.
ACTIVE_REPS = {
    "Dan Dixon", "Allison Kraj", "Ronnie Huff", "Christy Roberts",
    "Greg Tankersley", "Amanda Jervis", "JD Cook", "Garry Cagle",
    "Robbie Harris", "Hunter Bland",
}


def norm(s):
    """Lowercase + strip, null-safe."""
    return s.strip().lower() if isinstance(s, str) else ""


from notebookutils import mssparkutils
SENDGRID_API_KEY = mssparkutils.credentials.getSecret(
    "https://kv-kipu1.vault.azure.net/", "SENDGRID-API-KEY"
)


# CELL ********************
# ---- Resolve the reporting window ------------------------------------------

today = datetime.now(timezone.utc).date()

def _parse_d(s):
    return datetime.strptime(s, "%Y-%m-%d").date()

if REPORT_START and REPORT_END:
    week_start = _parse_d(REPORT_START)
    week_end   = _parse_d(REPORT_END)
elif REPORT_START:
    week_start = _parse_d(REPORT_START)
    week_end   = today - timedelta(days=1)
else:
    week_start = today - timedelta(days=LOOKBACK_DAYS)
    week_end   = today - timedelta(days=1)

assert week_start <= week_end, f"REPORT_START ({week_start}) must be <= REPORT_END ({week_end})"
print(f"Reporting on admits from {week_start} through {week_end} "
      f"({(week_end - week_start).days + 1} days)")


# CELL ********************
# ---- Kipu admits (Widespread only) -----------------------------------------
# Note: still dedups on mr_number to match original behavior. Blank/duplicate
# MR# handling is a known upstream issue (see review issue #3); if you want to
# stop collapsing blank-MR# admits, change the dropDuplicates key to
# ["mr_number", "first_name", "last_name", "admission_date"].

kipu = spark.read.table("kipu_lakehouse.dbo.census_clean")

kipu_admits = (
    kipu
    .filter(F.col("admission_date").between(F.lit(week_start), F.lit(week_end)))
    .filter(F.col("location_name").isin(WIDESPREAD_FACILITIES))
    .select(
        F.col("mr_number"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("admission_date"),
        F.col("location_name").alias("kipu_location"),
        F.col("program").alias("kipu_program"),
        F.col("insurance_company").alias("kipu_insurance"),
    )
    .dropDuplicates(["mr_number"])
)
print(f"Widespread admits in window: {kipu_admits.count()}")

# Fix 1: fail loud if a Kipu location has no crosswalk entry, so it never
# silently no-matches. (kipu_admits must exist first — moved here from config.)
kipu_locs = [r[0] for r in kipu_admits.select("kipu_location").distinct().collect()]
missing = [l for l in kipu_locs if norm(l) not in FACILITY_CROSSWALK]
if missing:
    print("WARNING unmapped Kipu locations, add to FACILITY_CROSSWALK:", missing)


# CELL ********************
# ---- Dazos IntakeOpportunity + Accounts ------------------------------------
# Fix 5: name source is Account_Name (Potential_Name is empty 99.5% of rows).
# Fix 6: Modified_Time parsed once into modified_ts.

ops      = spark.read.table("intake_opportunity_current")
accounts = spark.read.table("accounts_current")

ACCT_JOIN_KEY = "id"

def _nz(col):
    """Treat blanks and Dazos '--' placeholders as null."""
    c = F.trim(col.cast("string"))
    return F.when(c.isNull() | (c == "") | (c == "--"), None).otherwise(c)

accounts_select = accounts.select(
    F.col(ACCT_JOIN_KEY).alias("account_id"),
    _nz(F.col("Referral_Source")).alias("acct_referral_source"),
)

ops_select = ops.select(
    F.col("id").alias("dazos_id"),
    F.col("ParentID").alias("dazos_parent_id"),
    F.col("MR_Number").alias("dazos_mr_number"),
    # Fix 5: opportunity name from Account_Name
    F.col("Account_Name").alias("dazos_opportunity_name"),
    F.col("Sales_Stage").alias("sales_stage"),
    F.col("Treatment_Program").alias("dazos_facility"),
    F.col("VOB_Status").alias("vob_status"),
    F.col("Lead_Source").alias("lead_source"),
    F.col("Lead_Type").alias("lead_type"),
    F.col("Campaign_Source").alias("campaign_source"),
    F.col("Opener").alias("opener"),
    F.col("Closer").alias("closer"),
    F.col("BD_Rep").alias("bd_rep"),
    F.col("Modified_Time").alias("modified_time"),
    # Fix 6: real timestamp, parsed once
    F.to_timestamp(F.col("Modified_Time"), TS_FMT_SPARK).alias("modified_ts"),
    # Fix 5: clean name off Account_Name (strip trailing "-123" suffixes)
    F.lower(F.trim(F.regexp_replace(F.col("Account_Name"), r"-\d+\s*$", ""))).alias("_dazos_name_clean"),
    # Fix 1: normalized facility for crosswalk comparison
    F.lower(F.trim(F.col("Treatment_Program"))).alias("_dazos_prog_clean"),
)

dazos = (
    ops_select.join(
        accounts_select,
        ops_select["dazos_parent_id"] == accounts_select["account_id"],
        "left",
    )
    .withColumnRenamed("acct_referral_source", "referral_source")
    .drop("account_id")
)

# Pull to pandas once. All three match tiers run in pandas so the facility
# scoping and tiebreak logic is identical across them.
dazos_pdf = dazos.toPandas()
dazos_pdf["_modified_ts"] = pd.to_datetime(
    dazos_pdf["modified_time"], format=TS_FMT_PANDAS, errors="coerce"
)
dazos_pdf["_mr_clean"] = dazos_pdf["dazos_mr_number"].fillna("").astype(str).str.strip()

print(f"Dazos opps loaded: {len(dazos_pdf)}")
print(f"  with MR#:        {(dazos_pdf['_mr_clean'] != '').sum()}")
print(f"  with name:       {(dazos_pdf['_dazos_name_clean'].fillna('') != '').sum()}")


# CELL ********************
# ---- Matching (Fixes 1-5): facility-scoped, deterministic, in pandas -------

from rapidfuzz import process, fuzz

kipu_pdf = kipu_admits.toPandas()
kipu_pdf["_kipu_name_clean"] = (
    (kipu_pdf["first_name"].fillna("") + " " + kipu_pdf["last_name"].fillna(""))
    .str.strip().str.lower()
)


def pick_best(cands, admit_date):
    """Fix 3: deterministic tiebreak among candidate opps.
    Admitted first, then most recent Modified_Time, then nearest the admit date.
    """
    if len(cands) == 1:
        return cands.iloc[0]
    c = cands.copy()
    c["_is_admitted"] = (c["sales_stage"].fillna("").str.lower() == "admitted").astype(int)
    try:
        ad = pd.Timestamp(admit_date)
        c["_date_gap"] = (c["_modified_ts"] - ad).abs()
    except Exception:
        c["_date_gap"] = pd.NaT
    c = c.sort_values(
        by=["_is_admitted", "_modified_ts", "_date_gap"],
        ascending=[False, False, True],
        na_position="last",
    )
    return c.iloc[0]


def match_one(krow):
    """Return (dazos_row_or_None, method, confidence)."""
    k_mr   = str(krow["mr_number"]).strip() if pd.notna(krow["mr_number"]) else ""
    k_name = krow["_kipu_name_clean"]
    k_prog = FACILITY_CROSSWALK.get(norm(krow["kipu_location"]))
    admit_date = krow["admission_date"]

    if k_prog is None:
        return None, "No match (unmapped facility)", 0

    facility_mask = dazos_pdf["_dazos_prog_clean"] == k_prog

    # Tier 1: facility + MR#  (Fix 2 — blocks cross-facility MR# binds)
    if k_mr:
        c = dazos_pdf[facility_mask & (dazos_pdf["_mr_clean"] == k_mr)]
        if len(c):
            return pick_best(c, admit_date), "MR + Facility", 100

    # Tier 2: facility + exact name
    if k_name:
        c = dazos_pdf[facility_mask & (dazos_pdf["_dazos_name_clean"] == k_name)]
        if len(c):
            return pick_best(c, admit_date), "Name + Facility", 95

    # Tier 3: facility-scoped fuzzy name  (Fix 4 — correct index via cand.index)
    if k_name:
        cand = dazos_pdf[facility_mask & (dazos_pdf["_dazos_name_clean"].fillna("") != "")]
        if len(cand):
            names = cand["_dazos_name_clean"].tolist()
            best = process.extractOne(k_name, names, scorer=fuzz.ratio,
                                      score_cutoff=FUZZY_THRESHOLD)
            if best:
                _, score, pos = best
                return dazos_pdf.loc[cand.index[pos]], "Name fuzzy", int(score)

    return None, "No match", 0


# Columns carried from the matched Dazos opp into the report.
DAZOS_OUT_COLS = [
    "dazos_id", "dazos_parent_id", "dazos_mr_number", "dazos_opportunity_name",
    "sales_stage", "dazos_facility", "vob_status", "lead_source", "lead_type",
    "campaign_source", "opener", "closer", "bd_rep", "referral_source",
]

rows = []
for _, k in kipu_pdf.iterrows():
    d, method, conf = match_one(k)
    rec = {
        "mr_number": k["mr_number"],
        "first_name": k["first_name"],
        "last_name": k["last_name"],
        "admission_date": k["admission_date"],
        "kipu_location": k["kipu_location"],
        "kipu_program": k["kipu_program"],
        "kipu_insurance": k["kipu_insurance"],
        "match_method": method,
        "match_confidence": conf,
    }
    for col in DAZOS_OUT_COLS:
        rec[col] = (d[col] if d is not None and col in d.index else None)
    rows.append(rec)

full_pdf = pd.DataFrame(rows)

print(f"Total rows: {len(full_pdf)}")
for m in ["MR + Facility", "Name + Facility", "Name fuzzy", "No match", "No match (unmapped facility)"]:
    n = (full_pdf["match_method"] == m).sum()
    if n:
        print(f"  {m}: {n}")


# CELL ********************
# ---- Column ordering for Excel output (dropped columns removed) ------------
# Fix +: dropped Referral Source Contact, Referring Contact, Payment Method,
# Admitting to LOC, Internal Transfer?, Kipu Insurance, Dazos Insurance,
# Dazos MR# (if different).

col_order = [
    "admission_date", "mr_number", "first_name", "last_name",
    "kipu_location", "kipu_program",
    "match_method", "match_confidence",
    "dazos_opportunity_name", "dazos_facility", "sales_stage", "vob_status",
    "lead_source", "lead_type", "campaign_source", "referral_source",
    "opener", "closer", "bd_rep",
]
for c in col_order:
    if c not in full_pdf.columns:
        full_pdf[c] = None

report_pdf = full_pdf[col_order].copy()
report_pdf.columns = [
    "Admit Date", "MR #", "First Name", "Last Name",
    "Kipu Location", "Kipu Program",
    "Match Method", "Match Confidence",
    "Dazos Opportunity", "Dazos Facility", "Sales Stage", "VOB Status",
    "Lead Source", "Lead Type", "Campaign Source", "Referral Source",
    "Opener", "Closer", "BD Rep",
]
report_pdf = report_pdf.sort_values(["Admit Date", "Last Name"])
display(report_pdf.head(50))


# CELL ********************
# ---- Summaries + Excel + audit copy ---------------------------------------

with_closer = report_pdf[
    report_pdf["Closer"].notna() & (report_pdf["Closer"].astype(str).str.strip() != "")
]
closer_summary = (
    with_closer.groupby("Closer")
    .agg(Admits=("MR #", "count"))
    .reset_index()
    .sort_values("Admits", ascending=False)
)

facility_summary = (
    report_pdf.groupby("Kipu Location", dropna=False)
    .agg(Admits=("MR #", "count"))
    .reset_index()
    .sort_values("Admits", ascending=False)
)

low_confidence = report_pdf[
    (report_pdf["Match Method"].str.startswith("No match")) |
    ((report_pdf["Match Method"] == "Name fuzzy") & (report_pdf["Match Confidence"] < 90))
][[
    "Admit Date", "MR #", "First Name", "Last Name", "Kipu Location",
    "Match Method", "Match Confidence", "Dazos Opportunity"
]]

filename = f"daily_admits_attribution_{today.strftime('%Y-%m-%d')}.xlsx"
buf = io.BytesIO()
with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
    report_pdf.to_excel(writer, sheet_name="Admits Detail", index=False)
    closer_summary.to_excel(writer, sheet_name="Per Closer", index=False)
    facility_summary.to_excel(writer, sheet_name="Per Facility", index=False)
    if len(low_confidence) > 0:
        low_confidence.to_excel(writer, sheet_name="Review Needed", index=False)

buf.seek(0)
excel_bytes = buf.getvalue()

audit_path = f"/lakehouse/default/Files/admissions_reports/{filename}"
os.makedirs(os.path.dirname(audit_path), exist_ok=True)
with open(audit_path, "wb") as f:
    f.write(excel_bytes)
print(f"Saved audit copy: {audit_path}")


# CELL ********************
# ---- KPI scoring functions -------------------------------------------------

def vob_color(vob_status):
    """Normalize VOB Status to color category. Returns GY, R, P, or None."""
    if not vob_status or not isinstance(vob_status, str):
        return None
    s = vob_status.strip().lower()
    if "green" in s or "yellow" in s:
        return "GY"
    if "red" in s:
        return "R"
    if "purple" in s:
        return "P"
    if s == "approved":
        return "GY"
    return None


def facility_tier(facility):
    """Return 'inpatient', 'outpatient', or None."""
    if not facility:
        return None
    if facility in INPATIENT_FACILITIES:
        return "inpatient"
    if facility in OUTPATIENT_FACILITIES:
        return "outpatient"
    return None


def is_strategic_partner(referral_source):
    """Fix 7: read Referral Source (populated), match by substring."""
    rs = norm(referral_source)
    if not rs:
        return False
    return any(p in rs for p in STRATEGIC_PARTNERS)


def admit_category(lead_type, referral_source):
    """Priority: Strategic Partner > Alumni > BD > Standard.
    Fix 7: reads Referral Source, not the empty Referral Source Contact.
    """
    if is_strategic_partner(referral_source):
        return "strategic_partner"
    lt = norm(lead_type)
    if lt == "alumni":
        return "alumni"
    if lt == "bd":
        return "bd"
    return "standard"


# Scoring tables: (category, tier, color) -> points
ADMIT_POINTS = {
    ("standard",  "inpatient",  "GY"): 10,
    ("standard",  "inpatient",  "R"):  3,
    ("standard",  "inpatient",  "P"):  2,
    ("standard",  "outpatient", "GY"): 3,
    ("standard",  "outpatient", "R"):  1,
    ("standard",  "outpatient", "P"):  0,

    ("bd", "inpatient",  "GY"): 3,
    ("bd", "inpatient",  "R"):  1,
    ("bd", "inpatient",  "P"):  0.5,
    ("bd", "outpatient", "GY"): 2,
    ("bd", "outpatient", "R"):  0.5,
    ("bd", "outpatient", "P"):  0,

    ("alumni", "inpatient",  "GY"): 3,
    ("alumni", "inpatient",  "R"):  1,
    ("alumni", "inpatient",  "P"):  0.5,
    ("alumni", "outpatient", "GY"): 2,
    ("alumni", "outpatient", "R"):  0.5,
    ("alumni", "outpatient", "P"):  0,
}
SP_POINTS  = 2
VOB_POINTS = {"GY": 1.0, "R": 0.5, "P": 0.5}


def score_admit(facility, vob_status, lead_type, referral_source):
    tier     = facility_tier(facility)
    color    = vob_color(vob_status)
    category = admit_category(lead_type, referral_source)

    # Fix 8: self-pay handling, explicit
    if color is None and norm(vob_status) == "self pay":
        return (category, SELF_PAY_POINTS)

    if category == "strategic_partner":
        return ("strategic_partner", SP_POINTS)

    if tier is None or color is None:
        return (category, 0)

    return (category, ADMIT_POINTS.get((category, tier, color), 0))


def score_vob(vob_status):
    color = vob_color(vob_status)
    if color is None:
        return 0
    return VOB_POINTS.get(color, 0)


# CELL ********************
# ---- Build admit events from joined Kipu x Dazos data ----------------------
# Fix 7: admit_category now reads Referral Source.

admit_matches = report_pdf[
    (~report_pdf["Match Method"].str.startswith("No match")) &
    (report_pdf["Closer"].notna()) &
    (report_pdf["Closer"].astype(str).str.strip() != "")
].copy()

admit_events = []
for _, row in admit_matches.iterrows():
    closer = row.get("Closer")
    if closer not in ACTIVE_REPS:
        continue

    facility   = row.get("Kipu Location")
    vob_status = row.get("VOB Status")
    lead_type  = row.get("Lead Type")
    referral   = row.get("Referral Source")   # Fix 7: was Referral Source Contact

    category, points = score_admit(facility, vob_status, lead_type, referral)

    admit_events.append({
        "event_id": str(uuid.uuid4()),
        "event_type": "admit",
        "event_date": row["Admit Date"],
        "rep": closer,
        "patient_first_name": row.get("First Name"),
        "patient_last_name": row.get("Last Name"),
        "mr_number": row.get("MR #"),
        "facility": facility,
        "facility_tier": facility_tier(facility),
        "vob_status": vob_status,
        "vob_color": vob_color(vob_status),
        "lead_type": lead_type,
        "bd_rep": row.get("BD Rep"),
        # Column name kept to match existing Delta schema; value is now sourced
        # from Referral Source (Fix 7), not the empty Referral Source Contact.
        "referral_source_contact": referral,
        "category": category,
        "points": points,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    })

print(f"Admit events scored: {len(admit_events)}")
print(f"  Total admit points: {sum(e['points'] for e in admit_events)}")


# CELL ********************
# ---- Build VOB events from Dazos opps in window ----------------------------
# Fix 6: LEGACY parser + parse once. Fix 8: gated by SCORE_VOB_EVENTS.

vob_events = []
if SCORE_VOB_EVENTS:
    ops_for_vobs = (
        spark.read.table("intake_opportunity_current")
        .select(
            F.col("id").alias("dazos_id"),
            F.col("MR_Number").alias("mr_number"),
            F.col("Account_Name").alias("opportunity_name"),
            F.col("Treatment_Program").alias("facility"),
            F.col("VOB_Status").alias("vob_status"),
            F.col("Lead_Type").alias("lead_type"),
            F.col("Closer").alias("closer"),
            F.col("BD_Rep").alias("bd_rep"),
            F.col("Modified_Time").alias("modified_time"),
        )
        .filter(F.col("vob_status").isNotNull() & (F.trim(F.col("vob_status")) != ""))
        .withColumn("modified_date", F.to_date(F.col("modified_time"), TS_FMT_SPARK))
        .filter(F.col("modified_date").between(F.lit(week_start), F.lit(week_end)))
        .toPandas()
    )

    for _, row in ops_for_vobs.iterrows():
        closer = row.get("closer")
        if not closer or str(closer).strip() not in ACTIVE_REPS:
            continue

        points = score_vob(row["vob_status"])
        if points == 0:
            continue

        vob_events.append({
            "event_id": str(uuid.uuid4()),
            "event_type": "vob",
            "event_date": row["modified_date"],
            "rep": closer,
            "patient_first_name": None,
            "patient_last_name": None,
            "mr_number": row.get("mr_number"),
            "facility": row.get("facility"),
            "facility_tier": facility_tier(row.get("facility")),
            "vob_status": row["vob_status"],
            "vob_color": vob_color(row["vob_status"]),
            "lead_type": row.get("lead_type"),
            "bd_rep": row.get("bd_rep"),
            "referral_source_contact": None,
            "category": "vob",
            "points": points,
            "calculated_at": datetime.now(timezone.utc).isoformat(),
        })

    print(f"VOB events scored: {len(vob_events)}")
    print(f"  Total VOB points: {sum(e['points'] for e in vob_events)}")
else:
    print("VOB scoring disabled (SCORE_VOB_EVENTS=False)")


# CELL ********************
# ---- Write to admissions_kpi_events Delta table ----------------------------

from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType
)

all_events = admit_events + vob_events
print(f"Total events to write: {len(all_events)}")

if all_events:
    events_pdf = pd.DataFrame(all_events)
    events_pdf["event_date"] = pd.to_datetime(events_pdf["event_date"]).dt.date
    events_pdf["points"] = events_pdf["points"].astype(float)

    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_date", DateType(), True),
        StructField("rep", StringType(), True),
        StructField("patient_first_name", StringType(), True),
        StructField("patient_last_name", StringType(), True),
        StructField("mr_number", StringType(), True),
        StructField("facility", StringType(), True),
        StructField("facility_tier", StringType(), True),
        StructField("vob_status", StringType(), True),
        StructField("vob_color", StringType(), True),
        StructField("lead_type", StringType(), True),
        StructField("bd_rep", StringType(), True),
        StructField("referral_source_contact", StringType(), True),
        StructField("category", StringType(), True),
        StructField("points", DoubleType(), True),
        StructField("calculated_at", StringType(), True),
    ])

    events_spark = spark.createDataFrame(events_pdf, schema=schema)
    table_name = "admissions_kpi_events"

    try:
        spark.table(table_name)
        table_exists = True
    except Exception:
        table_exists = False

    if table_exists:
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.delete(
            f"event_date BETWEEN '{week_start.isoformat()}' AND '{week_end.isoformat()}'"
        )
        events_spark.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"Replaced events in {table_name} for {week_start} to {week_end} "
              f"({len(all_events)} new events)")
    else:
        events_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Created {table_name} with {len(all_events)} events")

    print("\nWeek summary by rep:")
    (events_spark.groupBy("rep")
        .agg(F.sum("points").alias("total_points"), F.count("*").alias("events"))
        .orderBy(F.desc("total_points"))
        .show(20, truncate=False))
else:
    print("No events to write")


# CELL ********************
# ---- Send daily admits email -----------------------------------------------

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Attachment, FileContent, FileName, FileType, Disposition
)

total_admits     = len(report_pdf)
matched_admits   = (~report_pdf["Match Method"].str.startswith("No match")).sum()
unmatched_admits = report_pdf["Match Method"].str.startswith("No match").sum()
match_rate       = (matched_admits / total_admits * 100) if total_admits > 0 else 0

email_body = f"""
<h2>Daily Admits Attribution Report</h2>
<p><b>Window:</b> {week_start} through {week_end}</p>
<p><b>Total Widespread admits:</b> {total_admits}</p>
<p><b>Matched to Dazos:</b> {matched_admits} ({match_rate:.1f}%)</p>
<p><b>Unmatched / Review needed:</b> {unmatched_admits}</p>

<h3>Top closers this period:</h3>
{closer_summary.head(10).to_html(index=False)}

<h3>By facility:</h3>
{facility_summary.to_html(index=False)}

<p><i>Full detail attached as Excel.</i></p>
"""

message = Mail(
    from_email=(SENDER_EMAIL, SENDER_NAME),
    to_emails=RECIPIENT,
    subject=f"Daily Admits Report — {today.strftime('%Y-%m-%d')}",
    html_content=email_body,
)

encoded = base64.b64encode(excel_bytes).decode()
attachment = Attachment(
    FileContent(encoded),
    FileName(filename),
    FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    Disposition("attachment"),
)
message.attachment = attachment

try:
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    response = sg.send(message)
    print(f"Email sent to {RECIPIENT} (status {response.status_code})")
except Exception as e:
    print(f"Email send failed: {e}")
    raise


# CELL ********************
# ---- Verification readout --------------------------------------------------

events = spark.read.table("admissions_kpi_events")

print(f"Total events: {events.count()}")
print("Date range:")
events.agg(F.min("event_date").alias("earliest"),
           F.max("event_date").alias("latest")).show()

print("By event type and category:")
(events.groupBy("event_type", "category")
    .agg(F.count("*").alias("events"), F.sum("points").alias("points"))
    .orderBy("event_type", "category").show())

print("By rep this week:")
(events.groupBy("rep")
    .agg(F.sum("points").alias("total_points"),
         F.sum(F.when(F.col("event_type") == "admit", 1).otherwise(0)).alias("admits"),
         F.sum(F.when(F.col("event_type") == "vob", 1).otherwise(0)).alias("vobs"))
    .orderBy(F.desc("total_points")).show())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
