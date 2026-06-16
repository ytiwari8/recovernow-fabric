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
# META           "id": "98a8d990-4a33-4845-b492-8359b66e5259"
# META         },
# META         {
# META           "id": "68cab2d5-d6ec-47a8-a3ce-904a41379bf5"
# META         },
# META         {
# META           "id": "12e92db1-a5e3-4866-98cd-cb0ffb3d8af4"
# META         },
# META         {
# META           "id": "d697189c-e81b-4074-89e8-86c1adfee2a6"
# META         },
# META         {
# META           "id": "7d8e32d8-17fe-4c76-bb9a-3c2893720aa6"
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
# SALES FUNNEL — Cell 1: Setup
# %pip MUST be first: it restarts the kernel and wipes anything defined above it.
# ════════════════════════════════════════════════════════════════════════════

# --- imports (everything all sections need) ---
import notebookutils                       # NOT auto-injected in Fabric
import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import gspread
from google.oauth2.service_account import Credentials

# --- spark config ---
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# --- run timestamp (shared by all sections) ---
RUN_TS = datetime.now(timezone.utc).isoformat()

# --- lakehouse-qualified table names ---
# Funnel outputs (all written to funnel_lakehouse):
T_CANONICAL   = "funnel_lakehouse.dbo.canonical_opportunities"
T_FUNNEL      = "funnel_lakehouse.dbo.funnel_facts"
T_CALLS       = "funnel_lakehouse.dbo.calls_facts"
T_ADMIT       = "funnel_lakehouse.dbo.admit_facts"
T_ADSPEND     = "funnel_lakehouse.dbo.ad_spend_facts"
T_ADSPEND_WK  = "funnel_lakehouse.dbo.ad_spend_channel_weekly"
T_LEADS_WK    = "funnel_lakehouse.dbo.dazos_leads_weekly"

# Sources (read-only, in their own lakehouses):
T_INTAKE      = "dazos_lakehouse.dbo.intake_opportunity_current"
T_ZOHO_LEADS  = "zoho_lakehouse.dbo.zoho_leads_current"
T_CENSUS      = "kipu_lakehouse.dbo.census_clean"
T_CTM_CALLS = "ctm_lakehouse.dbo.ctm_calls_raw"

# Bronze paths (read-only):
P_LEADS_BRONZE = "abfss://dazos-bronze@stkipu001.dfs.core.windows.net/leads/*/*/page_*.json"

# Key Vault / Google Sheets config (ad spend section):
AKV_NAME       = "kv-kipu1"
SA_SECRET_NAME = "GOOGLE-SHEETS-CREDENTIALS"
SHEET_ID       = "1JIkO2BxmO1Q5SJTFvse_YPHeJU4HmeJSsRrQ6-_mzUA"

print(f"Sales Funnel run starting: {RUN_TS}")
print("Setup complete — imports, config, qualified table names loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "funnel_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build Canonical Opportunities
#
# CRM-agnostic opportunity table. Now unions TWO sources:
#   - Dazos intake_opportunity_current  (Widespread + RNGA)  source_crm='dazos'
#   - Zoho zoho_leads_current           (Longbranch + EDTC)  source_crm='zoho'
#
# v2 changes (2026-05-21):
#   - opportunity_id is now STRING (Zoho ids overflow long; CRM-agnostic key)
#   - dob added to schema (lets admit_facts back-link without the intake join)
#   - Zoho projection added; reps canonicalized via rep_name_zoho
#
# Zoho stage caveat: VOB_Status / Admitted_Status are essentially unpopulated
# in Zoho Leads, so Zoho rows carry is_viable_vob_flag=False, is_admit_flag=
# False. Longbranch/EDTC admits come from KIPU (admit_facts), same as
# Widespread. Zoho provides opportunity + rep + referral + ad attribution only.
#
# Zoho facility rule: any EDTC signal across Location / Facility_Admit_to / Tag
# -> EDTC, else Longbranch.
#
# Reads:
#   dazos_lakehouse.dbo.intake_opportunity_current
#   zoho_lakehouse.dbo.zoho_leads_current
#   funnel_lakehouse/Files/Mappings/funnel_rep_dim.xlsx
# Writes: funnel_lakehouse.canonical_opportunities
# Schedule: 09:00 UTC (after both silver layers at 08:00).


import re
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame

SRC_DAZOS_INTAKE = T_INTAKE          # from Cell 1
SRC_ZOHO_LEADS   = T_ZOHO_LEADS      # from Cell 1
REP_DIM_PATH     = "/lakehouse/default/Files/Mappings/funnel_rep_dim.xlsx"
OUT_TABLE        = T_CANONICAL       # funnel_lakehouse.dbo.canonical_opportunities

DAZOS_TS_FORMAT = "MM-dd-yyyy h:mm a"
DAZOS_DOB_FMT   = "MM-dd-yyyy"
ZOHO_DOB_FMT    = "yyyy-MM-dd"

# Write modern-calendar dates as-is (Fabric is Spark 3.4+; safe). Without this,
# a stray pre-1900 dob/timestamp aborts the Parquet write with WRITE_ANCIENT_DATETIME.
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

RUN_TS = datetime.now(timezone.utc).isoformat()
print(f"Build_Canonical_Opportunities (v2, Dazos+Zoho) starting: {RUN_TS}")

# ============================================================================
# CELL 1
# ============================================================================

def normalize_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])

def get_or_null(df: DataFrame, colname: str, dtype: str = "string"):
    if colname.lower() in df.columns:
        return F.col(colname.lower())
    return F.lit(None).cast(dtype)

def e164_phone(c):
    digits = F.regexp_replace(F.coalesce(c.cast("string"), F.lit("")), r"\D", "")
    bare = F.when((F.length(digits) == 11) & (F.substring(digits, 1, 1) == F.lit("1")),
                  F.substring(digits, 2, 10)).otherwise(digits)
    return F.when(F.length(bare) == 10, F.concat(F.lit("+1"), bare)) \
            .when(F.length(bare) >= 7, F.concat(F.lit("+"), bare)) \
            .otherwise(F.lit(None).cast("string"))

def yes_no_to_bool(c):
    return F.when(F.lower(F.coalesce(c, F.lit(""))) == "yes", F.lit(True)) \
            .when(F.lower(F.coalesce(c, F.lit(""))) == "no",  F.lit(False)) \
            .otherwise(F.lit(None).cast("boolean"))

# ============================================================================
# CELL 2
# ============================================================================

import pandas as pd

io_df = normalize_columns(spark.read.table(SRC_DAZOS_INTAKE))
print(f"Dazos intake_opportunity_current: {io_df.count():,} rows")

try:
    zoho_df = normalize_columns(spark.read.table(SRC_ZOHO_LEADS))
    print(f"Zoho zoho_leads_current: {zoho_df.count():,} rows")
    HAS_ZOHO = True
except Exception as e:
    print(f"Zoho table not available yet ({e}); building Dazos-only.")
    HAS_ZOHO = False

rep_pd = pd.read_excel(REP_DIM_PATH, sheet_name="rep_dim")
print(f"rep_dim: {len(rep_pd)} rows")

# ============================================================================
# CELL 3
# ============================================================================

def build_rep_lookup(rep_pd, source_col):
    """Build raw->canonical lookup for a given source column (rep_name_dazos
    or rep_name_zoho). Returns a (possibly empty) Spark df."""
    if source_col not in rep_pd.columns:
        print(f"  (no {source_col} column in rep_dim)")
        return spark.createDataFrame([], "name_raw string, name_canonical string")
    sub = rep_pd[rep_pd[source_col].notna() & (rep_pd[source_col].astype(str).str.strip() != "")]
    sub = sub[[source_col, "rep_name_canonical"]].copy()
    sub[source_col] = sub[source_col].astype(str).str.strip()
    sub["rep_name_canonical"] = sub["rep_name_canonical"].astype(str).str.strip()
    sub = sub.drop_duplicates(subset=[source_col], keep="first")
    if len(sub) == 0:
        return spark.createDataFrame([], "name_raw string, name_canonical string")
    return spark.createDataFrame(sub).select(
        F.col(source_col).alias("name_raw"),
        F.col("rep_name_canonical").alias("name_canonical"),
    )

dazos_rep_lookup = build_rep_lookup(rep_pd, "rep_name_dazos")
zoho_rep_lookup  = build_rep_lookup(rep_pd, "rep_name_zoho")
print(f"Dazos rep mappings: {dazos_rep_lookup.count()}")
print(f"Zoho rep mappings:  {zoho_rep_lookup.count()}")

def _norm_rep(c):
    return F.lower(F.regexp_replace(F.trim(F.coalesce(c, F.lit(""))), r"\s+", " "))

def canonicalize_rep_column(df, col_name, rep_lookup):
    # Normalize both sides (lowercase + collapse whitespace) so case/spacing
    # variants resolve to one mapping row per person. Genuine variants
    # (first-name-only, name changes, nicknames) still need their own row.
    lk = (rep_lookup.withColumn("_nk", _norm_rep(F.col("name_raw")))
                    .select("_nk", "name_canonical").dropDuplicates(["_nk"]))
    joined = (df.withColumn("_rep_nk", _norm_rep(F.col(col_name)))
                .join(F.broadcast(lk), F.col("_rep_nk") == F.col("_nk"), "left"))
    return joined.withColumn(
        col_name,
        F.when(F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""), F.lit(None).cast("string"))
         .when(F.col("name_canonical").isNotNull(), F.col("name_canonical"))
         .otherwise(F.lit("Unknown"))
    ).drop("_rep_nk", "_nk", "name_canonical")

CANON_COLS = [
    "opportunity_id","source_crm","account_id","mr_number","member_id",
    "potential_name","client_first_name","client_last_name","dob","phone_e164",
    "opener","closer","bd_rep","lead_source","lead_type",
    "referring_company","referral_source","marketing_channel",
    "treatment_program",
    "admitting_to_loc","vob_status","is_viable_vob_flag","is_admit_flag",
    "payment_method","is_internal_transfer_flag","is_alumni_flag",
    "is_qualified_inquiry",
    "created_at","modified_at","source_loaded_at",
]

# ============================================================================
# CELL 4
# ============================================================================

def project_dazos_to_canonical(io_df):
    projected = io_df.select(
        get_or_null(io_df, "id").cast("string").alias("opportunity_id"),
        F.lit("dazos").alias("source_crm"),
        get_or_null(io_df, "accountid").alias("account_id"),
        get_or_null(io_df, "mr_number").alias("mr_number"),
        get_or_null(io_df, "member_id").alias("member_id"),
        get_or_null(io_df, "potential_name").alias("potential_name"),
        get_or_null(io_df, "client_first_name").alias("client_first_name"),
        get_or_null(io_df, "client_last_name").alias("client_last_name"),
        F.expr("CASE WHEN to_date(dob, 'MM-dd-yyyy') BETWEEN to_date('1900-01-01') AND current_date() "
               "THEN to_date(dob, 'MM-dd-yyyy') END").alias("dob"),
        e164_phone(get_or_null(io_df, "primary_phone")).alias("phone_e164"),
        get_or_null(io_df, "opener").alias("opener"),
        get_or_null(io_df, "closer").alias("closer"),
        get_or_null(io_df, "bd_rep").alias("bd_rep"),
        get_or_null(io_df, "lead_source").alias("lead_source"),
        get_or_null(io_df, "lead_type").alias("lead_type"),
        # referral fields (Dazos has no dedicated referring_company; use contact/hear-about-us)
        get_or_null(io_df, "referral_source_contact").alias("referring_company"),
        F.coalesce(get_or_null(io_df, "referral_source___hear_about_us"),
                   get_or_null(io_df, "referral_source_contact")).alias("referral_source"),
        get_or_null(io_df, "campaign_source").alias("marketing_channel"),
        get_or_null(io_df, "treatment_program").alias("treatment_program"),
        get_or_null(io_df, "admitting_to_loc").alias("admitting_to_loc"),
        get_or_null(io_df, "vob_status").alias("vob_status"),
        F.coalesce(get_or_null(io_df, "is_viable_vob_or_admit").isin("Viable VOBs","Admits"),
                   F.lit(False)).alias("is_viable_vob_flag"),
        (F.lower(F.coalesce(get_or_null(io_df, "sales_stage_1"), F.lit(""))) == "admitted").alias("is_admit_flag"),
        get_or_null(io_df, "payment_method").alias("payment_method"),
        yes_no_to_bool(get_or_null(io_df, "is_internal_transfer")).alias("is_internal_transfer_flag"),
        F.lit(None).cast("boolean").alias("is_alumni_flag"),
        F.lit(None).cast("boolean").alias("is_qualified_inquiry"),
        F.to_timestamp(get_or_null(io_df, "created_time"), DAZOS_TS_FORMAT).alias("created_at"),
        F.to_timestamp(get_or_null(io_df, "modified_time"), DAZOS_TS_FORMAT).alias("modified_at"),
        F.lit(RUN_TS).cast("timestamp").alias("source_loaded_at"),
    )
    projected = canonicalize_rep_column(projected, "opener", dazos_rep_lookup)
    projected = canonicalize_rep_column(projected, "closer", dazos_rep_lookup)
    projected = canonicalize_rep_column(projected, "bd_rep", dazos_rep_lookup)
    return projected.select(*CANON_COLS)

dazos_canonical = project_dazos_to_canonical(io_df)
print(f"Dazos canonical rows: {dazos_canonical.count():,}")

# ============================================================================
# CELL 5
# ============================================================================

def project_zoho_to_canonical(zoho_df):
    # Facility rule: any EDTC signal across Location / Facility_Admit_to / Tag -> EDTC
    loc = F.lower(F.coalesce(get_or_null(zoho_df, "location"), F.lit("")))
    fac = F.lower(F.coalesce(get_or_null(zoho_df, "facility_admit_to"), F.lit("")))
    tag = F.lower(F.coalesce(get_or_null(zoho_df, "tag"), F.lit("")))
    is_edtc = loc.contains("edtc") | fac.contains("eating disorder") | tag.contains("edtc")
    facility = F.when(is_edtc, F.lit("EDTC")).otherwise(F.lit("Longbranch"))

    projected = zoho_df.select(
        get_or_null(zoho_df, "id").cast("string").alias("opportunity_id"),
        F.lit("zoho").alias("source_crm"),
        F.lit(None).cast("string").alias("account_id"),
        F.lit(None).cast("string").alias("mr_number"),
        get_or_null(zoho_df, "member_id").alias("member_id"),
        get_or_null(zoho_df, "full_name").alias("potential_name"),
        get_or_null(zoho_df, "first_name").alias("client_first_name"),
        get_or_null(zoho_df, "last_name").alias("client_last_name"),
        F.expr("CASE WHEN to_date(date_of_birth, 'yyyy-MM-dd') BETWEEN to_date('1900-01-01') AND current_date() "
               "THEN to_date(date_of_birth, 'yyyy-MM-dd') END").alias("dob"),
        e164_phone(F.coalesce(get_or_null(zoho_df, "client_phone_number"),
                              get_or_null(zoho_df, "phone"),
                              get_or_null(zoho_df, "mobile"))).alias("phone_e164"),
        get_or_null(zoho_df, "owner").alias("opener"),                 # Inquiry Owner
        F.lit(None).cast("string").alias("closer"),                    # no closer concept in Zoho
        get_or_null(zoho_df, "bd_contact_owner").alias("bd_rep"),
        F.coalesce(get_or_null(zoho_df, "referral_source"),
                   get_or_null(zoho_df, "source")).alias("lead_source"),
        get_or_null(zoho_df, "lead_type").alias("lead_type"),
        # referral fields (Zoho has these directly - the BD-first signal source)
        get_or_null(zoho_df, "referring_company").alias("referring_company"),
        get_or_null(zoho_df, "referral_source").alias("referral_source"),
        get_or_null(zoho_df, "marketing_channel").alias("marketing_channel"),
        facility.alias("treatment_program"),
        F.coalesce(get_or_null(zoho_df, "facility_admit_to"),
                   get_or_null(zoho_df, "location")).alias("admitting_to_loc"),
        # VoB proxy: VOB_Status is unmaintained in Zoho (2/6893 filled), so a
        # populated Member_ID is treated as evidence a verification was run.
        # Real VOB_Status wins when present; label makes the proxy visible.
        F.when(F.coalesce(get_or_null(zoho_df, "vob_status"), F.lit("")) != "",
               get_or_null(zoho_df, "vob_status"))
         .when(F.trim(F.coalesce(get_or_null(zoho_df, "member_id"), F.lit(""))) != "",
               F.lit("VOB (Member ID present)"))
         .otherwise(F.lit(None).cast("string")).alias("vob_status"),
        F.lit(False).alias("is_viable_vob_flag"),   # not tracked in Zoho
        F.lit(False).alias("is_admit_flag"),         # admits come from KIPU
        F.lit(None).cast("string").alias("payment_method"),
        F.lit(None).cast("boolean").alias("is_internal_transfer_flag"),
        F.lit(None).cast("boolean").alias("is_alumni_flag"),
        F.when(F.lower(F.trim(get_or_null(zoho_df, "inquiry"))) == "qualified", F.lit(True))
         .when(F.lower(F.trim(get_or_null(zoho_df, "inquiry"))) == "not qualified", F.lit(False))
         .otherwise(F.lit(None).cast("boolean")).alias("is_qualified_inquiry"),
        F.to_timestamp(get_or_null(zoho_df, "created_time")).alias("created_at"),
        F.to_timestamp(get_or_null(zoho_df, "modified_time")).alias("modified_at"),
        F.lit(RUN_TS).cast("timestamp").alias("source_loaded_at"),
    )
    projected = canonicalize_rep_column(projected, "opener", zoho_rep_lookup)
    projected = canonicalize_rep_column(projected, "bd_rep", zoho_rep_lookup)
    return projected.select(*CANON_COLS)

if HAS_ZOHO:
    zoho_canonical = project_zoho_to_canonical(zoho_df)
    print(f"Zoho canonical rows: {zoho_canonical.count():,}")
    all_canonical = dazos_canonical.unionByName(zoho_canonical)
else:
    all_canonical = dazos_canonical

print(f"Total canonical rows: {all_canonical.count():,}")

# ============================================================================
# CELL 6
# ============================================================================

all_canonical.write.mode("overwrite").format("delta") \
    .option("overwriteSchema", "true").saveAsTable(OUT_TABLE)
print(f"Wrote {OUT_TABLE}")

# ============================================================================
# CELL 7
# ============================================================================

co = spark.read.table(OUT_TABLE)
total = co.count()
print("=" * 70)
print(f"canonical_opportunities: {total:,} rows")
print("=" * 70)

print("\nSource CRM breakdown:")
co.groupBy("source_crm").count().orderBy(F.col("count").desc()).show()

print("treatment_program by source:")
co.groupBy("source_crm","treatment_program").count().orderBy("source_crm", F.col("count").desc()).show(30, truncate=False)

print("dob fill rate by source:")
co.groupBy("source_crm").agg(
    F.count("*").alias("rows"),
    F.sum(F.col("dob").isNotNull().cast("int")).alias("dob_filled"),
    F.sum(F.col("phone_e164").isNotNull().cast("int")).alias("phone_filled"),
).show()

print("Rep coverage (canonical):")
for c in ("opener","closer","bd_rep"):
    matched = co.filter((F.col(c).isNotNull()) & (F.col(c) != "Unknown")).count()
    unknown = co.filter(F.col(c) == "Unknown").count()
    nulls   = co.filter(F.col(c).isNull()).count()
    print(f"  {c:8}: matched={matched:>6,}  Unknown={unknown:>6,}  NULL={nulls:>6,}")

print(f"\nBuild complete: {datetime.now(timezone.utc).isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "funnel_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build Funnel Facts
#
# Produces two tables in `funnel_lakehouse`:
# - `funnel_facts`  — one row per opportunity (Stages 2-5)
# - `calls_facts`   — one row per CTM call (Stage 1)
#
# Funnel scope: **Calls → Opportunities → VoBs → Viable VoBs → Admits**
# (Leads stage skipped per design decision 2026-05-17.)
#
# **Reads:**
# - `canonical_opportunities` (default lakehouse)
# - `ctm_lakehouse.dbo.ctm_calls_raw`
#
# **Match logic:** phone-only.
# - Normalize `phone_e164` on both sides
# - For each opportunity, find earliest inbound voice call with same phone
#   within 30 days BEFORE opportunity created_at
# - Match wins go on funnel_facts.first_call_*; non-matches stay NULL
#
# **What this notebook does NOT do (yet):**
# - KIPU admit verification (is_admit_flag remains Dazos's self-report)
# - 3-tier MR# match
# - AMA override
# These come in a future revision once the basic funnel is validated.
#
# **Schedule:** 09:30 UTC daily, after Build_Canonical_Opportunities at 09:00.


# --- imports ---
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# --- config ---
SRC_CANONICAL    = T_CANONICAL       # from Cell 1
SRC_CTM_CALLS    = T_CTM_CALLS       # from Cell 1
OUT_FUNNEL_FACTS = T_FUNNEL          # from Cell 1
OUT_CALLS_FACTS  = T_CALLS           # from Cell 1

# Look back this many days from opportunity.created_at for a matching inbound call
CALL_LOOKBACK_DAYS = 30
LOOKBACK_SECONDS   = CALL_LOOKBACK_DAYS * 24 * 3600

RUN_TS = datetime.now(timezone.utc).isoformat()
print(f"Build_Funnel_Facts starting: {RUN_TS}")
print(f"Call → Opportunity match window: {CALL_LOOKBACK_DAYS} days backward")

# ════════════════════════════════════════════════════════════════════════════
# CELL 1
# ════════════════════════════════════════════════════════════════════════════

# --- helpers (consistent with Build_Canonical_Opportunities) ---

def normalize_columns(df: DataFrame) -> DataFrame:
    """Lowercase all column names."""
    return df.toDF(*[c.lower() for c in df.columns])


def e164_phone(c):
    """Normalize a phone column to E.164."""
    digits = F.regexp_replace(F.coalesce(c.cast("string"), F.lit("")), r"\D", "")
    bare = F.when(
        (F.length(digits) == 11) & (F.substring(digits, 1, 1) == F.lit("1")),
        F.substring(digits, 2, 10)
    ).otherwise(digits)
    return F.when(F.length(bare) == 10, F.concat(F.lit("+1"), bare)) \
            .when(F.length(bare) >= 7, F.concat(F.lit("+"), bare)) \
            .otherwise(F.lit(None).cast("string"))

# ════════════════════════════════════════════════════════════════════════════
# CELL 2
# ════════════════════════════════════════════════════════════════════════════

# --- read sources ---
canonical = spark.read.table(SRC_CANONICAL)
ctm       = normalize_columns(spark.read.table(SRC_CTM_CALLS))

print(f"canonical_opportunities: {canonical.count():,} rows")
print(f"ctm_calls_raw:           {ctm.count():,} rows")

# ════════════════════════════════════════════════════════════════════════════
# CELL 3
# ════════════════════════════════════════════════════════════════════════════

# --- project CTM inbound voice calls with normalized phone (for matching) ---

ctm_voice = ctm.filter(
    (F.col("event_type") == "voice") &
    (F.col("direction")  == "inbound")
).select(
    F.col("id").alias("call_id"),
    F.col("called_at"),
    F.col("call_date"),
    F.col("unix_time"),
    F.col("caller_number"),
    F.col("caller_number_bare"),
    F.col("source").alias("ctm_source_name"),
    F.col("agent_id").alias("ctm_agent_id"),
    F.col("agent_name").alias("ctm_agent_name"),
    F.col("facility_resolved").alias("ctm_facility"),
    F.col("outcome").alias("call_outcome"),
).withColumn(
    "caller_phone_e164",
    F.coalesce(
        e164_phone(F.col("caller_number_bare")),
        e164_phone(F.col("caller_number")),
    )
)

ctm_voice_with_phone = ctm_voice.filter(F.col("caller_phone_e164").isNotNull())
print(f"Inbound voice calls: {ctm_voice.count():,}")
print(f"  ...with parseable phone: {ctm_voice_with_phone.count():,}")

# ════════════════════════════════════════════════════════════════════════════
# CELL 4
# ════════════════════════════════════════════════════════════════════════════

# --- match: each opportunity → earliest inbound voice call within window ---

opps_to_match = canonical.filter(
    F.col("phone_e164").isNotNull() & F.col("created_at").isNotNull()
).select(
    "opportunity_id",
    F.col("phone_e164").alias("opp_phone"),
    F.col("created_at").alias("opp_created_at"),
    F.unix_timestamp("created_at").alias("opp_created_unix"),
)

print(f"Opportunities with phone + created_at: {opps_to_match.count():,}")

candidates = opps_to_match.alias("o").join(
    ctm_voice_with_phone.alias("c"),
    (F.col("o.opp_phone") == F.col("c.caller_phone_e164")) &
    (F.col("c.unix_time") <= F.col("o.opp_created_unix")) &
    (F.col("c.unix_time") >= F.col("o.opp_created_unix") - F.lit(LOOKBACK_SECONDS)),
    "inner"
).select(
    F.col("o.opportunity_id"),
    F.col("c.call_id"),
    F.col("c.called_at"),
    F.col("c.call_date"),
    F.col("c.unix_time"),
    F.col("c.ctm_source_name"),
    F.col("c.ctm_agent_id"),
    F.col("c.ctm_agent_name"),
    F.col("c.ctm_facility"),
    F.col("c.call_outcome"),
)

# Earliest call per opportunity
w = Window.partitionBy("opportunity_id").orderBy(F.col("unix_time").asc())
opp_call_match = (
    candidates
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn", "unix_time")
    .withColumnRenamed("call_id",         "first_call_id")
    .withColumnRenamed("called_at",       "first_call_at")
    .withColumnRenamed("call_date",       "first_call_date")
    .withColumnRenamed("ctm_source_name", "first_call_source")
    .withColumnRenamed("ctm_agent_id",    "first_call_agent_id")
    .withColumnRenamed("ctm_agent_name",  "first_call_agent_name")
    .withColumnRenamed("ctm_facility",    "first_call_facility")
    .withColumnRenamed("call_outcome",    "first_call_outcome")
)

print(f"Opportunities matched to a call: {opp_call_match.count():,}")

# ════════════════════════════════════════════════════════════════════════════
# CELL 5
# ════════════════════════════════════════════════════════════════════════════

# --- build funnel_facts ---
# Start from canonical, attach matched call, derive stage flags.

funnel_facts = canonical.alias("c").join(
    opp_call_match.alias("m"), "opportunity_id", "left"
)

funnel_facts = funnel_facts.withColumn(
    # has_vob: opportunity has a real VOB status (anything other than blank/null/pending)
    "has_vob",
    F.col("vob_status").isNotNull() &
    (F.trim(F.col("vob_status")) != "") &
    (F.lower(F.col("vob_status")) != "pending")
).withColumn(
    # cohort anchor: matched call date if exists, else opportunity created_at date
    "cohort_anchor_date",
    F.coalesce(F.col("first_call_date"), F.to_date(F.col("created_at")))
).withColumn(
    "build_run_ts", F.lit(RUN_TS).cast("timestamp")
)

# Sanity: row count must equal canonical
assert funnel_facts.count() == canonical.count(), "funnel_facts row count drift"

print(f"funnel_facts rows: {funnel_facts.count():,}")
funnel_facts.printSchema()

# ════════════════════════════════════════════════════════════════════════════
# CELL 6
# ════════════════════════════════════════════════════════════════════════════

# --- write funnel_facts ---
funnel_facts.write.mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUT_FUNNEL_FACTS)

print(f"✅ Wrote {OUT_FUNNEL_FACTS}")

# ════════════════════════════════════════════════════════════════════════════
# CELL 7
# ════════════════════════════════════════════════════════════════════════════

# --- build calls_facts ---
# One row per CTM event (all directions, all event_types). Reverse-FK to
# opportunity if the call was matched.

# Project relevant CTM cols + normalized phone
calls_base = ctm.select(
    F.col("id").alias("call_id"),
    F.col("account_slug").alias("ctm_account_slug"),
    F.col("call_date"),
    F.col("called_at"),
    F.col("unix_time"),
    F.col("direction"),
    F.col("event_type"),
    F.col("outcome"),
    F.col("duration"),
    F.col("talk_time"),
    F.col("caller_number"),
    F.col("caller_number_bare"),
    F.col("tracking_number"),
    F.col("tracking_label"),
    F.col("source").alias("ctm_source_name"),
    F.col("agent_id").alias("ctm_agent_id"),
    F.col("agent_name").alias("ctm_agent_name"),
    F.col("facility_resolved").alias("ctm_facility"),
).withColumn(
    "caller_phone_e164",
    F.coalesce(
        e164_phone(F.col("caller_number_bare")),
        e164_phone(F.col("caller_number")),
    )
)

# Reverse join: pull opportunity_id from match table where call_id matches
calls_facts = calls_base.alias("c").join(
    opp_call_match.select(
        F.col("first_call_id").alias("_mc_call_id"),
        F.col("opportunity_id").alias("matched_opportunity_id"),
    ).alias("m"),
    F.col("c.call_id") == F.col("m._mc_call_id"),
    "left"
).drop("_mc_call_id")

calls_facts = calls_facts.withColumn("build_run_ts", F.lit(RUN_TS).cast("timestamp"))

print(f"calls_facts rows: {calls_facts.count():,}")

# ════════════════════════════════════════════════════════════════════════════
# CELL 8
# ════════════════════════════════════════════════════════════════════════════

# --- write calls_facts ---
calls_facts.write.mode("overwrite") \
    .format("delta") \
    .partitionBy("call_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUT_CALLS_FACTS)

print(f"✅ Wrote {OUT_CALLS_FACTS}")

# ════════════════════════════════════════════════════════════════════════════
# CELL 9
# ════════════════════════════════════════════════════════════════════════════

# --- sanity check / summary ---
ff = spark.read.table(OUT_FUNNEL_FACTS)
cf = spark.read.table(OUT_CALLS_FACTS)

calls_inbound_voice = cf.filter((F.col("event_type")=="voice") & (F.col("direction")=="inbound")).count()
opps_total          = ff.count()
opps_has_vob        = ff.filter(F.col("has_vob")).count()
opps_viable         = ff.filter(F.col("is_viable_vob_flag")).count()
opps_admit          = ff.filter(F.col("is_admit_flag")).count()
opps_matched_call   = ff.filter(F.col("first_call_id").isNotNull()).count()
opps_with_phone     = ff.filter(F.col("phone_e164").isNotNull()).count()

print("=" * 70)
print("FUNNEL STAGE COUNTS (all-time)")
print("=" * 70)
print(f"  Stage 1 — Inbound voice calls : {calls_inbound_voice:>8,}")
print(f"  Stage 2 — Opportunities       : {opps_total:>8,}")
print(f"  Stage 3 — VoBs                : {opps_has_vob:>8,}")
print(f"  Stage 4 — Viable VoBs         : {opps_viable:>8,}")
print(f"  Stage 5 — Admits (Dazos)      : {opps_admit:>8,}")
print()

print("CALL MATCH QUALITY")
print("-" * 70)
print(f"  Opps with phone (matchable)        : {opps_with_phone:>6,} / {opps_total:,}")
print(f"  Opps matched to a CTM call         : {opps_matched_call:>6,} / {opps_total:,}  ({100*opps_matched_call/max(opps_total,1):.1f}%)")
print(f"  Match rate among matchable opps    : {100*opps_matched_call/max(opps_with_phone,1):.1f}%")
print()

print("CALLS_FACTS REVERSE LINK")
print("-" * 70)
calls_total       = cf.count()
calls_matched     = cf.filter(F.col("matched_opportunity_id").isNotNull()).count()
print(f"  Total CTM events                   : {calls_total:>8,}")
print(f"  Calls linked to an opportunity     : {calls_matched:>8,}")
print()

print("FUNNEL BY TREATMENT_PROGRAM")
print("-" * 70)
ff.groupBy("treatment_program").agg(
    F.count("*").alias("opps"),
    F.sum(F.col("has_vob").cast("int")).alias("vobs"),
    F.sum(F.col("is_viable_vob_flag").cast("int")).alias("viable_vobs"),
    F.sum(F.col("is_admit_flag").cast("int")).alias("admits"),
).orderBy(F.col("admits").desc()).show(15, truncate=False)

print("FUNNEL BY first_call_source (CTM source on the matched call)")
print("-" * 70)
ff.filter(F.col("first_call_source").isNotNull()) \
  .groupBy("first_call_source").agg(
      F.count("*").alias("opps"),
      F.sum(F.col("is_viable_vob_flag").cast("int")).alias("viable_vobs"),
      F.sum(F.col("is_admit_flag").cast("int")).alias("admits"),
  ).orderBy(F.col("opps").desc()).show(15, truncate=False)

print("=" * 70)
print(f"Build complete: {datetime.now(timezone.utc).isoformat()}")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "funnel_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build Admit Facts (v2 - 8 facilities)
#
# KIPU is the admit truth. One row per real KIPU admission (mr_number +
# admission_date), all LOCs, back-linked to a canonical opportunity for
# attribution. The admit counts whether or not the link succeeds.
#
# v2 changes (2026-05-21):
#   - Added Longbranch + EDTC (KIPU facility="Longbranch"; EDTC split out by
#     location_name contains "eating disorder").
#   - Dropped the intake_opportunity_current dependency: dob now read straight
#     from canonical_opportunities (populated for both Dazos and Zoho).
#   - Added match_confidence (high / low / unmatched) and a name-only +
#     admit-date-proximity fallback tier (low confidence, filterable).
#
# Back-link tiers (best per admit):
#   1. mr_exact          (priority 1, high)   - Dazos facilities carry MR#
#   2. name_dob          (priority 2, high)   - primary for Longbranch/EDTC
#   3. name_only_proximity (priority 3, low)  - fallback; closest created_at to
#                                               admission_date; flagged low
#
# In-scope KIPU facilities -> facility_group:
#   Widespread:  Green Acres Recovery, Tides Edge Recovery, Lotus Wellness,
#                Chattanooga Recovery Center, Graceland Recovery
#   RNGA:        RNGA
#   Longbranch:  facility="Longbranch" AND NOT eating-disorder location
#   EDTC:        facility="Longbranch" AND location_name ~ "eating disorder"
#
# Reads:  kipu_lakehouse.dbo.census_clean, canonical_opportunities
# Writes: funnel_lakehouse.admit_facts
# Schedule: 09:45 UTC (after Build_Canonical 09:00, Build_Funnel 09:30).


from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

SRC_KIPU_CENSUS = T_CENSUS       # from Cell 1
SRC_CANONICAL   = T_CANONICAL    # from Cell 1
OUT_TABLE       = T_ADMIT        # from Cell 1

# Widespread + RNGA come straight from KIPU facility name.
SIMPLE_FACILITY_GROUP = {
    "Green Acres Recovery":        "Widespread",
    "Tides Edge Recovery":         "Widespread",
    "Lotus Wellness":              "Widespread",
    "Chattanooga Recovery Center": "Widespread",
    "Graceland Recovery":          "Widespread",
    "RNGA":                        "Recover Now Georgia",
}
# Longbranch + EDTC both have KIPU facility="Longbranch"; split by location_name.
LONGBRANCH_KIPU_FACILITY = "Longbranch"
EDTC_LOCATION_TOKEN = "eating disorder"

KIPU_DOB_FMT = "yyyy-MM-dd"

RUN_TS = datetime.now(timezone.utc).isoformat()
print(f"Build_Admit_Facts (v2, 8 facilities) starting: {RUN_TS}")

# ============================================================================
# CELL 1
# ============================================================================

def normalize_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])

def name_dob_key(fn, ln, dob_date):
    fn_n = F.lower(F.trim(F.coalesce(fn, F.lit(""))))
    ln_n = F.lower(F.trim(F.coalesce(ln, F.lit(""))))
    return F.when(
        dob_date.isNotNull() & (fn_n != "") & (ln_n != ""),
        F.concat_ws("|", fn_n, ln_n, dob_date.cast("string"))
    ).otherwise(F.lit(None).cast("string"))

def name_key(fn, ln):
    fn_n = F.lower(F.trim(F.coalesce(fn, F.lit(""))))
    ln_n = F.lower(F.trim(F.coalesce(ln, F.lit(""))))
    return F.when((fn_n != "") & (ln_n != ""), F.concat_ws("|", fn_n, ln_n)) \
            .otherwise(F.lit(None).cast("string"))

def norm_mr(c):
    u = F.upper(F.trim(F.coalesce(c.cast("string"), F.lit(""))))
    return F.when(u != "", u).otherwise(F.lit(None).cast("string"))

# ============================================================================
# CELL 2
# ============================================================================

cen = normalize_columns(spark.read.table(SRC_KIPU_CENSUS))

simple_facilities = list(SIMPLE_FACILITY_GROUP.keys())
in_scope = (
    F.col("facility").isin(simple_facilities) |
    (F.col("facility") == LONGBRANCH_KIPU_FACILITY)
)
adm_rows = cen.filter(in_scope & F.col("mr_number").isNotNull() & F.col("admission_date").isNotNull())

# Collapse to admission grain (earliest census_date wins)
w_adm = Window.partitionBy("mr_number", "admission_date").orderBy(F.col("census_date").asc())
admits = (adm_rows.withColumn("_rn", F.row_number().over(w_adm)).filter("_rn = 1").drop("_rn")
    .select("mr_number","admission_date","casefile_id","first_name","last_name",
            F.to_date("dob", KIPU_DOB_FMT).alias("dob_date"),
            "facility","location_name","program","level_of_care",
            "discharge_date","discharge_type","payment_method","insurance_company"))

# Facility + facility_group: split Longbranch vs EDTC by location_name
loc_l = F.lower(F.coalesce(F.col("location_name"), F.lit("")))
is_edtc = (F.col("facility") == LONGBRANCH_KIPU_FACILITY) & loc_l.contains(EDTC_LOCATION_TOKEN)

facility_resolved = (
    F.when(is_edtc, F.lit("EDTC"))
     .when(F.col("facility") == LONGBRANCH_KIPU_FACILITY, F.lit("Longbranch"))
     .otherwise(F.col("facility"))
)
simple_map = F.create_map(*[x for kv in SIMPLE_FACILITY_GROUP.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
group_resolved = (
    F.when(is_edtc, F.lit("EDTC"))
     .when(F.col("facility") == LONGBRANCH_KIPU_FACILITY, F.lit("Longbranch"))
     .otherwise(simple_map[F.col("facility")])
)

admits = (admits
    .withColumn("facility_resolved", facility_resolved)
    .withColumn("facility_group", group_resolved)
    .withColumn("admit_mr", norm_mr(F.col("mr_number")))
    .withColumn("admit_name_dob_key", name_dob_key(F.col("first_name"), F.col("last_name"), F.col("dob_date")))
    .withColumn("admit_name_key", name_key(F.col("first_name"), F.col("last_name")))
)
admits.cache()
total_admits = admits.count()
print(f"In-scope KIPU admissions (admission grain): {total_admits:,}")
admits.groupBy("facility_group","facility_resolved").count().orderBy("facility_group", F.col("count").desc()).show(truncate=False)

# ============================================================================
# CELL 3
# ============================================================================

canonical = spark.read.table(SRC_CANONICAL)

opps_match = canonical.select(
    "opportunity_id",
    norm_mr(F.col("mr_number")).alias("opp_mr"),
    name_dob_key(F.col("client_first_name"), F.col("client_last_name"), F.col("dob")).alias("opp_name_dob_key"),
    name_key(F.col("client_first_name"), F.col("client_last_name")).alias("opp_name_key"),
    F.col("created_at").alias("opp_created_at"),
    "source_crm","opener","closer","bd_rep","lead_source",
    F.col("treatment_program").alias("treatment_program_crm"),
)
opps_match.cache()
print(f"Canonical opportunities available for back-link: {opps_match.count():,}")
print(f"  with MR#:          {opps_match.filter(F.col('opp_mr').isNotNull()).count():,}")
print(f"  with name+dob key: {opps_match.filter(F.col('opp_name_dob_key').isNotNull()).count():,}")
print(f"  with name key:     {opps_match.filter(F.col('opp_name_key').isNotNull()).count():,}")

# ============================================================================
# CELL 4
# ============================================================================

COMMON = ["o.opportunity_id","o.opp_created_at","o.source_crm",
          "o.opener","o.closer","o.bd_rep","o.lead_source","o.treatment_program_crm"]

# Tier 1 - MR# exact (high)
mr_links = admits.select("mr_number","admission_date","admit_mr").alias("a").join(
    opps_match.alias("o"), F.col("a.admit_mr") == F.col("o.opp_mr"), "inner"
).select("a.mr_number","a.admission_date", *COMMON,
         F.lit(1).alias("match_priority"), F.lit("mr_exact").alias("match_method"),
         F.lit("high").alias("match_confidence"))

# Tier 2 - name + DOB (high)
namedob_links = admits.select("mr_number","admission_date","admit_name_dob_key").alias("a").join(
    opps_match.alias("o"), F.col("a.admit_name_dob_key") == F.col("o.opp_name_dob_key"), "inner"
).select("a.mr_number","a.admission_date", *COMMON,
         F.lit(2).alias("match_priority"), F.lit("name_dob").alias("match_method"),
         F.lit("high").alias("match_confidence"))

# Tier 3 - name only (low); resolved by admit-date proximity in the window below
nameonly_links = admits.select("mr_number","admission_date","admit_name_key").alias("a").join(
    opps_match.alias("o"), F.col("a.admit_name_key") == F.col("o.opp_name_key"), "inner"
).select("a.mr_number","a.admission_date", *COMMON,
         F.lit(3).alias("match_priority"), F.lit("name_only_proximity").alias("match_method"),
         F.lit("low").alias("match_confidence"))

all_links = mr_links.unionByName(namedob_links).unionByName(nameonly_links)

# Best per admit: lowest priority, then opp created closest to admission date
all_links = all_links.withColumn(
    "_days_apart", F.abs(F.datediff(F.col("opp_created_at"), F.col("admission_date"))))
w_best = Window.partitionBy("mr_number","admission_date").orderBy(
    F.col("match_priority").asc(), F.col("_days_apart").asc_nulls_last())
best_link = (all_links.withColumn("_rn", F.row_number().over(w_best))
             .filter("_rn = 1").drop("_rn","_days_apart","opp_created_at"))

print(f"Admits with a back-link: {best_link.count():,}")
best_link.groupBy("match_method","match_confidence").count().orderBy("match_method").show()

# ============================================================================
# CELL 5
# ============================================================================

admit_facts = admits.join(best_link, ["mr_number","admission_date"], "left")

admit_facts = (admit_facts
    .withColumn("is_back_linked", F.col("opportunity_id").isNotNull())
    .withColumn("match_method", F.coalesce(F.col("match_method"), F.lit("unmatched")))
    .withColumn("match_confidence", F.coalesce(F.col("match_confidence"), F.lit("unmatched")))
    .withColumn("build_run_ts", F.lit(RUN_TS).cast("timestamp"))
    .select(
        "mr_number","admission_date","casefile_id","first_name","last_name","dob_date",
        F.col("facility_resolved").alias("facility"), "facility_group",
        "location_name","program","level_of_care",
        "discharge_date","discharge_type","payment_method","insurance_company",
        "opportunity_id","source_crm","match_method","match_confidence","is_back_linked",
        "opener","closer","bd_rep","lead_source",
        F.col("treatment_program_crm").alias("treatment_program_crm"),
        "build_run_ts",
    ))
# canonicalize facility to short names
fac_map = {
    "Tides Edge Recovery": "Tides Edge", "Chattanooga Recovery Center": "Chattanooga",
    "Green Acres Recovery": "Green Acres", "Lotus Wellness": "Lotus",
    "Graceland Recovery": "Graceland", "Recover Now": "Recover Now (parent)",
}
_m = F.create_map([F.lit(x) for kv in fac_map.items() for x in kv])
admit_facts = admit_facts.withColumn("facility", F.coalesce(_m[F.col("facility")], F.col("facility")))
print(f"admit_facts rows: {admit_facts.count():,}")
admit_facts.write.mode("overwrite").format("delta").option("overwriteSchema","true").saveAsTable(OUT_TABLE)
print(f"Wrote {OUT_TABLE}")

# ============================================================================
# CELL 6
# ============================================================================

af = spark.read.table(OUT_TABLE)
total = af.count()
print("=" * 70)
print(f"admit_facts: {total:,} admissions (KIPU truth, 8 facilities)")
print("=" * 70)

print("\nBy facility_group:")
af.groupBy("facility_group").count().orderBy(F.col("count").desc()).show(truncate=False)
print("By facility:")
af.groupBy("facility").count().orderBy(F.col("count").desc()).show(truncate=False)

print("Back-link coverage:")
linked = af.filter(F.col("is_back_linked")).count()
print(f"  Linked:   {linked:>6,} / {total:,} ({100*linked/max(total,1):.1f}%)")
print(f"  Unlinked: {total-linked:>6,}  (counts anyway - KIPU is truth)")

print("\nMatch method x confidence:")
af.groupBy("match_method","match_confidence").count().orderBy(F.col("count").desc()).show()

print("Back-link rate by facility:")
af.groupBy("facility").agg(
    F.count("*").alias("admits"),
    F.sum(F.col("is_back_linked").cast("int")).alias("linked"),
    F.sum((F.col("match_confidence")=="high").cast("int")).alias("high_conf"),
    F.sum((F.col("match_confidence")=="low").cast("int")).alias("low_conf"),
).withColumn("link_pct", F.round(100*F.col("linked")/F.col("admits"),1)) \
 .orderBy(F.col("admits").desc()).show(truncate=False)

print("Source CRM of linked admits:")
af.filter(F.col("is_back_linked")).groupBy("source_crm").count().show()

print(f"\nBuild complete: {datetime.now(timezone.utc).isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": { "name": "synapse_pyspark" },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "funnel_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build Verified Admits  (admit truth = human-verified files, NOT the CRM)
#
# Reads the weekly verified admit files the team uploads and writes ONE clean
# table: funnel_lakehouse.dbo.verified_admits. The funnel reads THIS for admits.
# admit_facts (KIPU) is left intact for the weekly attribution scripts, but the
# funnel no longer uses it.
#
# Sources:
#   Widespread Admissions/Weekly_admits_attribution_*.xlsx  (tab: Admits Detail)
#       -> Widespread + RNGA + RNCA.  rep = Closer.
#   BD_Reports/BD_Admit_Source_*.xlsx  (tab: BD Admits)
#       -> Longbranch + EDTC, May-15-onward.  rep = "Admission*" (loose).
#   BD_Reports/2026 BD Data.xlsx  (tabs: 2026 Data EDTC, 2026 Data Longbranch)
#       -> Longbranch + EDTC history (pre-May-15).  rep left empty.
#
# Rebuild-from-scratch each run (overwrite): the team's latest files always win.
# Duplicates (same MR + admit_date across files) are FLAGGED, not auto-removed.
#
# Writes: funnel_lakehouse.dbo.verified_admits

import os
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

T_VERIFIED_ADMITS = "funnel_lakehouse.dbo.verified_admits"
WS_DIR = "/lakehouse/default/Files/Widespread Admissions"
BD_DIR = "/lakehouse/default/Files/BD_Reports"
RUN_TS = datetime.now(timezone.utc).isoformat()
print(f"Build Verified Admits starting: {RUN_TS}")

# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def _s(v):
    """Clean string: strip, treat blanks/--/nan/0 as empty."""
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in ("", "nan", "none", "--", "0"):
        return ""
    return s

def _find_col(cols, *substrings, exclude=()):
    """Find a column whose lowercased name contains any substring (and no exclude)."""
    for c in cols:
        cl = str(c).strip().lower()
        if any(x in cl for x in exclude):
            continue
        if any(sub in cl for sub in substrings):
            return c
    return None

# facility crosswalk -> short names (Kipu/CRM spellings on the left)
FAC_CROSSWALK = {
    "tides edge recovery services": "Tides Edge", "tides edge recovery": "Tides Edge",
    "11th ave north": "Tides Edge",
    "green acres wellness": "Green Acres", "green acres recovery": "Green Acres",
    "lotus wellness": "Lotus",
    "chattanooga recovery center": "Chattanooga", "chattanooga recovery c": "Chattanooga",
    "graceland recovery": "Graceland",
    "recover now georgia": "RNGA", "recover now greater atlanta": "RNGA",
    "recover now central alabama": "RNCA",
    "longbranch": "Longbranch", "longbranch recovery ce": "Longbranch",
    "longbranch recovery center": "Longbranch", "longbranch outpatient": "Longbranch",
    "longbranch wellness ce": "Longbranch",
    "edtc": "EDTC", "eating disorder treatm": "EDTC", "eating disorder treatment": "EDTC",
}
def norm_fac(raw):
    s = _s(raw).lower()
    if not s:
        return ""
    for k, v in FAC_CROSSWALK.items():
        if k in s:
            return v
    return _s(raw)  # leave as-is if unmapped (will surface in review)

# ---- channel rule (funnel 4-bucket): PPC / Organic / BD / Other ----
def channel_of(*sources):
    s = " ".join(_s(x).lower() for x in sources)
    if any(k in s for k in ("google ads", "ppc", "pmax", "paid", "meta", "fb ads", "facebook")):
        return "PPC"
    if any(k in s for k in ("transfer", "referral", "sales rep", "business development", "3rd party")):
        return "BD"
    if any(k in s for k in ("organic", "seo", "gmb", "local listing", "internet search",
                            "website", "direct site", "bing")):
        return "Organic"
    return "Other"

# ---- Widespread lead_type comes straight from the file's "Lead Type" column ----
# ---- Longbranch/EDTC lead_type: derive to match Widespread's labels ----
#   Order: BD first, then Alumni, PPC, SEO, else Other.
def bd_lead_type(referral_source, referring_company, bd_contact, marketing_channel):
    rs = _s(referral_source); rc = _s(referring_company)
    bd = _s(bd_contact); mc = _s(marketing_channel)
    blob = " ".join(x.lower() for x in (rs, rc, bd, mc))
    # 1. BD: a referring company OR bd contact present -> BD
    if rc or bd:
        return "BD"
    # 2. Alumni
    if any(k in blob for k in ("alumni", "word of mouth", "readmission")):
        return "Alumni"
    # 3. PPC
    if any(k in blob for k in ("google ads", "ppc", "pmax", "paid")):
        return "PPC"
    # 4. SEO (organic/internet)
    if any(k in blob for k in ("internet search", "organic", "gmb", "website", "direct site")):
        return "SEO"
    return "Other"

# unified output schema (one row per admit)
OUT_COLS = [
    "entity", "source_crm", "admit_date", "mr_number", "patient_name",
    "facility", "program", "admission_rep", "lead_type", "channel",
    "vob_status", "sales_stage", "opener",
    "bd_rep", "referral_source", "referring_company",
    "payor_category", "program_bucket", "points", "insurance",
    "match_confidence", "source_file",
]

rows = []          # list of dicts
unreadable = []    # files we couldn't parse

# ----------------------------------------------------------------------------
# 1. WIDESPREAD  (tab: Admits Detail; rep = Closer)
# ----------------------------------------------------------------------------
def read_widespread():
    if not os.path.isdir(WS_DIR):
        print(f"  Widespread dir not found: {WS_DIR}"); return
    for fn in sorted(os.listdir(WS_DIR)):
        if fn.startswith("~$") or not fn.lower().endswith((".xlsx", ".xls")):
            continue
        path = os.path.join(WS_DIR, fn)
        try:
            df = pd.read_excel(path, sheet_name="Admits Detail")
        except Exception as e:
            unreadable.append((fn, str(e))); continue
        cols = list(df.columns)
        c_date = _find_col(cols, "admit date", "admission date")
        c_mr   = _find_col(cols, "mr")
        c_fn   = _find_col(cols, "first name")
        c_ln   = _find_col(cols, "last name")
        c_loc  = _find_col(cols, "kipu location", "location", exclude=("program",))
        c_prog = _find_col(cols, "kipu program", "program", exclude=("bucket",))
        c_closer = _find_col(cols, "closer")
        c_opener = _find_col(cols, "opener")
        c_lt   = _find_col(cols, "lead type")
        c_ls   = _find_col(cols, "lead source")
        c_camp = _find_col(cols, "campaign source")
        c_vob  = _find_col(cols, "vob status")
        c_ss   = _find_col(cols, "sales stage")
        c_rs   = _find_col(cols, "referral source")
        c_bd   = _find_col(cols, "bd rep")
        c_conf = _find_col(cols, "match confidence")
        for _, r in df.iterrows():
            mr = _s(r.get(c_mr))
            nm = (_s(r.get(c_fn)) + " " + _s(r.get(c_ln))).strip()
            if not mr and not nm:
                continue
            lead_type = _s(r.get(c_lt)) or "Other"
            rows.append({
                "entity": "Widespread",
                "source_crm": "dazos",
                "admit_date": r.get(c_date),
                "mr_number": mr,
                "patient_name": nm,
                "facility": norm_fac(r.get(c_loc)),
                "program": _s(r.get(c_prog)),
                "admission_rep": _s(r.get(c_closer)),
                "lead_type": lead_type,
                "channel": channel_of(r.get(c_ls), r.get(c_camp), lead_type),
                "vob_status": _s(r.get(c_vob)),
                "sales_stage": _s(r.get(c_ss)),
                "opener": _s(r.get(c_opener)),
                "bd_rep": _s(r.get(c_bd)),
                "referral_source": _s(r.get(c_rs)),
                "referring_company": "",
                "payor_category": "", "program_bucket": "", "points": None, "insurance": "",
                "match_confidence": _s(r.get(c_conf)),
                "source_file": fn,
            })

# ----------------------------------------------------------------------------
# 2. BD WEEKLY  (tab: BD Admits; rep = loose "Admission*"; May-15-onward)
# ----------------------------------------------------------------------------
def read_bd_weekly():
    if not os.path.isdir(BD_DIR):
        print(f"  BD dir not found: {BD_DIR}"); return
    for fn in sorted(os.listdir(BD_DIR)):
        if fn.startswith("~$") or not fn.lower().endswith((".xlsx", ".xls")):
            continue
        if not fn.startswith("BD_Admit_Source"):
            continue
        path = os.path.join(BD_DIR, fn)
        try:
            df = pd.read_excel(path, sheet_name="BD Admits")
        except Exception as e:
            unreadable.append((fn, str(e))); continue
        cols = list(df.columns)
        c_mr   = _find_col(cols, "mr", exclude=("number",)) or _find_col(cols, "mr")
        c_pn   = _find_col(cols, "patient name", "name", exclude=("company", "bucket"))
        c_date = _find_col(cols, "admission date", "admit date")
        c_fac  = _find_col(cols, "facility", exclude=("admit",))
        c_loc  = _find_col(cols, "location")
        c_prog = _find_col(cols, "program", exclude=("bucket",))
        c_pb   = _find_col(cols, "program bucket")
        c_ins  = _find_col(cols, "insurance")
        c_pay  = _find_col(cols, "payor category")
        c_pts  = _find_col(cols, "points")
        c_mc   = _find_col(cols, "marketing channel")
        c_rs   = _find_col(cols, "referral source")
        c_rc   = _find_col(cols, "referring company")
        c_bd   = _find_col(cols, "bd contact", exclude=("2",))
        c_rep  = _find_col(cols, "admission")          # loose: Admission Rep/Reps/Admissions Rep
        for _, r in df.iterrows():
            mr = _s(r.get(c_mr))
            nm = _s(r.get(c_pn))
            if not mr and not nm:
                continue
            fac = norm_fac(r.get(c_fac)) or norm_fac(r.get(c_loc))
            entity = "EDTC" if fac == "EDTC" else "Longbranch"
            lt = bd_lead_type(r.get(c_rs), r.get(c_rc), r.get(c_bd), r.get(c_mc))
            pts = r.get(c_pts)
            try: pts = float(pts) if _s(pts) else None
            except Exception: pts = None
            rows.append({
                "entity": entity,
                "source_crm": "zoho",
                "admit_date": r.get(c_date),
                "mr_number": mr,
                "patient_name": nm,
                "facility": fac,
                "program": _s(r.get(c_prog)),
                "admission_rep": _s(r.get(c_rep)),
                "lead_type": lt,
                "channel": channel_of(r.get(c_mc), r.get(c_rs), lt),
                "vob_status": "", "sales_stage": "", "opener": "",
                "bd_rep": _s(r.get(c_bd)),
                "referral_source": _s(r.get(c_rs)),
                "referring_company": _s(r.get(c_rc)),
                "payor_category": _s(r.get(c_pay)),
                "program_bucket": _s(r.get(c_pb)),
                "points": pts,
                "insurance": _s(r.get(c_ins)),
                "match_confidence": "",
                "source_file": fn,
            })

# ----------------------------------------------------------------------------
# 3. 2026 BD DATA history  (tabs: 2026 Data EDTC / 2026 Data Longbranch; rep empty)
# ----------------------------------------------------------------------------
def read_bd_history():
    path = os.path.join(BD_DIR, "2026 BD Data.xlsx")
    if not os.path.exists(path):
        print("  2026 BD Data.xlsx not found (history skipped)"); return
    for tab, entity in [("2026 Data EDTC", "EDTC"), ("2026 Data Longbranch", "Longbranch")]:
        try:
            df = pd.read_excel(path, sheet_name=tab)
        except Exception as e:
            unreadable.append((f"2026 BD Data.xlsx[{tab}]", str(e))); continue
        cols = list(df.columns)
        c_mr   = _find_col(cols, "mr")
        c_pn   = _find_col(cols, "full name", "name", exclude=("company",))
        c_date = _find_col(cols, "admission date")
        c_fac  = _find_col(cols, "facility")
        c_loc  = _find_col(cols, "location")
        c_prog = _find_col(cols, "program", exclude=("bucket",))
        c_pb   = _find_col(cols, "program bucket")
        c_ins  = _find_col(cols, "insurance")
        c_pay  = _find_col(cols, "payor category")
        c_pts  = _find_col(cols, "points")
        c_rs   = _find_col(cols, "referral source")
        c_rc   = _find_col(cols, "referring company")
        c_bd   = _find_col(cols, "bd contact", exclude=("2",))
        for _, r in df.iterrows():
            mr = _s(r.get(c_mr))
            nm = _s(r.get(c_pn))
            if not mr and not nm:
                continue
            fac = norm_fac(r.get(c_fac)) or norm_fac(r.get(c_loc)) or entity
            ent = "EDTC" if fac == "EDTC" else "Longbranch"
            lt = bd_lead_type(r.get(c_rs), r.get(c_rc), r.get(c_bd), "")
            pts = r.get(c_pts)
            try: pts = float(pts) if _s(pts) else None
            except Exception: pts = None
            rows.append({
                "entity": ent,
                "source_crm": "zoho",
                "admit_date": r.get(c_date),
                "mr_number": mr,
                "patient_name": nm,
                "facility": fac,
                "program": _s(r.get(c_prog)),
                "admission_rep": "",                      # no rep in history (expected)
                "lead_type": lt,
                "channel": channel_of(r.get(c_rs), lt),
                "vob_status": "", "sales_stage": "", "opener": "",
                "bd_rep": _s(r.get(c_bd)),
                "referral_source": _s(r.get(c_rs)),
                "referring_company": _s(r.get(c_rc)),
                "payor_category": _s(r.get(c_pay)),
                "program_bucket": _s(r.get(c_pb)),
                "points": pts,
                "insurance": _s(r.get(c_ins)),
                "match_confidence": "",
                "source_file": "2026 BD Data.xlsx",
            })

read_widespread()
read_bd_weekly()
read_bd_history()

print(f"Raw rows read: {len(rows)}")
if unreadable:
    print("UNREADABLE files:")
    for fn, e in unreadable:
        print(f"  {fn}: {e}")

# ----------------------------------------------------------------------------
# assemble, normalize dates, flag duplicates
# ----------------------------------------------------------------------------
pdf = pd.DataFrame(rows, columns=OUT_COLS)

# normalize admit_date -> date
pdf["admit_date"] = pd.to_datetime(pdf["admit_date"], errors="coerce").dt.date

# blank strings -> None so Spark types cleanly
for c in OUT_COLS:
    if c in ("points",):
        continue
    pdf[c] = pdf[c].apply(lambda v: v if (v is not None and str(v).strip() != "") else None)

# duplicate flag on MR + admit_date (no auto-pick)
key = pdf["mr_number"].astype(str) + "|" + pdf["admit_date"].astype(str)
dup_counts = key.value_counts()
dups = set(dup_counts[dup_counts > 1].index)
pdf["dup_flag"] = key.isin(dups)
# list the files each dup key appears in
src_by_key = pdf.groupby(key)["source_file"].apply(lambda s: " | ".join(sorted(set(s.astype(str)))))
pdf["dup_sources"] = key.map(src_by_key).where(pdf["dup_flag"], None)

# ----------------------------------------------------------------------------
# write Delta (full overwrite each run)
# ----------------------------------------------------------------------------
schema = T.StructType([
    T.StructField("entity", T.StringType()),
    T.StructField("source_crm", T.StringType()),
    T.StructField("admit_date", T.DateType()),
    T.StructField("mr_number", T.StringType()),
    T.StructField("patient_name", T.StringType()),
    T.StructField("facility", T.StringType()),
    T.StructField("program", T.StringType()),
    T.StructField("admission_rep", T.StringType()),
    T.StructField("lead_type", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("vob_status", T.StringType()),
    T.StructField("sales_stage", T.StringType()),
    T.StructField("opener", T.StringType()),
    T.StructField("bd_rep", T.StringType()),
    T.StructField("referral_source", T.StringType()),
    T.StructField("referring_company", T.StringType()),
    T.StructField("payor_category", T.StringType()),
    T.StructField("program_bucket", T.StringType()),
    T.StructField("points", T.DoubleType()),
    T.StructField("insurance", T.StringType()),
    T.StructField("match_confidence", T.StringType()),
    T.StructField("source_file", T.StringType()),
    T.StructField("dup_flag", T.BooleanType()),
    T.StructField("dup_sources", T.StringType()),
])

# build_run_ts on every row
pdf["build_run_ts"] = RUN_TS

sdf = spark.createDataFrame(pdf, schema=T.StructType(list(schema.fields) + [
    T.StructField("build_run_ts", T.StringType())
]))
sdf = sdf.withColumn("build_run_ts", F.to_timestamp("build_run_ts"))

sdf.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \
    .saveAsTable(T_VERIFIED_ADMITS)
print(f"Wrote {T_VERIFIED_ADMITS}")

# ----------------------------------------------------------------------------
# summary
# ----------------------------------------------------------------------------
n = len(pdf)
print("=" * 60)
print(f"verified_admits built: {n} rows")
for ent in ["Widespread", "Longbranch", "EDTC"]:
    print(f"  {ent:11}: {(pdf['entity'] == ent).sum()}")
print(f"  no rep (history/blank): {pdf['admission_rep'].isna().sum()}")
print(f"Duplicates flagged: {int(pdf['dup_flag'].sum())}  (see dup_flag / dup_sources)")
print(f"Unreadable files: {len(unreadable)}")
print("lead_type distribution:")
print(pdf["lead_type"].value_counts().to_string())
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ════════════════════════════════════════════════════════════════════════════
# SALES FUNNEL — Ad Spend Facts
# Reads 3 Google Ads tabs from one Sheet, maps campaign -> facility + channel,
# rolls daily -> weekly, UPSERTS into ad_spend_facts (newest pull wins).
# NOTE: gspread/google-auth installed + notebookutils imported in Cell 1.
# PREREQ: the Sheet must be shared (Viewer) with the service account email in
#         the GOOGLE-SHEETS-CREDENTIALS secret, or open_by_key 404s.
# ════════════════════════════════════════════════════════════════════════════

import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

AKV_NAME       = "kv-kipu1"
SA_SECRET_NAME = "GOOGLE-SHEETS-CREDENTIALS"
SHEET_ID       = "1JIkO2BxmO1Q5SJTFvse_YPHeJU4HmeJSsRrQ6-_mzUA"
TABS           = ["ad_spend_GAW", "ad_spend_LW", "ad_spend_RN"]
OUT_TABLE      = T_ADSPEND   # funnel_lakehouse.dbo.ad_spend_facts (from Cell 1)
RNGA_CAMPAIGN  = "s | webserv | rehab & detox"

print(f"Ad Spend Facts starting: {RUN_TS}")

# --- authorize gspread with the service-account JSON from Key Vault ---
sa_json = notebookutils.credentials.getSecret(f"https://{AKV_NAME}.vault.azure.net/", SA_SECRET_NAME)
sa_info = json.loads(sa_json)
creds   = Credentials.from_service_account_info(
    sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
gc = gspread.authorize(creds)

sh = gc.open_by_key(SHEET_ID)
print(f"Opened sheet: {sh.title}")
print(f"Tabs present: {[ws.title for ws in sh.worksheets()]}")

# --- read each tab, union into one pandas frame ---
frames = []
for tab in TABS:
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        print(f"  tab not found, skipping: {tab}")
        continue
    records = ws.get_all_records()
    if not records:
        print(f"  tab empty, skipping: {tab}")
        continue
    df = pd.DataFrame(records)
    df["_tab"] = tab
    frames.append(df)
    print(f"  {tab}: {len(df):,} rows")

if not frames:
    raise RuntimeError("No spend rows read from any tab.")

pdf = pd.concat(frames, ignore_index=True)

for col in ["cost", "impressions", "clicks", "conversions", "conversions_value", "avg_cpc", "ctr"]:
    pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0.0)
pdf["campaign_id"] = pdf["campaign_id"].astype(str)
pdf["account_id"]  = pdf["account_id"].astype(str)

raw = spark.createDataFrame(pdf).withColumn("date", F.to_date("date"))
print(f"Total daily rows (all tabs): {raw.count():,}")

# --- map campaign + account_name -> facility + facility_group + channel ---
toks = F.split(F.lower(F.col("campaign_name")), r"[ \-|]+")
def has(t): return F.array_contains(toks, t)

acct = F.trim(F.col("account_name"))
camp_norm = F.lower(F.trim(F.regexp_replace(F.col("campaign_name"), r"\s+", " ")))

facility = (
    F.when(acct == "Longbranch Recovery & Wellness", F.lit("Longbranch"))
     .when(acct == "Eating Disorder Treatment Centers", F.lit("EDTC"))
     .when(acct == "Recover Now",
           F.when(camp_norm == RNGA_CAMPAIGN, F.lit("RNGA")).otherwise(F.lit("Recover Now")))
     .when(has("gaw"), F.lit("Green Acres Recovery"))
     .when(has("glr") | has("graceland"), F.lit("Graceland Recovery"))
     .when(has("crc") | has("chattanooga"), F.lit("Chattanooga Recovery Center"))
     .when(has("ltsw") | has("lw") | has("lotus"), F.lit("Lotus Wellness"))
     .when(has("ted") | has("br") | has("tides") | has("beaches"), F.lit("Tides Edge Recovery"))
     .when(acct == "GAW", F.lit("Green Acres Recovery"))
     .when(acct == "LW", F.lit("Lotus Wellness"))
     .otherwise(F.lit("Unmapped"))
)

mapped = raw.withColumn("facility", facility).withColumn(
    "facility_group",
    F.when(F.col("facility").isin(
        "Green Acres Recovery", "Graceland Recovery", "Chattanooga Recovery Center",
        "Lotus Wellness", "Tides Edge Recovery"), F.lit("Widespread"))
     .when(F.col("facility") == "RNGA", F.lit("Recover Now Georgia"))
     .when(F.col("facility") == "Longbranch", F.lit("Longbranch"))
     .when(F.col("facility") == "EDTC", F.lit("EDTC"))
     .when(F.col("facility") == "Recover Now", F.lit("Recover Now"))
     .otherwise(F.lit("Unmapped"))
).withColumn(
    "channel",
    F.when(has("pmax"), F.lit("Performance Max"))
     .when(has("s") | has("search"), F.lit("Search"))
     .when(has("b") | has("brand"), F.lit("Brand Search"))
     .when(has("display") | has("gdn"), F.lit("Display"))
     .when(has("video") | has("yt"), F.lit("Video"))
     .otherwise(F.lit("Other"))
)

unmapped = mapped.filter(F.col("facility") == "Unmapped").select("campaign_name", "account_name").distinct()
n_unmapped = unmapped.count()
if n_unmapped:
    print(f"WARNING {n_unmapped} campaign(s) did not map - review:")
    unmapped.show(50, truncate=False)
else:
    print("OK all campaigns mapped to a facility")

# --- roll daily -> weekly (week_ending = THURSDAY; Friday-to-Thursday week) ---
weekly = mapped.withColumn(
    "week_ending", F.expr("date_add(date, (5 - dayofweek(date) + 7) % 7)")
).groupBy(
    "week_ending", "facility", "facility_group", "channel",
    "campaign_id", "campaign_name", "account_id", "account_name"
).agg(
    F.sum("cost").alias("cost"),
    F.sum("impressions").alias("impressions"),
    F.sum("clicks").alias("clicks"),
    F.sum("conversions").alias("conversions"),
    F.sum("conversions_value").alias("conversions_value"),
)

weekly = weekly.withColumn(
    "ctr", F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions")).otherwise(F.lit(0.0))
).withColumn(
    "avg_cpc", F.when(F.col("clicks") > 0, F.col("cost") / F.col("clicks")).otherwise(F.lit(0.0))
).withColumn(
    "roas", F.when(F.col("cost") > 0, F.col("conversions_value") / F.col("cost")).otherwise(F.lit(0.0))
).withColumn(
    "pulled_at", F.lit(RUN_TS).cast("timestamp")
)
print(f"Weekly rows this run: {weekly.count():,}")

# --- ONE-TIME rebuild: drop stale Sunday-week rows so only Thursday weeks remain.
#     The Google Sheet holds full daily history, so this rebuilds every week cleanly.
#     Set REBUILD_ADSPEND_WEEKLY = False after the first successful run. ---
REBUILD_ADSPEND_WEEKLY = True
if REBUILD_ADSPEND_WEEKLY:
    spark.sql(f"DROP TABLE IF EXISTS {OUT_TABLE}")
    print("REBUILD: dropped existing ad_spend_facts for clean Thursday-week rebuild")

# --- upsert: union with existing history, dedup keeping newest pull ---
key_cols = ["week_ending", "campaign_id", "account_id"]
try:
    existing = spark.read.table(OUT_TABLE)
    combined = existing.unionByName(weekly, allowMissingColumns=True)
    print(f"Existing rows: {existing.count():,}  +  new: {weekly.count():,}")
except AnalysisException:
    combined = weekly
    print("No existing ad_spend_facts — first build.")

w = Window.partitionBy(*key_cols).orderBy(F.col("pulled_at").desc())
deduped = combined.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

print(f"Rows after dedup: {deduped.count():,}")
# canonicalize facility to short names
fac_map = {
    "Tides Edge Recovery": "Tides Edge", "Chattanooga Recovery Center": "Chattanooga",
    "Green Acres Recovery": "Green Acres", "Lotus Wellness": "Lotus",
    "Graceland Recovery": "Graceland", "Recover Now": "Recover Now (parent)",
}
_m = F.create_map([F.lit(x) for kv in fac_map.items() for x in kv])
deduped = deduped.withColumn("facility", F.coalesce(_m[F.col("facility")], F.col("facility")))
deduped.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(OUT_TABLE)
print(f"Wrote {OUT_TABLE}")
# ════════════════════════════════════════════════════════════════════════════
# MONTHLY rollup — same daily source as weekly, own table, own upsert.
# Rolled from `mapped` (daily) because weeks span months and can't be summed up.
# ════════════════════════════════════════════════════════════════════════════
OUT_TABLE_MONTHLY = f"{OUT_TABLE}_monthly"   # funnel_lakehouse.dbo.ad_spend_facts_monthly

monthly = mapped.withColumn(
    "month_start", F.date_trunc("month", F.col("date")).cast("date")
).groupBy(
    "month_start", "facility", "facility_group", "channel",
    "campaign_id", "campaign_name", "account_id", "account_name"
).agg(
    F.sum("cost").alias("cost"),
    F.sum("impressions").alias("impressions"),
    F.sum("clicks").alias("clicks"),
    F.sum("conversions").alias("conversions"),
    F.sum("conversions_value").alias("conversions_value"),
)

monthly = monthly.withColumn(
    "ctr", F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions")).otherwise(F.lit(0.0))
).withColumn(
    "avg_cpc", F.when(F.col("clicks") > 0, F.col("cost") / F.col("clicks")).otherwise(F.lit(0.0))
).withColumn(
    "roas", F.when(F.col("cost") > 0, F.col("conversions_value") / F.col("cost")).otherwise(F.lit(0.0))
).withColumn(
    "pulled_at", F.lit(RUN_TS).cast("timestamp")
)
print(f"Monthly rows this run: {monthly.count():,}")

# --- upsert: union with existing, dedup keeping newest pull ---
key_cols_m = ["month_start", "campaign_id", "account_id"]
try:
    existing_m = spark.read.table(OUT_TABLE_MONTHLY)
    combined_m = existing_m.unionByName(monthly, allowMissingColumns=True)
    print(f"Existing monthly rows: {existing_m.count():,}  +  new: {monthly.count():,}")
except AnalysisException:
    combined_m = monthly
    print("No existing ad_spend_facts_monthly — first build.")

w_m = Window.partitionBy(*key_cols_m).orderBy(F.col("pulled_at").desc())
deduped_m = combined_m.withColumn("_rn", F.row_number().over(w_m)).filter("_rn = 1").drop("_rn")

# canonicalize facility to short names (same map as weekly)
deduped_m = deduped_m.withColumn("facility", F.coalesce(_m[F.col("facility")], F.col("facility")))
deduped_m.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(OUT_TABLE_MONTHLY)
print(f"Wrote {OUT_TABLE_MONTHLY}")

# --- summary ---
am = spark.read.table(OUT_TABLE_MONTHLY)
print("=" * 70)
print(f"ad_spend_facts_monthly: {am.count():,} monthly rows")
print("=" * 70)
am.groupBy("month_start").agg(
    F.round(F.sum("cost"), 0).alias("total_cost"),
    F.round(F.sum("conversions_value"), 0).alias("total_conv_value"),
).orderBy("month_start").show(36, truncate=False)
# --- summary ---
af = spark.read.table(OUT_TABLE)
print("=" * 70)
print(f"ad_spend_facts: {af.count():,} weekly rows")
print("=" * 70)
print("Total spend by facility_group:")
af.groupBy("facility_group").agg(
    F.round(F.sum("cost"), 0).alias("total_cost"),
    F.round(F.sum("conversions_value"), 0).alias("total_conv_value"),
).orderBy(F.col("total_cost").desc()).show(truncate=False)
print("Spend by facility × channel:")
af.groupBy("facility", "channel").agg(
    F.round(F.sum("cost"), 0).alias("cost"),
    F.round(F.avg("ctr"), 4).alias("avg_ctr"),
    F.round(F.avg("roas"), 2).alias("avg_roas"),
).orderBy(F.col("cost").desc()).show(40, truncate=False)
print(f"Build complete: {datetime.now(timezone.utc).isoformat()}")3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "funnel_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build Dazos Leads Weekly
#
# Company-wide weekly Dazos lead count, for cost-per-lead.
#
# Dazos `leads` is pulled to bronze (180-day rolling snapshot) but is NOT in
# the funnel: leads and intake_opportunity are separate Dazos modules with no
# reliable linkage (~2.4% phone overlap; is_converted/parentid/sf_id empty), so
# the funnel starts at IntakeOpportunity. This table is a standalone top-of-
# funnel volume metric only.
#
# No facility: Dazos leads carry no facility/program/location field (facility
# isn't assigned until qualification), so this is COMPANY-WIDE only. Pair with
# total Dazos ad spend per week for cost-per-lead. Dazos-only (Widespread +
# RNGA); Longbranch/EDTC equivalent = Zoho inquiries in canonical_opportunities.
#
# Reads:  abfss://dazos-bronze@stkipu001.dfs.core.windows.net/leads/*/*/page_*.json
# Writes: dazos_leads_weekly  (one row per week_ending)
# Schedule: 09:15 UTC (after dazos-puller; independent of the funnel chain).


from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

BRONZE = "abfss://dazos-bronze@stkipu001.dfs.core.windows.net/leads/*/*/page_*.json"
OUT = T_LEADS_WK
CREATED_FMT  = "MM-dd-yyyy h:mm a"
MODIFIED_FMT = "MM-dd-yyyy h:mm a"
RUN_TS = datetime.now(timezone.utc).isoformat()
print(f"Build_Dazos_Leads_Weekly starting: {RUN_TS}")

# ============================================================================
# CELL 1
# ============================================================================

raw = spark.read.option("multiline","true").json(BRONZE)
leads = raw.select(F.explode("result.data").alias("rec")).select("rec.*")
leads = leads.toDF(*[c.lower() for c in leads.columns])

# Dedup stacked snapshots -> one row per lead id (latest modified wins)
w = Window.partitionBy("id").orderBy(F.to_timestamp("modified time", MODIFIED_FMT).desc_nulls_last())
leads_dedup = (leads.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn"))

n_distinct = leads_dedup.count()
print(f"Distinct Dazos leads: {n_distinct:,}")

weekly = (leads_dedup
    .withColumn("created", F.to_date(F.to_timestamp("created time", CREATED_FMT)))
    .filter(F.col("created").isNotNull())
    .withColumn("week_ending", F.to_date(F.date_add(F.date_trunc("week", F.col("created")), 6)))
    .groupBy("week_ending")
    .agg(F.count("*").alias("leads"))
    .withColumn("source_crm", F.lit("dazos"))
    .withColumn("build_run_ts", F.lit(RUN_TS).cast("timestamp"))
    .select("week_ending","source_crm","leads","build_run_ts"))

print(f"{OUT} rows (weeks): {weekly.count():,}")
weekly.write.mode("overwrite").format("delta").option("overwriteSchema","true").saveAsTable(OUT)
print(f"Wrote {OUT}")

# ============================================================================
# CELL 2
# ============================================================================

t = spark.read.table(OUT)
print("Recent weekly lead volume (2025-12 onward):")
t.filter(F.col("week_ending") >= F.lit("2025-12-01")).orderBy("week_ending").show(40)
print(f"Total leads (all weeks): {t.agg(F.sum('leads')).first()[0]:,}")
print(f"Build complete: {datetime.now(timezone.utc).isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Build Dimensions — dim_date + dim_facility
#
# Creates the two shared dimension tables the Power BI Direct Lake model needs
# so every fact table (admit_facts, ad_spend_facts, calls_facts, funnel_facts,
# canonical_opportunities, dazos_leads_weekly) can be filtered by ONE facility
# slicer and ONE date slicer at once.
#
#   dim_date     — one row per calendar day, with week_ending for weekly facts
#   dim_facility — one row per canonical facility (the 8 RN facilities)
#
# Writes to funnel_lakehouse.dbo. Run once; re-run only to extend the date range
# or change the facility list. Relationships are wired later in the semantic model.

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from datetime import date

# NOTE: this notebook's DEFAULT lakehouse is funnel_lakehouse, so we write/read with
# BARE table names (saveAsTable("dim_date")). A two-part name like "funnel_lakehouse.dim_date"
# gets mis-parsed as catalog.schema and fails (SCHEMA_NOT_FOUND). Bare names resolve against
# the pinned default lakehouse, which is exactly funnel_lakehouse.

# ════════ dim_date ════════
# Daily grain. week_ending = the Sunday ending that week (matches the weekly
# facts, which we key on week-ending Sunday). Adjust START/END as needed.
START = "2024-01-01"
END   = "2026-12-31"

d = (spark.sql(f"SELECT explode(sequence(to_date('{START}'), to_date('{END}'), interval 1 day)) AS date_key"))
dim_date = (d
    .withColumn("year",        F.year("date_key"))
    .withColumn("month_num",   F.month("date_key"))
    .withColumn("month_name",  F.date_format("date_key","MMMM"))
    .withColumn("year_month",  F.date_format("date_key","yyyy-MM"))
    .withColumn("quarter",     F.concat(F.lit("Q"), F.quarter("date_key")))
    .withColumn("day_of_week", F.date_format("date_key","EEEE"))
    .withColumn("dow_num",     F.dayofweek("date_key"))            # 1=Sun..7=Sat
    # week_ending = next THURSDAY (or same day if already Thursday), to match the
    # funnel's Friday-to-Thursday weeks. Thursday = 5 in Spark dayofweek (Sun=1..Sat=7).
    .withColumn("week_ending", F.expr("date_add(date_key, (5 - dayofweek(date_key) + 7) % 7)"))
    .withColumn("is_weekend",  F.col("dow_num").isin(1,7)))
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_date")
print(f"dim_date: {dim_date.count():,} rows  ({START} .. {END})")

# ════════ dim_facility ════════
# Canonical facility list. The 'facility' column is the exact string your fact
# tables carry (must match facility_resolved / facility values in the facts).
# entity / region are optional descriptive columns for grouping in Power BI.
facilities = [
    ("Longbranch",          "Longbranch Wellness",        "LA"),
    ("EDTC",                "Eating Disorder TC",         "LA"),
    ("RNGA",                "Recover Now Greater Atlanta","GA"),
    ("Tides Edge",          "Tides Edge Recovery",        "FL"),
    ("Lotus",               "Lotus Wellness",             "TN"),
    ("Chattanooga",         "Chattanooga Recovery Center","TN"),
    ("Graceland",           "Graceland Recovery",         "TN"),
    ("Green Acres",         "Green Acres Recovery",       "GA"),
    ("RNCA",                "Recover Now Central Alabama","AL"),
    # --- catch-all members: keep unmatched fact values slice-able, not blank ---
    ("Recover Now (parent)", "Recover Now",   "Rollup"),
    ("Unassigned",           "Unassigned",     "Unmapped"),
    ("Unmapped",             "Unmapped",       "Unmapped"),
    ("Other",                "Non-Facility",   "Non-Facility"),
    ("ChatBot",              "Non-Facility",   "Non-Facility"),
    ("Outbound Calls",       "Non-Facility",   "Non-Facility"),
    ("Widespread",           "Non-Facility",   "Non-Facility"),
]
schema = StructType([
    StructField("facility",  StringType(), False),
    StructField("entity",    StringType(), True),
    StructField("region",    StringType(), True),
])
dim_facility = spark.createDataFrame(facilities, schema)
dim_facility.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_facility")
print(f"dim_facility: {dim_facility.count()} rows")
dim_facility.show(truncate=False)

# ════════ sanity: do the fact facility values match dim_facility? ════════
# (Read-only check; helps catch a fact carrying a name not in the dimension,
#  which would silently drop rows when filtering by facility in Power BI.)
print("\n--- facility value check vs facts ---")
for tbl, col in [("admit_facts","facility"), ("ad_spend_facts","facility"),
                 ("calls_facts","facility"), ("funnel_facts","facility")]:
    try:
        fv = spark.read.table(tbl).select(F.col(col).alias("facility")).distinct()
        unmatched = fv.join(dim_facility, "facility", "left_anti").collect()
        names = sorted([r["facility"] for r in unmatched if r["facility"] is not None])
        print(f"{tbl}.{col}: {'all match' if not names else 'NOT in dim_facility -> ' + str(names)}")
    except Exception as e:
        print(f"{tbl}: (skipped: {e})")

print("\nDone. Next: build the Direct Lake semantic model and wire relationships.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": { "name": "synapse_pyspark" },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "funnel_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build Admissions Funnel (v18 - lead_type everywhere, channel dropped)
#
# TWO funnel shapes, matched to what each CRM actually tracks:
#   Zoho  (Longbranch, EDTC):          Leads -> Qualified Leads -> VoBs -> Admits
#   Dazos (Widespread, RNGA, RNCA):    Leads -> VoBs -> Viable VoBs -> Admits
#
# Grouped by: facility, lead_type, opener, closer.
# lead_type buckets (ONE consistent label across all stages):
#   BD / SEO / PPC / Alumni / Step Down / Other
#
# lead_type per stage (different fields per source, SAME buckets):
#   Dazos leads   -> derived from BD_Rep + Lead_Source
#   Zoho leads    -> BD-first rule (Referring_Company/BD_Contact_Owner -> BD, etc.)
#   VoBs (both)   -> funnel_facts.lead_type (already present)
#   Admits        -> verified_admits.lead_type (file/derived, already correct)
#
# Calls + unique callers are a SEPARATE side metric, NOT a funnel stage.
# (calls have no lead_type, so the calls side metric stays grouped by facility only.)
#
# Writes:
#   admissions_funnel_dazos   (Leads->VoBs->Viable->Admits)
#   admissions_funnel_zoho    (Leads->Qualified->VoBs->Admits)
#   calls_detail              (one row per CTM event: voice/sms/form/chat)

from pyspark.sql import functions as F
from datetime import date, timedelta
from delta.tables import DeltaTable

# ---- History config -------------------------------------------------------
# HISTORY_START: hard floor; nothing before this ever enters history.
# BACKFILL: set True for the FIRST run to (re)build all weeks from HISTORY_START.
#           set False for normal weekly runs (only recompute the recent window).
HISTORY_START = date(2026, 5, 1)
BACKFILL = False          # <-- set to False after the first successful backfill run
WEEKS_BACK = 12           # normal-run window (recent weeks that stay correctable)

today = date.today()
# most recent Thursday (week-ending day). Python weekday(): Mon=0..Sun=6, Thu=3.
last_thursday = today - timedelta(days=(today.weekday() - 3) % 7)

if BACKFILL:
    # cover everything from HISTORY_START through the latest complete week
    window_start = HISTORY_START
else:
    window_start = last_thursday - timedelta(weeks=WEEKS_BACK - 1)
    # never go below the floor
    if window_start < HISTORY_START:
        window_start = HISTORY_START

print(f"Funnel window: weeks ending {window_start} -> {last_thursday}  (BACKFILL={BACKFILL})")

# merge a recomputed window into a permanent history table.
# Overwrite semantics: weeks in the window get replaced with fresh numbers;
# weeks outside the window (older, already frozen) are left untouched.
# Floor enforced: rows before HISTORY_START are dropped.
MERGE_KEYS = ["week_ending", "location", "lead_type", "opener", "closer"]

def merge_history(new_df, table_name):
    new_df = new_df.filter(F.col("week_ending") >= F.lit(HISTORY_START))
    if not spark.catalog.tableExists(table_name):
        new_df.write.format("delta").saveAsTable(table_name)
        print(f"  created {table_name}")
        return
    tgt = DeltaTable.forName(spark, table_name)
    cond = " AND ".join([f"t.{k} <=> s.{k}" for k in MERGE_KEYS])
    (tgt.alias("t").merge(new_df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"  merged into {table_name}")

def week_ending(col):
    # Friday-to-Thursday weeks (matches the verified file cadence).
    # Thursday is dayofweek=5 in Spark (Sun=1..Sat=7). For any date, the
    # week-ending Thursday is the next Thursday on/after that date.
    # days to add to reach Thursday: (5 - dayofweek + 7) % 7
    return F.date_add(col, F.pmod(F.lit(5) - F.dayofweek(col) + F.lit(7), F.lit(7)))

# ============================================================================
# ONE shared lead_type rule -> BD / SEO / PPC / Alumni / Step Down / Other
# Reads whatever signal columns a stage has. BD-first (matches verified admits).
#   bd_signal      : a referring company OR bd rep/contact is present
#   text columns   : lead source / campaign / referral / marketing channel text
# ============================================================================
def lead_type_of(bd_signal_col, *text_cols):
    blob = F.lower(F.concat_ws(" ", *[F.coalesce(c, F.lit("")) for c in text_cols]))
    return (
        # 1. BD wins: referring company / bd rep present, or BD-ish text
        F.when(bd_signal_col, F.lit("BD"))
         .when(blob.rlike(r"transfer|referral|sales rep|business development|3rd party"), F.lit("BD"))
        # 2. Alumni
         .when(blob.rlike(r"alumni|word of mouth|readmission"), F.lit("Alumni"))
        # 3. PPC (paid)
         .when(blob.rlike(r"google ads|ppc|pmax|p max|paid|meta|fb ads|facebook"), F.lit("PPC"))
        # 4. SEO (organic / internet)
         .when(blob.rlike(r"organic|seo|gmb|local listing|director|website|internet search|direct site|brand|web|psychology today|tawk|multi-ai"), F.lit("SEO"))
        # 5. Step Down (internal level-of-care move)
         .when(blob.rlike(r"step down|step-down"), F.lit("Step Down"))
         .otherwise(F.lit("Other"))
    )

# normalize an existing lead_type value to the canonical buckets (for funnel_facts
# / verified_admits which already carry a lead_type string)
def norm_lead_type(c):
    s = F.lower(F.trim(F.coalesce(c, F.lit(""))))
    return (F.when(s == "bd", F.lit("BD"))
             .when(s == "seo", F.lit("SEO"))
             .when(s == "ppc", F.lit("PPC"))
             .when(s == "alumni", F.lit("Alumni"))
             .when(s.rlike(r"step.?down"), F.lit("Step Down"))
             .when(s == "", F.lit("Other"))
             .otherwise(F.lit("Other")))

# ---- facility normalizer (long -> short) ----
fac_map = F.create_map(
    F.lit("Green Acres Wellness"), F.lit("Green Acres"),
    F.lit("Chattanooga Recovery Center"), F.lit("Chattanooga"),
    F.lit("Lotus Wellness"), F.lit("Lotus"),
    F.lit("Tides Edge Recovery"), F.lit("Tides Edge"),
    F.lit("Graceland Recovery"), F.lit("Graceland"),
    F.lit("Recover Now Greater Atlanta"), F.lit("RNGA"),
    F.lit("Recover Now Central Alabama"), F.lit("RNCA"),
)
def norm_fac(c):
    return F.coalesce(fac_map[c], c)

# Parse a clean facility out of a Dazos Campaign_Source string (which mixes real
# facility names with campaign codes like "PPC CRC - Pmax", "GLR SEO"). Keyword
# match to a canonical facility; "Unassigned" when there is no facility signal
# (e.g. "Outbound Calls", "Facebook", "--"). Same code set as the ad-spend mapper.
def facility_from_campaign(col):
    s = F.lower(F.coalesce(col, F.lit("")))
    return (F.when(s.rlike(r"\bted\b|tides|beaches|\bbr\b"), F.lit("Tides Edge"))
             .when(s.rlike(r"\bcrc\b|chattanooga"), F.lit("Chattanooga"))
             .when(s.rlike(r"\bglr\b|graceland"), F.lit("Graceland"))
             .when(s.rlike(r"\bgaw\b|green acres"), F.lit("Green Acres"))
             .when(s.rlike(r"\blw\b|lotus|ltsw"), F.lit("Lotus"))
             .when(s.rlike(r"\brnga\b|greater atlanta|recover now georgia"), F.lit("RNGA"))
             .when(s.rlike(r"\brnca\b|central alabama"), F.lit("RNCA"))
             .when(s.rlike(r"longbranch|\blb\b"), F.lit("Longbranch"))
             .when(s.rlike(r"edtc|eating disorder"), F.lit("EDTC"))
             .otherwise(F.lit("Unassigned")))

DAZOS_FACS = ["Green Acres","Chattanooga","Lotus","Tides Edge","Graceland","RNGA","RNCA"]
ZOHO_FACS  = ["Longbranch","EDTC"]

def _has(col):
    return col.isNotNull() & (F.trim(col) != "") & (F.trim(col) != "--")

# ============================================================================
# LEADS — lead_type derived per CRM
# ============================================================================
# Dazos leads: BD signal = BD_Rep present; text = Lead_Source + Campaign_Source +
# Referral_Source___Hear_About_Us
dz_leads = (spark.table("dazos_lakehouse.dbo.leads_current")
    .withColumn("week_ending", week_ending(F.to_date(F.to_timestamp(F.col("Created_Time"), "MM-dd-yyyy h:mm a"))))
    .withColumn("lead_type", lead_type_of(
        F.lit(False),                                  # Dazos: no bd_rep signal (junk-filled); text only
        F.col("Lead_Source"), F.col("Campaign_Source"),
        F.col("Referral_Source___Hear_About_Us")))
    .withColumn("opener", F.coalesce(F.col("Opener"), F.lit("Unknown")))
    .withColumn("closer", F.coalesce(F.col("Closer"), F.lit("Unknown")))
    .withColumn("location", facility_from_campaign(F.col("Campaign_Source")))
    .filter((F.col("week_ending") >= F.lit(window_start)) & (F.col("week_ending") <= F.lit(last_thursday))))

# Zoho leads: BD signal = Referring_Company OR BD_Contact_Owner present;
# text = Marketing_Channel + Referral_Source + Lead_Type + Source
zh_leads = (spark.table("zoho_lakehouse.dbo.zoho_leads_current")
    .withColumn("week_ending", week_ending(F.to_date(F.to_timestamp(F.col("Created_Time")))))
    .withColumn("lead_type", lead_type_of(
        _has(F.col("Referring_Company")) | _has(F.col("BD_Contact_Owner")),
        F.col("Marketing_Channel"), F.col("Referral_Source"),
        F.col("Lead_Type"), F.col("Source")))
    .withColumn("opener", F.coalesce(F.col("Owner"), F.lit("Unknown")))
    .withColumn("closer", F.lit("Unknown"))   # Zoho has no closer concept on leads
    .withColumn("is_qualified", F.lower(F.trim(F.coalesce(F.col("Inquiry"), F.lit("")))) == "qualified")
    .withColumn("location",
        F.when(F.lower(F.coalesce(F.col("Location"), F.lit(""))).contains("edtc") |
               F.lower(F.coalesce(F.col("Facility_Admit_to"), F.lit(""))).contains("eating disorder"),
               F.lit("EDTC")).otherwise(F.lit("Longbranch")))
    .filter((F.col("week_ending") >= F.lit(window_start)) & (F.col("week_ending") <= F.lit(last_thursday))))

# ============================================================================
# VoB stage — funnel_facts ALREADY carries lead_type (use directly, normalized)
# ============================================================================
ff = (spark.table("funnel_lakehouse.dbo.funnel_facts")
    .withColumn("week_ending", week_ending(F.to_date(F.col("created_at"))))
    .withColumn("location", norm_fac(F.col("treatment_program")))
    # lead_type derived with the SAME BD-first rule as leads/admits, using the
    # referral fields now carried through canonical. Zoho's native lead_type is a
    # contact method (Phone Call/Live Chat) so we do NOT use it here.
    .withColumn("lead_type",
        # Dazos: use the real Lead_Type column (89% filled, clean buckets) carried
        #        through canonical/funnel_facts. blank -> Other. Consistent with the
        #        Opportunities stage which uses the same column.
        # Zoho:  Lead_Type is a junk contact-method field, so derive via BD-first text rule.
        F.when(F.col("source_crm") == "dazos", norm_lead_type(F.col("lead_type")))
         .otherwise(lead_type_of(
             _has(F.col("referring_company")) | _has(F.col("bd_rep")),
             F.col("marketing_channel"), F.col("referral_source"),
             F.col("lead_source"), F.col("first_call_source"))))
    .withColumn("opener", F.coalesce(F.col("opener"), F.lit("Unknown")))
    .withColumn("closer", F.coalesce(F.col("closer"), F.lit("Unknown")))
    .withColumn("is_vob",
        F.when(F.col("source_crm") == "zoho",
               F.col("member_id").isNotNull() & (F.trim(F.col("member_id")) != ""))
         .otherwise(F.col("has_vob")))
    .filter((F.col("week_ending") >= F.lit(window_start)) & (F.col("week_ending") <= F.lit(last_thursday))))

GROUP = ["week_ending","location","lead_type","opener","closer"]

# ============================================================================
# ADMITS — verified_admits.lead_type (already correct: file or derived)
# admission_rep -> closer ; opener blank ("Unknown") on admit rows.
# ============================================================================
va = (spark.table("funnel_lakehouse.dbo.verified_admits")
    .withColumn("week_ending", week_ending(F.col("admit_date")))
    .withColumn("location", norm_fac(F.col("facility")))
    .withColumn("lead_type", norm_lead_type(F.col("lead_type")))
    .withColumn("opener", F.lit("Unknown"))
    .withColumn("closer", F.coalesce(F.col("admission_rep"), F.lit("Unknown")))
    .filter((F.col("week_ending") >= F.lit(window_start)) & (F.col("week_ending") <= F.lit(last_thursday))))

va_dazos = (va.filter(F.col("source_crm") == "dazos")
    .groupBy(*GROUP).agg(F.count("*").alias("admits")))
va_zoho = (va.filter(F.col("source_crm") == "zoho")
    .groupBy(*GROUP).agg(F.count("*").alias("admits")))

# ============================================================================
# DAZOS FUNNEL: Leads -> VoBs -> Viable VoBs -> Admits
# ============================================================================
dz_lead_agg = dz_leads.groupBy(*GROUP).agg(F.count("*").alias("leads"))

# Dazos OPPORTUNITIES stage - every opportunity by created date, Lead_Type column.
dz_opps = (spark.table("dazos_lakehouse.dbo.intake_opportunity_current")
    .withColumn("week_ending", week_ending(F.to_date(F.to_timestamp(F.col("Created_Time"), "MM-dd-yyyy h:mm a"))))
    .withColumn("location", norm_fac(F.col("Treatment_Program")))
    .withColumn("lead_type", norm_lead_type(F.col("Lead_Type")))
    .withColumn("opener", F.coalesce(F.col("Opener"), F.lit("Unknown")))
    .withColumn("closer", F.coalesce(F.col("Closer"), F.lit("Unknown")))
    .filter((F.col("week_ending") >= F.lit(window_start)) & (F.col("week_ending") <= F.lit(last_thursday)))
    .groupBy(*GROUP).agg(F.count("*").alias("opportunities")))

dz_stage = (ff.filter(F.col("source_crm") == "dazos").groupBy(*GROUP)
    .agg(
        F.sum(F.col("is_vob").cast("int")).alias("vobs"),
        F.sum(F.col("is_viable_vob_flag").cast("int")).alias("viable_vobs"),
    ))

dazos_funnel = (dz_lead_agg
    .join(dz_opps, GROUP, "full_outer")
    .join(dz_stage, GROUP, "full_outer")
    .join(va_dazos, GROUP, "full_outer")
    .select(*GROUP,
        F.coalesce("leads", F.lit(0)).alias("leads"),
        F.coalesce("opportunities", F.lit(0)).alias("opportunities"),
        F.coalesce("vobs", F.lit(0)).alias("vobs"),
        F.coalesce("viable_vobs", F.lit(0)).alias("viable_vobs"),
        F.coalesce("admits", F.lit(0)).alias("admits"))
    .orderBy("week_ending","location","lead_type"))

merge_history(dazos_funnel, "funnel_lakehouse.dbo.admissions_funnel_dazos_history")
print("Merged admissions_funnel_dazos_history")

# ============================================================================
# ZOHO FUNNEL: Leads -> Qualified Leads -> VoBs -> Admits
# ============================================================================
zh_lead_agg = zh_leads.groupBy(*GROUP).agg(
    F.count("*").alias("leads"),
    F.sum(F.col("is_qualified").cast("int")).alias("qualified_leads"))

zh_stage = (ff.filter(F.col("source_crm") == "zoho").groupBy(*GROUP)
    .agg(
        F.sum(F.col("is_vob").cast("int")).alias("vobs"),
    ))

zoho_funnel = (zh_lead_agg
    .join(zh_stage, GROUP, "full_outer")
    .join(va_zoho, GROUP, "full_outer")
    .select(*GROUP,
        F.coalesce("leads", F.lit(0)).alias("leads"),
        F.coalesce("qualified_leads", F.lit(0)).alias("qualified_leads"),
        F.coalesce("vobs", F.lit(0)).alias("vobs"),
        F.coalesce("admits", F.lit(0)).alias("admits"))
    .orderBy("week_ending","location","lead_type"))

merge_history(zoho_funnel, "funnel_lakehouse.dbo.admissions_funnel_zoho_history")
print("Merged admissions_funnel_zoho_history")

# ============================================================================
# CALLS — DETAILED table, one row per event (voice/sms/form_fill/chat).
# Reads ctm_calls_raw (already has agent identity normalized, outcome, channel
# inputs). Friendly-named columns + derived Day / Week Ending / Month Ending /
# Tracking Source Type (channel). Full table (not windowed) so Power BI has all
# history; filter by date there.
# ============================================================================
def classify_channel_expr(src):
    s = F.regexp_replace(F.lower(F.trim(F.coalesce(src, F.lit("")))), r"\s+", " ")
    return (F.when(s == "outbound calls", F.lit("Outbound"))
             .when(s.rlike(r"ai agent|tawk to"), F.lit("Chat"))
             .when(s.rlike(r"billing number|metro atlanta treatment|3rd mh|pre assessments|other static") | (s == "mat"), F.lit("Other"))
             .when(s.rlike(r"^ppc |^ppc-|^meta |google ads|facebook paid|facebook cta|fb ads|geofencing|linkedin|pmax|performance max"), F.lit("PPC"))
             .when(s == "print", F.lit("Other"))
             .when(s.rlike(r"organic|seo|gmb|bing local"), F.lit("Organic"))
             .when(s.rlike(r"referral|facility transfer"), F.lit("Referral"))
             .when(s.rlike(r"direct|target number|website"), F.lit("Direct"))
             .when(s.isin("eating disorder treatment centers","longbranch recovery",
                          "recover now greater atlanta","chattanooga recovery center",
                          "graceland recovery","green acres wellness","lotus wellness",
                          "tides edge detox","beaches recovery","widespread wellness","recover now"), F.lit("Direct"))
             .otherwise(F.lit("Other")))

calls_detail = (spark.table("ctm_lakehouse.dbo.ctm_calls_raw")
    .withColumn("day", F.col("call_date"))
    .withColumn("week_ending", week_ending(F.col("call_date")))
    .withColumn("month_ending", F.last_day(F.col("call_date")))
    .withColumn("location", norm_fac(F.col("facility_resolved")))
    .withColumn("tracking_source_type", classify_channel_expr(F.col("source")))
    .select(
        F.col("id").alias("call_id"),
        "day", "week_ending", "month_ending",
        "location",
        F.col("event_type").alias("type"),               # voice / sms / form_fill / chat
        "direction",                                     # inbound / outbound / msg_* / form
        F.col("outcome").alias("status"),                # answered / missed / voicemail / ...
        F.col("status").alias("raw_status"),             # answered / no answer / delivered ...
        F.col("agent_name").alias("rep"),
        F.col("agent_email").alias("rep_email"),
        F.col("source").alias("tracking_source"),
        "tracking_source_type",
        F.col("tracking_label").alias("tracking_label"),
        F.col("caller_number").alias("caller_number"),
        "is_new_caller",
        F.col("talk_time").alias("talk_time_sec"),
        F.col("ring_time").alias("ring_time_sec"),
        F.col("duration").alias("duration_sec"),
        "is_inbound_missed", "needs_callback",
        "callback_status", "callback_minutes", "callback_connected",
        "city", "state",
        F.col("called_at").alias("called_at"),
    ))

calls_detail.write.mode("overwrite").format("delta").option("overwriteSchema","true") \
    .saveAsTable("funnel_lakehouse.dbo.calls_detail")
print("Wrote calls_detail (one row per event)")

# ============================================================================
# QUICK LOOK — now by lead_type
# ============================================================================
print("\n=== DAZOS funnel - lead_type rollup (window) ===")
(dazos_funnel.groupBy("lead_type").agg(
    F.sum("leads").alias("leads"), F.sum("opportunities").alias("opps"),
    F.sum("vobs").alias("vobs"),
    F.sum("viable_vobs").alias("viable"), F.sum("admits").alias("admits"))
 .orderBy(F.col("admits").desc()).show(truncate=False))

print("=== ZOHO funnel - lead_type rollup (window) ===")
(zoho_funnel.groupBy("lead_type").agg(
    F.sum("leads").alias("leads"), F.sum("qualified_leads").alias("qualified"),
    F.sum("vobs").alias("vobs"), F.sum("admits").alias("admits"))
 .orderBy(F.col("admits").desc()).show(truncate=False))

print("=== CALLS detail - type x direction (all history) ===")
(calls_detail.groupBy("type","direction").count()
 .orderBy(F.col("count").desc()).show(truncate=False))
print("=== CALLS detail - by rep (voice, answered, top 15) ===")
(calls_detail.filter((F.col("type")=="voice") & (F.col("status")=="answered"))
 .groupBy("rep").count().orderBy(F.col("count").desc()).show(15, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": { "name": "synapse_pyspark" },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "funnel_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Monthly rollups (TRUE calendar month, from raw record dates)
#
# Unlike the weekly tables (which bucket by week_ending Thursday), the monthly
# tables group each record by the actual calendar month of its OWN date:
#   Leads        -> lead Created_Time month
#   VoBs/Viable  -> opportunity created_at month (no separate VoB date exists)
#   Admits       -> admit_date month  (exact)
#
# So a June 30 admit counts in June even if its week ends in July.
# Re-reads sources (not the weekly tables) so real dates are preserved.
# Month floored at HISTORY_START's month. Full overwrite each run.
#
# Writes:
#   admissions_funnel_dazos_monthly
#   admissions_funnel_zoho_monthly

from pyspark.sql import functions as F
from datetime import date

HISTORY_START = date(2026, 5, 1)
MONTH_FLOOR = F.lit(HISTORY_START.replace(day=1))

def month_of(col):
    return F.date_trunc("month", col).cast("date")

# reuse the same helpers/rules as the funnel cell by re-declaring the minimal bits
def week_unused():  # placeholder to keep imports tidy
    pass

# ---- facility normalizer (same as funnel cell) ----
fac_map = F.create_map(
    F.lit("Green Acres Wellness"), F.lit("Green Acres"),
    F.lit("Chattanooga Recovery Center"), F.lit("Chattanooga"),
    F.lit("Lotus Wellness"), F.lit("Lotus"),
    F.lit("Tides Edge Recovery"), F.lit("Tides Edge"),
    F.lit("Graceland Recovery"), F.lit("Graceland"),
    F.lit("Recover Now Greater Atlanta"), F.lit("RNGA"),
    F.lit("Recover Now Central Alabama"), F.lit("RNCA"),
)
def norm_fac(c):
    return F.coalesce(fac_map[c], c)

def facility_from_campaign(col):
    s = F.lower(F.coalesce(col, F.lit("")))
    return (F.when(s.rlike(r"\bted\b|tides|beaches|\bbr\b"), F.lit("Tides Edge"))
             .when(s.rlike(r"\bcrc\b|chattanooga"), F.lit("Chattanooga"))
             .when(s.rlike(r"\bglr\b|graceland"), F.lit("Graceland"))
             .when(s.rlike(r"\bgaw\b|green acres"), F.lit("Green Acres"))
             .when(s.rlike(r"\blw\b|lotus|ltsw"), F.lit("Lotus"))
             .when(s.rlike(r"\brnga\b|greater atlanta|recover now georgia"), F.lit("RNGA"))
             .when(s.rlike(r"\brnca\b|central alabama"), F.lit("RNCA"))
             .when(s.rlike(r"longbranch|\blb\b"), F.lit("Longbranch"))
             .when(s.rlike(r"edtc|eating disorder"), F.lit("EDTC"))
             .otherwise(F.lit("Unassigned")))

def _has(col):
    return col.isNotNull() & (F.trim(col) != "") & (F.trim(col) != "--")

def lead_type_of(bd_signal_col, *text_cols):
    blob = F.lower(F.concat_ws(" ", *[F.coalesce(c, F.lit("")) for c in text_cols]))
    return (F.when(bd_signal_col, F.lit("BD"))
             .when(blob.rlike(r"transfer|referral|sales rep|business development|3rd party"), F.lit("BD"))
             .when(blob.rlike(r"alumni|word of mouth|readmission"), F.lit("Alumni"))
             .when(blob.rlike(r"google ads|ppc|pmax|p max|paid|meta|fb ads|facebook"), F.lit("PPC"))
             .when(blob.rlike(r"organic|seo|gmb|local listing|director|website|internet search|direct site|brand|web|psychology today|tawk|multi-ai"), F.lit("SEO"))
             .when(blob.rlike(r"step down|step-down"), F.lit("Step Down"))
             .otherwise(F.lit("Other")))

def norm_lead_type(c):
    s = F.lower(F.trim(F.coalesce(c, F.lit(""))))
    return (F.when(s == "bd", F.lit("BD")).when(s == "seo", F.lit("SEO"))
             .when(s == "ppc", F.lit("PPC")).when(s == "alumni", F.lit("Alumni"))
             .when(s.rlike(r"step.?down"), F.lit("Step Down"))
             .otherwise(F.lit("Other")))

MGROUP = ["month", "location", "lead_type", "opener", "closer"]

# ============================================================================
# LEADS by true month
# ============================================================================
dz_leads = (spark.table("dazos_lakehouse.dbo.leads_current")
    .withColumn("month", month_of(F.to_timestamp(F.col("Created_Time"), "MM-dd-yyyy h:mm a")))
    .withColumn("lead_type", lead_type_of(F.lit(False),
        F.col("Lead_Source"), F.col("Campaign_Source"), F.col("Referral_Source___Hear_About_Us")))
    .withColumn("location", facility_from_campaign(F.col("Campaign_Source")))
    .withColumn("opener", F.coalesce(F.col("Opener"), F.lit("Unknown")))
    .withColumn("closer", F.coalesce(F.col("Closer"), F.lit("Unknown")))
    .filter(F.col("month") >= MONTH_FLOOR))

zh_leads = (spark.table("zoho_lakehouse.dbo.zoho_leads_current")
    .withColumn("month", month_of(F.to_timestamp(F.col("Created_Time"))))
    .withColumn("lead_type", lead_type_of(
        _has(F.col("Referring_Company")) | _has(F.col("BD_Contact_Owner")),
        F.col("Marketing_Channel"), F.col("Referral_Source"), F.col("Lead_Type"), F.col("Source")))
    .withColumn("is_qualified", F.lower(F.trim(F.coalesce(F.col("Inquiry"), F.lit("")))) == "qualified")
    .withColumn("opener", F.coalesce(F.col("Owner"), F.lit("Unknown")))
    .withColumn("closer", F.lit("Unknown"))
    .withColumn("location",
        F.when(F.lower(F.coalesce(F.col("Location"), F.lit(""))).contains("edtc") |
               F.lower(F.coalesce(F.col("Facility_Admit_to"), F.lit(""))).contains("eating disorder"),
               F.lit("EDTC")).otherwise(F.lit("Longbranch")))
    .filter(F.col("month") >= MONTH_FLOOR))

# ============================================================================
# VoB stage by true month (opportunity created_at)
# ============================================================================
ff = (spark.table("funnel_lakehouse.dbo.funnel_facts")
    .withColumn("month", month_of(F.to_date(F.col("created_at"))))
    .withColumn("location", norm_fac(F.col("treatment_program")))
    .withColumn("lead_type",
        F.when(F.col("source_crm") == "dazos", norm_lead_type(F.col("lead_type")))
         .otherwise(lead_type_of(
             _has(F.col("referring_company")) | _has(F.col("bd_rep")),
             F.col("marketing_channel"), F.col("referral_source"),
             F.col("lead_source"), F.col("first_call_source"))))
    .withColumn("opener", F.coalesce(F.col("opener"), F.lit("Unknown")))
    .withColumn("closer", F.coalesce(F.col("closer"), F.lit("Unknown")))
    .withColumn("is_vob",
        F.when(F.col("source_crm") == "zoho",
               F.col("member_id").isNotNull() & (F.trim(F.col("member_id")) != ""))
         .otherwise(F.col("has_vob")))
    .filter(F.col("month") >= MONTH_FLOOR))

# ============================================================================
# ADMITS by true month (admit_date - exact)
# ============================================================================
va = (spark.table("funnel_lakehouse.dbo.verified_admits")
    .withColumn("month", month_of(F.col("admit_date")))
    .withColumn("location", norm_fac(F.col("facility")))
    .withColumn("lead_type", norm_lead_type(F.col("lead_type")))
    .withColumn("opener", F.lit("Unknown"))
    .withColumn("closer", F.coalesce(F.col("admission_rep"), F.lit("Unknown")))
    .filter(F.col("month") >= MONTH_FLOOR))

# Dazos OPPORTUNITIES by true month (Lead_Type column)
dz_opps = (spark.table("dazos_lakehouse.dbo.intake_opportunity_current")
    .withColumn("month", month_of(F.to_timestamp(F.col("Created_Time"), "MM-dd-yyyy h:mm a")))
    .withColumn("location", norm_fac(F.col("Treatment_Program")))
    .withColumn("lead_type", norm_lead_type(F.col("Lead_Type")))
    .withColumn("opener", F.coalesce(F.col("Opener"), F.lit("Unknown")))
    .withColumn("closer", F.coalesce(F.col("Closer"), F.lit("Unknown")))
    .filter(F.col("month") >= MONTH_FLOOR))

# ============================================================================
# DAZOS monthly: leads / opportunities / vobs / viable / admits
# ============================================================================
dz_l = dz_leads.groupBy(*MGROUP).agg(F.count("*").alias("leads"))
dz_o = dz_opps.groupBy(*MGROUP).agg(F.count("*").alias("opportunities"))
dz_s = (ff.filter(F.col("source_crm")=="dazos").groupBy(*MGROUP)
    .agg(F.sum(F.col("is_vob").cast("int")).alias("vobs"),
         F.sum(F.col("is_viable_vob_flag").cast("int")).alias("viable_vobs")))
dz_a = (va.filter(F.col("source_crm")=="dazos").groupBy(*MGROUP).agg(F.count("*").alias("admits")))

dazos_monthly = (dz_l.join(dz_o, MGROUP, "full_outer").join(dz_s, MGROUP, "full_outer").join(dz_a, MGROUP, "full_outer")
    .select(*MGROUP,
        F.coalesce("leads", F.lit(0)).alias("leads"),
        F.coalesce("opportunities", F.lit(0)).alias("opportunities"),
        F.coalesce("vobs", F.lit(0)).alias("vobs"),
        F.coalesce("viable_vobs", F.lit(0)).alias("viable_vobs"),
        F.coalesce("admits", F.lit(0)).alias("admits"))
    .orderBy("month","location","lead_type"))

dazos_monthly = dazos_monthly.withColumnRenamed("month", "month_start")
(dazos_monthly.write.mode("overwrite").format("delta").option("overwriteSchema","true")
    .saveAsTable("funnel_lakehouse.dbo.admissions_funnel_dazos_monthly"))
print("Wrote admissions_funnel_dazos_monthly")

# ============================================================================
# ZOHO monthly: leads / qualified / vobs / admits
# ============================================================================
zh_l = zh_leads.groupBy(*MGROUP).agg(
    F.count("*").alias("leads"),
    F.sum(F.col("is_qualified").cast("int")).alias("qualified_leads"))
zh_s = (ff.filter(F.col("source_crm")=="zoho").groupBy(*MGROUP)
    .agg(F.sum(F.col("is_vob").cast("int")).alias("vobs")))
zh_a = (va.filter(F.col("source_crm")=="zoho").groupBy(*MGROUP).agg(F.count("*").alias("admits")))

zoho_monthly = (zh_l.join(zh_s, MGROUP, "full_outer").join(zh_a, MGROUP, "full_outer")
    .select(*MGROUP,
        F.coalesce("leads", F.lit(0)).alias("leads"),
        F.coalesce("qualified_leads", F.lit(0)).alias("qualified_leads"),
        F.coalesce("vobs", F.lit(0)).alias("vobs"),
        F.coalesce("admits", F.lit(0)).alias("admits"))
    .orderBy("month","location","lead_type"))

zoho_monthly = zoho_monthly.withColumnRenamed("month", "month_start")
(zoho_monthly.write.mode("overwrite").format("delta").option("overwriteSchema","true")
    .saveAsTable("funnel_lakehouse.dbo.admissions_funnel_zoho_monthly"))
print("Wrote admissions_funnel_zoho_monthly")

# ---- quick look ----
print("\n=== Dazos monthly (month x lead_type) ===")
(dazos_monthly.groupBy("month_start","lead_type")
    .agg(F.sum("leads").alias("leads"), F.sum("opportunities").alias("opps"),
         F.sum("vobs").alias("vobs"), F.sum("admits").alias("admits"))
    .orderBy("month_start","lead_type").show(40, truncate=False))
print("=== Zoho monthly (month x lead_type) ===")
(zoho_monthly.groupBy("month_start","lead_type")
    .agg(F.sum("leads").alias("leads"), F.sum("vobs").alias("vobs"), F.sum("admits").alias("admits"))
    .orderBy("month_start","lead_type").show(40, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ════════════════════════════════════════════════════════════════════════════
# VOB DETAIL (audit) — one row per Dazos VOB, for drill-through auditability.
# Source: dazos_lakehouse.dbo.vob_current -> funnel_lakehouse.dbo.vob_detail.
# Curated / PHI-limited columns only. pulled_by = Created_By (rep).
# ════════════════════════════════════════════════════════════════════════════
_vsrc = spark.table("dazos_lakehouse.dbo.vob_current")

def _blank_null(c):
    return F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))

_vts = F.coalesce(
    F.to_timestamp("Created_Time", "MM-dd-yyyy h:mm a"),
    F.to_timestamp("Created_Time", "yyyy-MM-dd HH:mm:ss"),
    F.to_timestamp("Created_Time"),
)

vob_detail = (_vsrc
    .withColumn("_cts", _vts)
    .select(
        F.col("id").cast("string").alias("vob_id"),
        F.to_date("_cts").alias("created_date"),
        _blank_null("Created_By").alias("pulled_by"),
        F.lit("dazos").alias("source_crm"),
        F.trim(F.concat_ws(" ", F.col("Client_First_Name"), F.col("Client_Last_Name"))).alias("client_name"),
        _blank_null("DOB").alias("client_dob"),
        _blank_null("VOB_Type").alias("vob_type"),
        _blank_null("VOB_Stage").alias("vob_stage"),
        F.coalesce(_blank_null("Behavioral_Health_Payer"), _blank_null("Primary_Insurance_Company")).alias("payor"),
        _blank_null("Treatment_Program").alias("treatment_program"),
        _blank_null("Opportunity").alias("opportunity"),
        F.coalesce(_blank_null("Source"), _blank_null("Lead_Source")).alias("source"),
        F.lit(RUN_TS).cast("timestamp").alias("build_run_ts"),
    ))

vob_detail.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("funnel_lakehouse.dbo.vob_detail")
print(f"Wrote funnel_lakehouse.dbo.vob_detail: {vob_detail.count():,} VOB records")
