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
# May 2026 Raw Data Pull — Calls, Leads, VOBs, Admits + LOS, Cross-ref, Scoring
# For MR# / facility matching verification + v9 KPI point scoring
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date
from rapidfuzz import fuzz
import io, os, pandas as pd

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ============================================================================
# Configuration
# ============================================================================

MAY_START = date(2026, 6, 5)
MAY_END   = date(2026, 6, 11)
TS_FMT    = "MM-dd-yyyy h:mm a"

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

FACILITY_CROSSWALK = {
    "tides edge recovery services": "tides edge recovery",
    "11th ave north":               "tides edge recovery",
    "green acres wellness":         "green acres wellness",
    "green acres recovery":         "green acres wellness",
    "lotus wellness":               "lotus wellness",
    "chattanooga recovery center":  "chattanooga recovery center",
    "graceland recovery":           "graceland recovery",
    "recover now georgia":          "recover now greater atlanta",
}

FACILITY_LINES = {"Abita: Nurse's Station", "RGNA Nurse Station", "EDTC Facility Call"}

BRONZE       = "abfss://dazos-bronze@stkipu001.dfs.core.windows.net/leads/*/*/page_*.json"
CREATED_FMT  = "MM-dd-yyyy h:mm a"
MODIFIED_FMT = "MM-dd-yyyy h:mm a"

# ---- v9 Scoring configuration ----------------------------------------------
POINT_VALUE_USD  = 20.0
LOS_MIN_DAYS     = 7   # LOS < this -> 0 points (all admits); empty LOS -> keep points

# Lead-source groupings (Dazos Lead_Source values)
BD_LEAD_SOURCES = {
    "Business Development", "BD - Stephanie Patton", "BD - Tamera Colcord",
    "BD - Dan Mcgowens", "BD - Troy Roundy",
    "Employee Referral", "Sales Rep / Team Member",
}
STANDARD_EXCLUDED_LEAD_SOURCES = BD_LEAD_SOURCES | {"Alumni", "Step Down", "Main Line"}
ALUMNI_LEAD_SOURCE = "Alumni"

STANDARD_EXCLUDED_LEAD_TYPES = {"Alumni", "BD", "Step Down"}
BD_EXCLUDED_LEAD_TYPES       = {"SEO", "PPC", "Alumni", "Step Down"}
ALUMNI_EXCLUDED_LEAD_TYPES   = {"SEO", "PPC", "BD", "Step Down"}
SELFPAY_EXCLUDED_LEAD_TYPES  = {"BD", "Step Down"}

# Dazos Treatment_Program tiers
INPATIENT_PROGRAMS  = {
    "Green Acres Wellness", "Lotus Wellness",
    "Tides Edge Recovery", "Recover Now Greater Atlanta",
}
OUTPATIENT_PROGRAMS = {"Chattanooga Recovery Center", "Graceland Recovery"}
SELFPAY_PROGRAMS    = INPATIENT_PROGRAMS | OUTPATIENT_PROGRAMS | {"Recover Now Central Alabama"}

# VOB color groupings
VOB_GREEN_YELLOW = {"Approved - Green", "Approved - Yellow"}
VOB_RED          = {"Approved - Red"}
VOB_PURPLE       = {"Approved - Purple"}
VOB_ORANGE       = {"Approved - Orange"}
VOB_SELF_PAY     = {"Self Pay"}

ORANGE_POINTS    = 2   # flat, like Self Pay


# ============================================================================
# TAB 1 — Outbound Calls
# ============================================================================

ctm_raw     = spark.read.table("ctm_lakehouse.dbo.ctm_calls_raw")
mapping_pdf = pd.read_excel("/lakehouse/default/Files/agent_mapping.xlsx")
ctm_to_dazos = dict(zip(
    mapping_pdf.dropna(subset=["ctm_agent_name"])["ctm_agent_name"],
    mapping_pdf.dropna(subset=["ctm_agent_name"])["dazos_rep_name"]
))

CALL_COLS = [
    "call_date", "called_at", "agent_name", "agent_email",
    "direction", "status", "call_status", "dial_status", "outcome",
    "duration", "talk_time", "ring_time", "hold_time", "wait_time",
    "caller_number", "caller_number_bare", "tracking_number",
    "tracking_label", "business_number", "source",
    "facility", "facility_resolved", "is_new_caller", "is_voice",
    "city", "state", "summary",
    "needs_callback", "callback_status", "callback_connected",
]

calls_pdf = (
    ctm_raw
    .filter(F.col("call_date").between(F.lit(MAY_START), F.lit(MAY_END)))
    .filter(F.lower(F.col("direction")) == "outbound")
    .filter(F.col("agent_name").isNotNull())
    .filter(~F.col("agent_name").isin(list(FACILITY_LINES)))
    .select(*[F.col(c) for c in CALL_COLS])
    .toPandas()
)

calls_pdf["dazos_rep"] = calls_pdf["agent_name"].map(ctm_to_dazos)
calls_pdf = calls_pdf.sort_values(["call_date", "agent_name"])

print(f"Outbound calls in May: {len(calls_pdf)}")
print(f"  Mapped to a rep:   {calls_pdf['dazos_rep'].notna().sum()}")
print(f"  Unmapped agents:   {calls_pdf[calls_pdf['dazos_rep'].isna()]['agent_name'].unique().tolist()}")
print(f"\nOutcome breakdown:\n{calls_pdf['outcome'].value_counts().to_string()}")
print(f"\nFacility breakdown:\n{calls_pdf['facility_resolved'].value_counts().to_string()}")


# ============================================================================
# TAB 2 — Inbound Calls
# ============================================================================

inbound_pdf = (
    ctm_raw
    .filter(F.col("call_date").between(F.lit(MAY_START), F.lit(MAY_END)))
    .filter(F.lower(F.col("direction")) == "inbound")
    .filter(F.col("agent_name").isNotNull())
    .filter(~F.col("agent_name").isin(list(FACILITY_LINES)))
    .select(*[F.col(c) for c in CALL_COLS], F.col("is_inbound_missed"))
    .toPandas()
)

inbound_pdf["dazos_rep"] = inbound_pdf["agent_name"].map(ctm_to_dazos)
inbound_pdf = inbound_pdf.sort_values(["call_date", "agent_name"])

print(f"\nInbound calls in May: {len(inbound_pdf)}")
print(f"Outcome breakdown:\n{inbound_pdf['outcome'].value_counts().to_string()}")
print(f"Missed inbound: {inbound_pdf['is_inbound_missed'].sum()}")


# ============================================================================
# TAB 3 — Raw Dazos Leads (true top-of-funnel, company-wide)
# ============================================================================

raw_leads = spark.read.option("multiline", "true").json(BRONZE)
leads = raw_leads.select(F.explode("result.data").alias("rec")).select("rec.*")
leads = leads.toDF(*[c.lower() for c in leads.columns])

w_lead = Window.partitionBy("id").orderBy(
    F.to_timestamp("modified time", MODIFIED_FMT).desc_nulls_last()
)
leads_dedup = leads.withColumn("_rn", F.row_number().over(w_lead)).filter("_rn = 1").drop("_rn")

JUNK      = ["Spam", "Wrong Number", "Missed Call"]
DISQUAL   = ["Medicare/Medicaid", "Uninsured", "Already Working With PT", "Facility Related"]
QUALIFIED = ["Good Call", "Needs Follow Up"]

raw_leads_pdf = (
    leads_dedup
    .withColumn("created_date", F.to_date(F.to_timestamp("created time", CREATED_FMT)))
    .filter(F.col("created_date").between(F.lit(MAY_START), F.lit(MAY_END)))
    .withColumn("lead_bucket",
        F.when(F.col("qualify ctm").isin(QUALIFIED), "Qualified")
         .when(F.col("qualify ctm").isin(DISQUAL), "Disqualified")
         .when(F.col("qualify ctm").isin(JUNK), "Junk")
         .when(F.col("qualify ctm") == "Spanish only", "Spanish only")
         .otherwise("Unknown / no disposition"))
    .select(
        F.col("created_date"),
        F.col("lead no").alias("lead_no"),
        F.col("lead number").alias("lead_number"),
        F.col("first name").alias("first_name"),
        F.col("last name").alias("last_name"),
        F.col("phone"), F.col("primary phone").alias("primary_phone"),
        F.col("email"),
        F.col("lead source").alias("lead_source"),
        F.col("referral source contact").alias("referral_source_contact"),
        F.col("campaign source").alias("campaign_source"),
        F.col("gclid"), F.col("gacid"), F.col("landing page").alias("landing_page"),
        F.col("has insurance?").alias("has_insurance"),
        F.col("insurance provider").alias("insurance_provider"),
        F.col("qualify ctm").alias("qualify_ctm"),
        F.col("lead_bucket"),
        F.col("city"), F.col("state"),
        F.col("opener"), F.col("closer"), F.col("bd rep").alias("bd_rep"),
        F.col("created time").alias("created_time_raw"),
    )
    .toPandas()
    .sort_values(["created_date", "last_name"])
)

print(f"\nRaw Dazos leads created in May: {len(raw_leads_pdf)}")
print(f"\nBy bucket:\n{raw_leads_pdf['lead_bucket'].value_counts().to_string()}")
print(f"\nBy lead source (top 15):\n{raw_leads_pdf['lead_source'].value_counts().head(15).to_string()}")
print(f"\nBy CTM disposition:\n{raw_leads_pdf['qualify_ctm'].value_counts().to_string()}")


# ============================================================================
# TAB 4 — Funnel Leads (IntakeOpportunities created in window, per-facility)
# ============================================================================

ops = spark.read.table("intake_opportunity_current")

funnel_leads_pdf = (
    ops
    .withColumn("_created_date", F.to_date(F.col("Created_Time"), TS_FMT))
    .filter(F.col("_created_date").between(F.lit(MAY_START), F.lit(MAY_END)))
    .select(
        F.col("_created_date").alias("created_date"),
        F.col("MR_Number").alias("mr_number"),
        F.col("Account_Name").alias("opportunity_name"),
        F.col("Treatment_Program").alias("dazos_facility"),
        F.col("Sales_Stage").alias("sales_stage"),
        F.col("VOB_Status").alias("vob_status"),
        F.col("Lead_Source").alias("lead_source"),
        F.col("Lead_Type").alias("lead_type"),
        F.col("Campaign_Source").alias("campaign_source"),
        F.col("Opener").alias("opener"),
        F.col("Closer").alias("closer"),
        F.col("BD_Rep").alias("bd_rep"),
        F.col("Created_Time").alias("created_time_raw"),
    )
    .toPandas()
    .sort_values(["created_date", "opportunity_name"])
)

print(f"\nFunnel leads (IntakeOpportunities) created in May: {len(funnel_leads_pdf)}")
print(f"By lead source:\n{funnel_leads_pdf['lead_source'].value_counts().to_string()}")
print(f"\nBy lead type:\n{funnel_leads_pdf['lead_type'].value_counts().to_string()}")


# ============================================================================
# TAB 5 — VOBs (raw, all statuses)
# ============================================================================

vobs_pdf = (
    ops
    .withColumn("_created_date", F.to_date(F.col("Created_Time"), TS_FMT))
    .withColumn("_modified_date", F.to_date(F.col("Modified_Time"), TS_FMT))
    .filter(F.col("_created_date").between(F.lit(MAY_START), F.lit(MAY_END)))
    .filter(F.col("VOB_Status").isNotNull() & (F.trim(F.col("VOB_Status")) != ""))
    .select(
        F.col("_created_date").alias("created_date"),
        F.col("_modified_date").alias("modified_date"),
        F.col("MR_Number").alias("mr_number"),
        F.col("Account_Name").alias("opportunity_name"),
        F.col("Member_ID").alias("member_id"),
        F.col("VOB_Status").alias("vob_status"),
        F.col("Treatment_Program").alias("dazos_facility"),
        F.col("Sales_Stage").alias("sales_stage"),
        F.col("Lead_Source").alias("lead_source"),
        F.col("Lead_Type").alias("lead_type"),
        F.col("Opener").alias("opener"),
        F.col("Closer").alias("closer"),
        F.col("BD_Rep").alias("bd_rep"),
        F.col("Referring_Contact").alias("referring_contact"),
        F.col("Campaign_Source").alias("campaign_source"),
        F.col("Unqualified_Reason").alias("unqualified_reason"),
        F.col("Modified_Time").alias("modified_time_raw"),
        F.col("Created_Time").alias("created_time_raw"),
    )
    .toPandas()
    .sort_values(["created_date", "opportunity_name"])
)

print(f"\nVOBs created in May: {len(vobs_pdf)}")
print(f"VOB status breakdown:\n{vobs_pdf['vob_status'].value_counts().to_string()}")


# ============================================================================
# TAB 6 — Kipu Admits with LOS
# ============================================================================

kipu = spark.read.table("kipu_lakehouse.dbo.census_clean")

w = Window.partitionBy("mr_number", "admission_date", "location_name") \
          .orderBy(F.col("census_date").desc())

w_unbounded = Window.partitionBy("mr_number", "admission_date", "location_name") \
                    .orderBy(F.col("census_date").desc()) \
                    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

kipu_admits = (
    kipu
    .filter(F.col("location_name").isin(WIDESPREAD_FACILITIES))
    .filter(F.col("mr_number").isNotNull() & (F.trim(F.col("mr_number")) != ""))
    .withColumn("discharge_type_filled",
        F.first(F.col("discharge_type"), ignorenulls=True).over(w_unbounded))
    .withColumn("discharge_type_code_filled",
        F.first(F.col("discharge_type_code"), ignorenulls=True).over(w_unbounded))
    .withColumn("discharge_group_filled",
        F.first(F.col("discharge_group"), ignorenulls=True).over(w_unbounded))
    .withColumn("discharge_date_derived",
        F.when(F.col("discharge_type").isNotNull(), F.col("census_date")))
    .withColumn("discharge_date_filled",
        F.coalesce(
            F.first(F.col("discharge_date"), ignorenulls=True).over(w_unbounded),
            F.first(F.col("discharge_date_derived"), ignorenulls=True).over(w_unbounded),
        ))
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "discharge_date_derived")
    .filter(F.col("admission_date").between(F.lit(MAY_START), F.lit(MAY_END)))
    .select(
        F.col("mr_number"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("admission_date"),
        F.col("discharge_date_filled").alias("discharge_date"),
        F.col("discharge_type_filled").alias("discharge_type"),
        F.col("discharge_type_code_filled").alias("discharge_type_code"),
        F.col("discharge_group_filled").alias("discharge_group"),
        F.col("level_of_care"),
        F.col("program"),
        F.col("location_name").alias("kipu_location"),
        F.col("insurance_company").alias("kipu_insurance"),
        F.col("payment_method"),
        F.col("payment_method_category"),
        F.col("census_date").alias("latest_census_date"),
    )
)

kipu_admits_pdf = kipu_admits.toPandas()

kipu_admits_pdf["kipu_location"] = kipu_admits_pdf["kipu_location"].replace(
    "11th Ave North", "Tides Edge Recovery Services"
)

before = len(kipu_admits_pdf)
kipu_admits_pdf = kipu_admits_pdf.drop_duplicates(
    subset=["mr_number", "kipu_location"], keep="first"
).reset_index(drop=True)
after = len(kipu_admits_pdf)
if before != after:
    print(f"Removed {before - after} duplicates after 11th Ave -> Tides Edge rename")

kipu_admits_pdf["admission_date"] = pd.to_datetime(kipu_admits_pdf["admission_date"])
kipu_admits_pdf["discharge_date"] = pd.to_datetime(kipu_admits_pdf["discharge_date"])
kipu_admits_pdf["los_days"] = (
    kipu_admits_pdf["discharge_date"] - kipu_admits_pdf["admission_date"]
).dt.days

# Estimate discharge for patients who dropped out of the census
latest_extract = kipu.agg(F.max("census_date")).collect()[0][0]
kipu_admits_pdf["_dropped_out"] = (
    kipu_admits_pdf["discharge_date"].isna() &
    (pd.to_datetime(kipu_admits_pdf["latest_census_date"]) < pd.Timestamp(latest_extract))
)
kipu_admits_pdf.loc[kipu_admits_pdf["_dropped_out"], "discharge_date"] = (
    pd.to_datetime(kipu_admits_pdf.loc[kipu_admits_pdf["_dropped_out"], "latest_census_date"])
)
kipu_admits_pdf.loc[kipu_admits_pdf["_dropped_out"], "discharge_type"] = "Estimated (dropped from census)"

kipu_admits_pdf["los_days"] = (
    kipu_admits_pdf["discharge_date"] - kipu_admits_pdf["admission_date"]
).dt.days

print(f"\nEstimated discharge for {kipu_admits_pdf['_dropped_out'].sum()} dropped-out patients")

kipu_admits_pdf["_kipu_name"] = (
    kipu_admits_pdf["first_name"].fillna("") + " " +
    kipu_admits_pdf["last_name"].fillna("")
).str.strip().str.lower()
kipu_admits_pdf["_mr_clean"]      = kipu_admits_pdf["mr_number"].fillna("").astype(str).str.strip()
kipu_admits_pdf["_kipu_fac_norm"] = (
    kipu_admits_pdf["kipu_location"].str.strip().str.lower().map(FACILITY_CROSSWALK)
)

missing = kipu_admits_pdf[kipu_admits_pdf["_kipu_fac_norm"].isna()]["kipu_location"].unique()
if len(missing):
    print(f"WARNING — locations not in crosswalk: {missing.tolist()}")

print(f"\nUnique admissions in May: {len(kipu_admits_pdf)}")
print(f"  Still admitted (no discharge): {kipu_admits_pdf['discharge_date'].isna().sum()}")
print(f"  Discharged:                    {kipu_admits_pdf['discharge_date'].notna().sum()}")
print(f"\nBy location:\n{kipu_admits_pdf['kipu_location'].value_counts().to_string()}")
print(f"\nLOS (discharged only):\n{kipu_admits_pdf['los_days'].dropna().describe()}")
print(f"\nDischarge type breakdown:\n{kipu_admits_pdf['discharge_type'].value_counts().to_string()}")


# ============================================================================
# TAB 7 — Admits x VOBs cross-reference
# Tier 1: MR# + Facility (exact) | Tier 2: Name fuzzy + Facility + closest date
# ============================================================================

ops_all  = spark.read.table("intake_opportunity_current")
accounts = spark.read.table("accounts_current")

def _nz(col):
    c = F.trim(col.cast("string"))
    return F.when(c.isNull() | (c == "") | (c == "--"), None).otherwise(c)

accounts_sel = accounts.select(
    F.col("id").alias("account_id"),
    _nz(F.col("Referral_Source")).alias("referral_source"),
)

dazos_all_pdf = (
    ops_all
    .select(
        F.col("MR_Number").alias("mr_number"),
        F.col("ParentID").alias("parent_id"),
        F.col("Account_Name").alias("opportunity_name"),
        F.col("Treatment_Program").alias("dazos_facility"),
        F.col("Sales_Stage").alias("sales_stage"),
        F.col("VOB_Status").alias("vob_status"),
        F.col("Opener").alias("opener"),
        F.col("Closer").alias("closer"),
        F.col("BD_Rep").alias("bd_rep"),
        F.col("Lead_Source").alias("lead_source"),
        F.col("Lead_Type").alias("lead_type"),
        F.col("Campaign_Source").alias("campaign_source"),
        F.col("Modified_Time").alias("modified_time_raw"),
        F.to_date(F.col("Modified_Time"), TS_FMT).alias("modified_date"),
    )
    .join(accounts_sel, F.col("parent_id") == accounts_sel["account_id"], "left")
    .drop("account_id")
    .toPandas()
)

dazos_all_pdf["_mr_clean"]        = dazos_all_pdf["mr_number"].fillna("").astype(str).str.strip()
dazos_all_pdf["_dazos_name"]      = dazos_all_pdf["opportunity_name"].fillna("").str.strip().str.lower()
dazos_all_pdf["_dazos_fac_clean"] = dazos_all_pdf["dazos_facility"].fillna("").str.strip().str.lower()
dazos_all_pdf["_modified_dt"]     = pd.to_datetime(
    dazos_all_pdf["modified_time_raw"], format="%m-%d-%Y %I:%M %p", errors="coerce"
)
dazos_all_pdf["_is_admitted"] = (
    dazos_all_pdf["sales_stage"].fillna("").str.lower() == "admitted"
).astype(int)

dazos_deduped = (
    dazos_all_pdf
    .sort_values(
        ["_mr_clean", "_dazos_fac_clean", "_is_admitted", "_modified_dt"],
        ascending=[True, True, False, False]
    )
    .drop_duplicates(subset=["_mr_clean", "_dazos_fac_clean"], keep="first")
)

print(f"\nDazos unique (MR#, facility) combos: {len(dazos_deduped)}")

# ---- Tier 1: MR# + normalized facility -------------------------------------
tier1 = kipu_admits_pdf.merge(
    dazos_deduped,
    left_on=["_mr_clean", "_kipu_fac_norm"],
    right_on=["_mr_clean", "_dazos_fac_clean"],
    how="left",
    suffixes=("_kipu", "_dazos")
)

tier1_matched = tier1[tier1["dazos_facility"].notna()].copy()
tier1_matched["_match_method"] = "Tier 1: MR# + Facility"

tier1_unmatched = tier1[tier1["dazos_facility"].isna()][
    ["mr_number_kipu", "first_name", "last_name", "_kipu_name", "_mr_clean",
     "admission_date", "los_days", "discharge_date", "discharge_type",
     "kipu_location", "kipu_insurance", "_kipu_fac_norm"]
].rename(columns={"mr_number_kipu": "mr_number"}).copy()

print(f"Tier 1 matched:   {len(tier1_matched)}")
print(f"Tier 1 unmatched: {len(tier1_unmatched)}")

# ---- Tier 2: Name fuzzy + Facility + closest modified date -----------------
def _no_match(k, reason):
    return {**k.to_dict(), "_match_method": reason,
            "dazos_facility": None, "sales_stage": None, "vob_status": None,
            "opener": None, "closer": None, "bd_rep": None,
            "lead_source": None, "lead_type": None, "campaign_source": None,
            "referral_source": None, "mr_number_dazos": None, "_dazos_name": None}

tier2_rows = []
for _, k in tier1_unmatched.iterrows():
    fac      = k["_kipu_fac_norm"]
    name     = k["_kipu_name"]
    admit_dt = pd.Timestamp(k["admission_date"])

    cands = dazos_all_pdf[dazos_all_pdf["_dazos_fac_clean"] == fac].copy()
    if len(cands) == 0:
        tier2_rows.append(_no_match(k, "No match (facility not in Dazos)"))
        continue

    cands["_name_sim"] = cands["_dazos_name"].apply(
        lambda d: fuzz.ratio(name, d) if isinstance(d, str) else 0
    )
    cands = cands[cands["_name_sim"] >= 80]
    if len(cands) == 0:
        tier2_rows.append(_no_match(k, "No match (name below threshold)"))
        continue

    cands["_date_gap"] = (cands["_modified_dt"] - admit_dt).abs()
    best = cands.sort_values(["_name_sim", "_date_gap"], ascending=[False, True]).iloc[0]

    tier2_rows.append({
        **k.to_dict(),
        "dazos_facility":  best["dazos_facility"],
        "sales_stage":     best["sales_stage"],
        "vob_status":      best["vob_status"],
        "opener":          best["opener"],
        "closer":          best["closer"],
        "bd_rep":          best["bd_rep"],
        "lead_source":     best["lead_source"],
        "lead_type":       best["lead_type"],
        "campaign_source": best["campaign_source"],
        "referral_source": best["referral_source"],
        "mr_number_dazos": best["mr_number"],
        "_dazos_name":     best["_dazos_name"],
        "_match_method":   f"Tier 2: Name + Facility (sim={int(best['_name_sim'])})",
    })

tier2_pdf       = pd.DataFrame(tier2_rows) if tier2_rows else pd.DataFrame()
tier2_matched   = tier2_pdf[tier2_pdf["_match_method"].str.startswith("Tier 2: Name")].copy() if len(tier2_pdf) > 0 else pd.DataFrame()
tier2_unmatched = tier2_pdf[~tier2_pdf["_match_method"].str.startswith("Tier 2: Name")].copy() if len(tier2_pdf) > 0 else pd.DataFrame()

print(f"Tier 2 matched:   {len(tier2_matched)}")
print(f"Tier 2 unmatched: {len(tier2_unmatched)}")

# ---- Build final crossref output -------------------------------------------
OUTPUT_COLS = [
    "mr_number", "first_name", "last_name", "kipu_name", "dazos_name",
    "admission_date", "los_days", "discharge_date", "discharge_type",
    "kipu_insurance", "kipu_location", "kipu_fac_normalized",
    "dazos_facility", "sales_stage", "vob_status",
    "lead_source", "lead_type", "campaign_source", "bd_rep", "referral_source",
    "opener", "closer",
    "match_method",
]

def prep_tier(df, mr_col="mr_number"):
    if len(df) == 0:
        return pd.DataFrame(columns=OUTPUT_COLS)
    out = df.copy()
    if mr_col != "mr_number" and mr_col in out.columns:
        out = out.rename(columns={mr_col: "mr_number"})
    out = out.rename(columns={
        "_kipu_name":     "kipu_name",
        "_dazos_name":    "dazos_name",
        "_kipu_fac_norm": "kipu_fac_normalized",
        "_match_method":  "match_method",
    })
    for c in OUTPUT_COLS:
        if c not in out.columns:
            out[c] = None
    return out[OUTPUT_COLS]

crossref = pd.concat([
    prep_tier(tier1_matched, mr_col="mr_number_kipu"),
    prep_tier(tier2_matched),
    prep_tier(tier2_unmatched),
], ignore_index=True, sort=False).sort_values(
    ["kipu_location", "admission_date", "last_name"]
)

print(f"\n{'='*50}")
print(f"Cross-reference summary ({len(crossref)} total admits)")
print(f"{'='*50}")
print(f"  Tier 1 (MR# + Facility):  {(crossref['match_method'] == 'Tier 1: MR# + Facility').sum()}")
print(f"  Tier 2 (Name + Facility): {crossref['match_method'].str.startswith('Tier 2').sum()}")
print(f"  No match:                 {crossref['match_method'].str.startswith('No match').sum()}")
if crossref["match_method"].str.startswith("No match").any():
    print(f"\nNo-match breakdown by facility:")
    print(crossref[crossref["match_method"].str.startswith("No match")]["kipu_location"].value_counts().to_string())


# ============================================================================
# TAB 8 — v9 Point Scoring (routing by LEAD TYPE)
# ============================================================================
# Routing changes per request:
#   - BD admits      = lead_type == "BD"
#   - Alumni admits  = lead_type == "Alumni"
#   - Self Pay       = exclude lead_type in {BD, Step Down}
#   - (VOB filter handled in the VOB scoring section below)

def categorize_admit(row):
    lead_type   = (row.get("lead_type") or "").strip()
    vob_status  = row.get("vob_status")
    program     = row.get("dazos_facility")
    sales_stage = row.get("sales_stage")
    method      = row.get("match_method") or ""

    if method.startswith("No match"):
        return ("Not in Dazos", 0)
    if sales_stage != "Admitted":
        return (f"Not Admitted in Dazos ({sales_stage})", 0)

    is_self_pay = vob_status in VOB_SELF_PAY
    is_gy       = vob_status in VOB_GREEN_YELLOW
    is_r        = vob_status in VOB_RED
    is_p        = vob_status in VOB_PURPLE
    is_orange   = vob_status in VOB_ORANGE

    if not (is_self_pay or is_gy or is_r or is_p or is_orange):
        return (f"No scorable VOB Status ({vob_status})", 0)

    is_inpt  = program in INPATIENT_PROGRAMS
    is_outpt = program in OUTPATIENT_PROGRAMS
    is_selfpay_program = program in SELFPAY_PROGRAMS

    if not (is_inpt or is_outpt or is_selfpay_program):
        return (f"Untracked program ({program})", 0)

    lt = lead_type.lower()

    # Approved-Orange: flat 2 pts
    if is_orange:
        return ("Approved-Orange (flat)", ORANGE_POINTS)

    # Self Pay: flat 3, exclude BD / Step Down lead types
    if is_self_pay:
        if lt in {"bd", "step down"}:
            return (f"Self Pay excluded (lead type {lead_type})", 0)
        if not is_selfpay_program:
            return ("Self Pay excluded (program)", 0)
        return ("Self Pay", 3)

    # BD admits: lead_type == BD
    if lt == "bd":
        if is_inpt:
            if is_gy: return ("BD Inpt G/Y", 3)
            if is_r:  return ("BD Inpt R", 1)
            if is_p:  return ("BD Inpt P", 0.5)
        if is_outpt:
            if is_gy: return ("BD Outpt G/Y", 2)
            if is_r:  return ("BD Outpt R", 0.5)
            if is_p:  return ("BD Outpt P (not scored)", 0)
        return (f"BD untracked program ({program})", 0)

    # Alumni admits: lead_type == Alumni
    if lt == "alumni":
        if is_inpt:
            if is_gy: return ("Alumni Inpt G/Y", 3)
            if is_r:  return ("Alumni Inpt R", 1)
            if is_p:  return ("Alumni Inpt P", 0.5)
        if is_outpt:
            if is_gy: return ("Alumni Outpt G/Y", 2)
            if is_r:  return ("Alumni Outpt R", 0.5)
            if is_p:  return ("Alumni Outpt P (not scored)", 0)
        return (f"Alumni untracked program ({program})", 0)

    # Step Down lead type: not scored as standard
    if lt == "step down":
        return ("Standard excluded (lead type Step Down)", 0)

    # Standard: everything else
    if is_inpt:
        if is_gy: return ("Standard LW/GAW/TED/RNGA G/Y", 10)
        if is_r:  return ("Standard LW/GAW/TED/RNGA R", 3)
        if is_p:
            if program == "Tides Edge Recovery":
                return ("Standard Tides Purple", 2)
            return ("Standard Inpt Purple (LW/GAW not scored)", 0)
    if is_outpt:
        if is_gy: return ("Standard CRC/GLR G/Y", 3)
        if is_r:  return ("Standard CRC/GLR R", 1)
        if is_p:  return ("Standard Outpt P (not scored)", 0)

    return (f"Uncategorized (LT={lead_type})", 0)


scored = crossref.copy()
scored["block"], scored["points_raw"] = zip(*scored.apply(categorize_admit, axis=1))

def apply_los_rule(row):
    raw = row["points_raw"]
    if raw == 0:
        return raw, False, ""
    los = row.get("los_days")
    if pd.isna(los):
        return raw, False, ""
    if los < LOS_MIN_DAYS:
        return 0, True, f"LOS={int(los)}d < {LOS_MIN_DAYS}d"
    return raw, False, ""

los_results = scored.apply(apply_los_rule, axis=1)
scored["points"]          = [r[0] for r in los_results]
scored["los_override"]    = [r[1] for r in los_results]
scored["override_reason"] = [r[2] for r in los_results]

print(f"\nLOS < {LOS_MIN_DAYS}-day admits zeroed: {scored['los_override'].sum()}")
print(f"Total admit points (after LOS rule): {scored['points'].sum():.1f}")

scored_cols = [
    "admission_date", "first_name", "last_name", "mr_number",
    "kipu_location", "dazos_facility", "sales_stage", "vob_status",
    "lead_source", "lead_type", "campaign_source", "bd_rep", "referral_source",
    "opener", "closer",
    "los_days", "discharge_type",
    "block", "points_raw", "los_override", "override_reason", "points",
    "match_method",
]
scored_out = scored[[c for c in scored_cols if c in scored.columns]].sort_values(
    ["closer", "admission_date", "last_name"]
).reset_index(drop=True)


# ============================================================================
# TAB 9 — VOB scoring detail (credited to Opener)
# ============================================================================
# VOB filter: exclude lead_type in {BD, Alumni, Step Down}.

VOB_EXCLUDED_LEAD_TYPES = {"bd", "alumni", "step down"}

vob_scored = vobs_pdf.copy()

def vob_block_points(row):
    vs = row["vob_status"]
    lt = (row.get("lead_type") or "").strip().lower()
    if lt in VOB_EXCLUDED_LEAD_TYPES:
        return (f"Excluded VOB (lead type {row.get('lead_type')})", 0.0)
    if vs in VOB_GREEN_YELLOW:
        return ("OON VoBs (Green & Yellow)", 1.0)
    if vs in (VOB_RED | VOB_PURPLE):
        return ("INN VoBs (Red & Purple)", 0.5)
    return ("Non-scoring VOB", 0.0)

vob_blocks = vob_scored.apply(vob_block_points, axis=1)
vob_scored["vob_block"]  = [b[0] for b in vob_blocks]
vob_scored["vob_points"] = [b[1] for b in vob_blocks]

vob_scored_out = vob_scored[[
    "created_date", "opener", "closer", "opportunity_name", "mr_number",
    "vob_status", "dazos_facility", "lead_source", "lead_type",
    "vob_block", "vob_points",
]].sort_values(["opener", "created_date"]).reset_index(drop=True)


# ============================================================================
# TAB 10 — Outbound call counts per rep
# ============================================================================

outbound_counts = (
    calls_pdf.dropna(subset=["dazos_rep"])
    .groupby("dazos_rep").size().reset_index(name="outbound_calls")
    .rename(columns={"dazos_rep": "rep"})
    .sort_values("outbound_calls", ascending=False)
    .reset_index(drop=True)
)


# ============================================================================
# Summary config
# ============================================================================

reps_with_activity = sorted(
    set(scored.loc[scored["closer"].notna(), "closer"])
    | set(vob_scored.loc[vob_scored["opener"].notna(), "opener"])
    | set(outbound_counts["rep"])
)
reps_with_activity = [r for r in reps_with_activity if str(r).strip() and str(r) != "None"]

ADMIT_BLOCK_POINTS = {
    "Standard LW/GAW/TED/RNGA G/Y": 10,
    "Standard LW/GAW/TED/RNGA R":   3,
    "Standard Tides Purple":        2,
    "Standard CRC/GLR G/Y":         3,
    "Standard CRC/GLR R":           1,
    "BD Inpt G/Y":                  3,
    "BD Inpt R":                    1,
    "BD Inpt P":                    0.5,
    "BD Outpt G/Y":                 2,
    "BD Outpt R":                   0.5,
    "Alumni Inpt G/Y":              3,
    "Alumni Inpt R":                1,
    "Alumni Inpt P":                0.5,
    "Alumni Outpt G/Y":             2,
    "Alumni Outpt R":               0.5,
    "Self Pay":                     3,
    "Approved-Orange (flat)":       ORANGE_POINTS,
}
VOB_BLOCK_POINTS = {
    "OON VoBs (Green & Yellow)": 1.0,
    "INN VoBs (Red & Purple)":   0.5,
}

SH_ADMIT = "Admit Scoring"
SH_VOB   = "VOB Scoring"
SH_OB    = "Outbound Detail"

def col_letter(idx0):
    s = ""
    n = idx0 + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

admit_cols   = list(scored_out.columns)
ADMIT_CLOSER = col_letter(admit_cols.index("closer"))
ADMIT_BLOCK  = col_letter(admit_cols.index("block"))

vob_cols   = list(vob_scored_out.columns)
VOB_OPENER = col_letter(vob_cols.index("opener"))
VOB_BLOCK  = col_letter(vob_cols.index("vob_block"))

ob_cols  = list(outbound_counts.columns)
OB_REP   = col_letter(ob_cols.index("rep"))
OB_CALLS = col_letter(ob_cols.index("outbound_calls"))

n_admit = len(scored_out)
n_vob   = len(vob_scored_out)
n_ob    = len(outbound_counts)

# Summary columns: (header, kind, block, points)
summary_spec = [("Outbound Calls", "outbound", None, 0)]
for blk, pts in VOB_BLOCK_POINTS.items():
    summary_spec.append((blk, "vob_count", blk, pts))
for blk, pts in ADMIT_BLOCK_POINTS.items():
    summary_spec.append((blk, "admit_count", blk, pts))


# ============================================================================
# Filters & Conditions documentation tab
# ============================================================================

filters_doc = pd.DataFrame([
    ["SCORING STREAMS", "", ""],
    ["VOBs", "Credited to", "Opener"],
    ["Admits", "Credited to", "Closer"],
    ["Point value", "Per point", f"${POINT_VALUE_USD:.2f}"],
    ["", "", ""],
    ["VOB FILTERS", "", ""],
    ["VOB lead type exclusion", "Exclude lead_type in", "BD, Alumni, Step Down"],
    ["OON VoBs (Green & Yellow)", "Points", "1.0"],
    ["INN VoBs (Red & Purple)", "Points", "0.5"],
    ["", "", ""],
    ["ADMIT ROUTING (by LEAD TYPE)", "", ""],
    ["BD admits", "Condition", "lead_type == 'BD'"],
    ["Alumni admits", "Condition", "lead_type == 'Alumni'"],
    ["Self Pay", "Condition", "VOB_Status == 'Self Pay'; exclude lead_type BD / Step Down"],
    ["Standard", "Condition", "lead_type not BD / Alumni / Step Down"],
    ["Step Down lead type", "Condition", "Not scored"],
    ["", "", ""],
    ["LOS RULE", "", ""],
    ["LOS < 7 days", "Effect", "Admit points -> 0 (all discharge types)"],
    ["LOS empty (still admitted)", "Effect", "Keep points as relevant"],
    ["", "", ""],
    ["VOB COLORS", "", ""],
    ["Approved - Green / Yellow", "Tier", "G/Y (best OON benefits)"],
    ["Approved - Red", "Tier", "R"],
    ["Approved - Purple", "Tier", "P"],
    ["Approved - Orange", "Tier", f"Flat {ORANGE_POINTS} pts (any tier)"],
    ["Self Pay", "Tier", "Flat 3 pts"],
    ["", "", ""],
    ["FACILITY TIERS", "", ""],
    ["Inpatient", "Programs", ", ".join(sorted(INPATIENT_PROGRAMS))],
    ["Outpatient", "Programs", ", ".join(sorted(OUTPATIENT_PROGRAMS))],
    ["", "", ""],
    ["ADMIT POINT TABLE", "Block", "Points"],
] + [[f"  {blk}", "", str(pts)] for blk, pts in ADMIT_BLOCK_POINTS.items()],
    columns=["Section", "Field", "Value"]
)


# ============================================================================
# Write Excel
# ============================================================================

kipu_admits_excel = kipu_admits_pdf.drop(
    columns=["_kipu_name", "_mr_clean", "_kipu_fac_norm", "_dropped_out"], errors="ignore"
)

filename = "may_2026_raw_data_verification.xlsx"
buf = io.BytesIO()

with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
    wb  = writer.book
    hdr = wb.add_format({"bold": True, "bg_color": "#305496",
                         "font_color": "white", "border": 1, "text_wrap": True,
                         "align": "center", "valign": "vcenter"})
    fmt_pts_row = wb.add_format({"italic": True, "bg_color": "#E8EDF5",
                                 "align": "center", "border": 1, "num_format": "#,##0.0"})
    fmt_pts_lbl = wb.add_format({"italic": True, "bg_color": "#E8EDF5",
                                 "bold": True, "border": 1})
    fmt_int   = wb.add_format({"num_format": "#,##0"})
    fmt_pts   = wb.add_format({"num_format": "#,##0.0"})
    fmt_money = wb.add_format({"num_format": "$#,##0.00"})
    fmt_tot   = wb.add_format({"bold": True, "bg_color": "#D9E1F2", "top": 2})
    fmt_tot_m = wb.add_format({"bold": True, "bg_color": "#D9E1F2", "top": 2,
                               "num_format": "$#,##0.00"})

    def write_tab(df, sheet, freeze_col=0):
        df.to_excel(writer, sheet_name=sheet, index=False)
        ws = writer.sheets[sheet]
        for i, col in enumerate(df.columns):
            ws.write(0, i, col, hdr)
            width = max(df[col].astype(str).map(len).max() if len(df) else 0,
                        len(str(col))) + 2
            ws.set_column(i, i, min(width, 45))
        ws.freeze_panes(1, freeze_col)

    write_tab(calls_pdf,         "Outbound Calls")
    write_tab(inbound_pdf,       "Inbound Calls")
    write_tab(raw_leads_pdf,     "Raw Leads (Company-wide)")
    write_tab(funnel_leads_pdf,  "Funnel Leads (Per-Facility)")
    write_tab(vobs_pdf,          "VOBs")
    write_tab(kipu_admits_excel, "Kipu Admits + LOS")
    write_tab(crossref,          "Admits x VOBs (Cross-ref)")
    write_tab(scored_out,        SH_ADMIT, freeze_col=4)
    write_tab(vob_scored_out,    SH_VOB,   freeze_col=3)
    write_tab(outbound_counts,   SH_OB)
    write_tab(filters_doc,       "Filters & Conditions")

    # --- SUMMARY tab: header row + points row + per-rep formulas ---
    ws = wb.add_worksheet("Summary")
    writer.sheets["Summary"] = ws

    headers = ["Admissions Agent"] + [s[0] for s in summary_spec] + ["Total Points", "Bonus ($)"]
    for c, h in enumerate(headers):
        ws.write(0, c, h, hdr)

    # Row 2: point value per column (label in col A)
    ws.write(1, 0, "Points per event ->", fmt_pts_lbl)
    for cidx, (hname, kind, blk, pts) in enumerate(summary_spec, start=1):
        if kind == "outbound":
            ws.write(1, cidx, "n/a", fmt_pts_lbl)
        else:
            ws.write(1, cidx, pts, fmt_pts_row)
    ws.write(1, len(summary_spec) + 1, "", fmt_pts_lbl)
    ws.write(1, len(summary_spec) + 2, "", fmt_pts_lbl)

    ws.set_column(0, 0, 22)
    ws.set_column(1, len(headers) - 1, 13)
    ws.freeze_panes(2, 1)

    admit_rng_block = f"'{SH_ADMIT}'!${ADMIT_BLOCK}$2:${ADMIT_BLOCK}${n_admit+1}"
    admit_rng_clos  = f"'{SH_ADMIT}'!${ADMIT_CLOSER}$2:${ADMIT_CLOSER}${n_admit+1}"
    vob_rng_block   = f"'{SH_VOB}'!${VOB_BLOCK}$2:${VOB_BLOCK}${n_vob+1}"
    vob_rng_open    = f"'{SH_VOB}'!${VOB_OPENER}$2:${VOB_OPENER}${n_vob+1}"
    ob_rng_rep      = f"'{SH_OB}'!${OB_REP}$2:${OB_REP}${n_ob+1}"
    ob_rng_calls    = f"'{SH_OB}'!${OB_CALLS}$2:${OB_CALLS}${n_ob+1}"

    first_data_row = 3  # row 1 header, row 2 points, row 3 first rep
    for ridx, rep in enumerate(reps_with_activity):
        xl = first_data_row + ridx
        rep_cell = f"$A{xl}"
        ws.write(xl - 1, 0, rep)

        count_cells = []
        for cidx, (hname, kind, blk, pts) in enumerate(summary_spec, start=1):
            if kind == "outbound":
                f = f'=SUMIFS({ob_rng_calls},{ob_rng_rep},{rep_cell})'
                ws.write_formula(xl - 1, cidx, f, fmt_int)
            elif kind == "vob_count":
                f = f'=COUNTIFS({vob_rng_open},{rep_cell},{vob_rng_block},"{blk}")'
                ws.write_formula(xl - 1, cidx, f, fmt_int)
                count_cells.append((cidx, pts))
            elif kind == "admit_count":
                f = f'=COUNTIFS({admit_rng_clos},{rep_cell},{admit_rng_block},"{blk}")'
                ws.write_formula(xl - 1, cidx, f, fmt_int)
                count_cells.append((cidx, pts))

        total_col = len(summary_spec) + 1
        terms = [f"{col_letter(ci)}{xl}*{pts}" for ci, pts in count_cells]
        ws.write_formula(xl - 1, total_col, f"={'+'.join(terms)}", fmt_pts)

        bonus_col = total_col + 1
        ws.write_formula(xl - 1, bonus_col,
                         f"={col_letter(total_col)}{xl}*{POINT_VALUE_USD}", fmt_money)

    last_rep_row = first_data_row + len(reps_with_activity) - 1
    tot_xl = last_rep_row + 1
    ws.write(tot_xl - 1, 0, "TOTAL", fmt_tot)
    for cidx in range(1, len(summary_spec) + 2):
        cl = col_letter(cidx)
        ws.write_formula(tot_xl - 1, cidx,
                         f"=SUM({cl}{first_data_row}:{cl}{last_rep_row})", fmt_tot)
    bcl = col_letter(len(summary_spec) + 2)
    ws.write_formula(tot_xl - 1, len(summary_spec) + 2,
                     f"=SUM({bcl}{first_data_row}:{bcl}{last_rep_row})", fmt_tot_m)

buf.seek(0)
excel_bytes = buf.getvalue()

out_path = f"/lakehouse/default/Files/admissions_reports/{filename}"
os.makedirs(os.path.dirname(out_path), exist_ok=True)
with open(out_path, "wb") as f:
    f.write(excel_bytes)

print(f"\nSaved: {out_path}")
print(f"Summary reps: {len(reps_with_activity)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
