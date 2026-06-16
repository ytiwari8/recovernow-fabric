# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "12e92db1-a5e3-4866-98cd-cb0ffb3d8af4",
# META       "default_lakehouse_name": "ctm_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795",
# META       "known_lakehouses": [
# META         {
# META           "id": "12e92db1-a5e3-4866-98cd-cb0ffb3d8af4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric notebook source
# ============================================================================
# Transform CTM Data  (PATCHED)
# ----------------------------------------------------------------------------
# Reads CTM call JSON and builds Delta tables in ctm_lakehouse.
#
# WHAT CHANGED IN THIS VERSION:
#   1. DUAL-CONTAINER READ. Reads BOTH ctm-bronze-rebuild (clean rebuilt
#      history) AND ctm-bronze (live daily merges from the hardened puller).
#      Overlapping dates are collapsed by the dedupe step. This is what keeps
#      the table both clean (history) and current (new days) without ever
#      overwriting the live blobs.
#   2. REAL DEDUPE. ingested_at now carries the blob-level pull timestamp
#      (_ingested_at) instead of a fresh notebook literal, so the
#      partitionBy(id) dedupe genuinely keeps the freshest copy of each call.
#   3. SCHEMA ASSERTION. Fails loud and early if a read returns nothing or the
#      expected call fields are missing (e.g. CTM changes its API shape).
#
# Output tables:
#   - ctm_calls_raw              fact, partitioned by call_date
#   - ctm_facility_mapping       dim, loaded from xlsx
#   - ctm_daily_facility_stats   pre-aggregated daily rollup by facility
#   - ctm_daily_agent_stats      pre-aggregated daily rollup by agent
#   - ctm_unmapped_log           audit trail of source_names not in mapping
#
# Schedule: daily, after the Function App puller (which now runs 5 AM Central).
# ============================================================================

# CELL ********************

# --- imports ---
import json
import re
from datetime import datetime, timezone
from typing import Iterable

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

import notebookutils

# CELL ********************

# --- config ---
STORAGE_ACCOUNT  = "stkipu001"
KEY_VAULT_URL    = "https://kv-kipu1.vault.azure.net/"

LAKEHOUSE_NAME   = "ctm_lakehouse"
MAPPING_XLSX_PATH = f"Files/CTM Mappings/CTM_Facility_Mapping_FINAL.xlsx"

# Blob source paths — read BOTH containers (see read cell for why)
REBUILD_PATH = "abfss://ctm-bronze-rebuild@stkipu001.dfs.core.windows.net/*/*/calls.json"
LIVE_PATH    = "abfss://ctm-bronze@stkipu001.dfs.core.windows.net/*/*/calls.json"

# Timezone used for partition key (matches Function App)
REPORT_TZ = "America/Chicago"

# SendGrid alert settings
ALERT_FROM_EMAIL = "data@recovernow.com"
ALERT_TO_EMAIL   = "data@recovernow.com"
ALERT_SUBJECT    = "[CTM] New unmapped sources detected"

# Callback matching
CALLBACK_LOOKAHEAD_DAYS = 7
CALLBACK_MIN_DURATION_SEC = 30       # drop sub-30s outbounds (butt-dials)
PENDING_HOURS = 24                    # calls less than this old = 'pending'

# CELL ********************

# --- read JSON blobs from BOTH the rebuild (clean history) and live (daily) ---
# ctm-bronze-rebuild = frozen clean rebuild (Feb 22 -> rebuild date)
# ctm-bronze         = live daily merges from the hardened puller (ongoing)
# Overlapping dates appear in both; the dedupe step (next cell) collapses by id,
# keeping the most-recently-ingested copy via the blob envelope's ingested_at.

def read_blobs(path, label):
    try:
        df = spark.read.option("multiLine", "true").json(path)
        n = df.count()
        print(f"  {label}: {n:,} blob-day records")
        return df
    except Exception as e:
        print(f"  {label}: read failed ({e}) - skipping")
        return None

print("Reading CTM blobs from both containers:")
parts = [d for d in (read_blobs(REBUILD_PATH, "rebuild"),
                     read_blobs(LIVE_PATH, "live")) if d is not None]

if not parts:
    raise RuntimeError("FATAL: neither ctm-bronze-rebuild nor ctm-bronze returned data")

# unionByName handles any minor column-order differences between the two
raw_files = parts[0]
for p in parts[1:]:
    raw_files = raw_files.unionByName(p, allowMissingColumns=True)

# Each JSON file is {account_slug, call_date, call_count, ingested_at, calls:[...]}
raw_df = raw_files.select(
    F.col("account_slug").alias("_account_slug"),
    F.col("call_date").alias("_call_date"),
    F.col("ingested_at").alias("_ingested_at"),
    F.explode("calls").alias("call"),
)

print(f"Total raw events read (both containers, pre-dedupe): {raw_df.count():,}")

# CELL ********************

# --- schema assertion: fail loud and early if the read is wrong ---
# Catches three failure modes before they corrupt downstream tables:
#   a) empty read (bad path / empty containers)
#   b) CTM renamed/removed a field we depend on
#   c) the explode produced no usable call objects
EXPECTED_CALL_FIELDS = {
    "id", "unix_time", "direction", "status",
    "caller_number", "source", "account_id",
}

# fields present on the exploded `call` struct
call_field_names = set(raw_df.select("call.*").columns)
missing = EXPECTED_CALL_FIELDS - call_field_names
if missing:
    raise RuntimeError(
        f"SCHEMA CHECK FAILED: expected call fields missing from CTM JSON: {sorted(missing)}. "
        f"CTM may have changed its API shape, or the blob structure is wrong. "
        f"Fields seen: {sorted(call_field_names)[:40]}"
    )

row_count = raw_df.count()
if row_count == 0:
    raise RuntimeError("SCHEMA CHECK FAILED: 0 events read. Check blob paths / container contents.")

print(f"Schema check passed: {row_count:,} events, all {len(EXPECTED_CALL_FIELDS)} key fields present.")

# CELL ********************

# --- flatten + derive core columns ---
flat_df = raw_df.select(
    F.col("call.id").cast("long").alias("id"),
    F.col("call.sid").alias("sid"),
    F.col("call.account_id").cast("int").alias("account_id"),
    F.col("_account_slug").alias("account_slug"),

    # Timestamps: unix_time is authoritative; derive call_date in REPORT_TZ
    F.col("call.unix_time").cast("long").alias("unix_time"),
    F.from_unixtime(F.col("call.unix_time")).cast("timestamp").alias("called_at"),
    F.to_date(F.from_utc_timestamp(
        F.from_unixtime(F.col("call.unix_time")).cast("timestamp"),
        REPORT_TZ
    )).alias("call_date"),

    # Raw status fields
    F.col("call.direction").alias("direction"),
    F.lower(F.trim(F.col("call.status"))).alias("status"),
    F.lower(F.trim(F.col("call.call_status"))).alias("call_status"),
    F.lower(F.trim(F.col("call.dial_status"))).alias("dial_status"),

    F.col("call.duration").cast("int").alias("duration"),
    F.col("call.talk_time").cast("int").alias("talk_time"),
    F.col("call.ring_time").cast("int").alias("ring_time"),
    F.col("call.hold_time").cast("int").alias("hold_time"),
    F.col("call.wait_time").cast("int").alias("wait_time"),

    # Parties
    F.col("call.caller_number").alias("caller_number"),
    F.col("call.caller_number_bare").alias("caller_number_bare"),
    F.col("call.tracking_number").alias("tracking_number"),
    F.col("call.tracking_label").alias("tracking_label"),
    F.col("call.business_number").alias("business_number"),

    # Source
    F.trim(F.col("call.source")).alias("source"),
    F.col("call.source_id").cast("int").alias("source_id"),
    F.col("call.source_sid").alias("source_sid"),

    # Agent (nested object)
    F.col("call.agent_id").alias("agent_id"),
    F.col("call.agent.name").alias("agent_name"),
    F.col("call.agent.email").alias("agent_email"),

    # Location
    F.col("call.city").alias("city"),
    F.col("call.state").alias("state"),
    F.col("call.postal_code").alias("postal_code"),

    # Misc
    F.col("call.is_new_caller").cast("boolean").alias("is_new_caller"),
    F.col("call.tag_list").alias("tag_list"),
    F.col("call.audio").alias("audio_url"),
    F.col("call.transcription").alias("transcription_url"),
    F.col("call.transcription_text").alias("transcription_text"),
    F.col("call.summary").alias("summary"),

    # Carry the BLOB-LEVEL ingestion timestamp through (NOT a fresh literal).
    # This is what makes the dedupe below meaningful: rebuild blobs carry their
    # rebuild timestamp, live blobs carry each morning's pull time, so "freshest
    # copy wins" actually resolves to the right record.
    F.col("_ingested_at").alias("ingested_at"),
)

# Dedupe - same id appears in multiple blobs: across the 2-day puller overlap
# AND across the rebuild + live containers. Keep the copy with the most recent
# blob-level ingested_at (freshest pull wins), breaking ties by unix_time so a
# real record beats an empty/edge one.
w = Window.partitionBy("id").orderBy(
    F.col("ingested_at").desc_nulls_last(),
    F.col("unix_time").desc_nulls_last(),
)
flat_df = flat_df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

print(f"Unique events after dedupe: {flat_df.count():,}")

# --- agent identity normalization (collapse duplicate agent_ids) ---
# Several real people have multiple CTM agent_ids (primary + rollover/intermedia
# lines), sometimes with misspelled names ("Pellegin"). We normalize the name,
# pick one canonical agent_id per person (the one with the most calls), and
# remap. Every merge is logged so a wrong-merge (two different people, same
# name) can be caught by eye.

# Normalize a name for matching: lowercase, strip, collapse internal whitespace,
# drop non-letter chars. This makes "Kenneth  Sumera" == "Kenneth Sumera".
def norm_name_col(c):
    x = F.lower(F.trim(c))
    x = F.regexp_replace(x, r"[^a-z ]", "")      # letters + spaces only
    x = F.regexp_replace(x, r"\s+", " ")         # collapse whitespace
    return F.trim(x)

# NOTE on misspellings like "Pellegin" vs "Pellegrin": exact-normalized match
# won't catch a dropped letter. We handle the known ones via an alias map below;
# everything else collapses on exact normalized name.
NAME_ALIASES = {
    "braeden pellegin": "braeden pellegrin",
    # add more as discovered: "<misspelled normalized>": "<correct normalized>"
}
alias_expr = F.col("_norm_name_raw")
for wrong, right in NAME_ALIASES.items():
    alias_expr = F.when(F.col("_norm_name_raw") == wrong, F.lit(right)).otherwise(alias_expr)

agents = (flat_df
    .filter(F.col("agent_id").isNotNull())
    .withColumn("_norm_name_raw", norm_name_col(F.col("agent_name")))
    .withColumn("_norm_name", alias_expr)
    .filter(F.col("_norm_name") != "")
)

# Per normalized name: pick the agent_id with the most calls as canonical
id_counts = (agents.groupBy("_norm_name", "agent_id", "agent_name", "agent_email")
                   .count())

w_canon = Window.partitionBy("_norm_name").orderBy(F.col("count").desc())
canonical = (id_counts
    .withColumn("_rk", F.row_number().over(w_canon))
    .filter("_rk = 1")
    .select(
        F.col("_norm_name"),
        F.col("agent_id").alias("canonical_agent_id"),
        F.col("agent_name").alias("canonical_agent_name"),
        F.col("agent_email").alias("canonical_agent_email"),
    ))

# Build the full crosswalk: every agent_id -> its canonical id
agent_xwalk = (id_counts.select("_norm_name", "agent_id", "agent_name", "count")
    .join(canonical, "_norm_name", "left"))

# LOG the merges so you can verify (any name with >1 distinct agent_id)
print("=== Agent merges (names with multiple IDs collapsed) ===")
(agent_xwalk
    .groupBy("_norm_name", "canonical_agent_name")
    .agg(F.countDistinct("agent_id").alias("n_ids"),
         F.sum("count").alias("total_calls"))
    .filter(F.col("n_ids") > 1)
    .orderBy(F.col("total_calls").desc())
    .show(50, truncate=False))

# Persist the crosswalk as a table for inspection / reuse
agent_xwalk.select(
    "agent_id", "agent_name",
    "canonical_agent_id", "canonical_agent_name"
).write.mode("overwrite").format("delta").option("overwriteSchema","true") \
 .saveAsTable("ctm_agent_crosswalk")

# Remap flat_df: replace agent_id/name/email with canonical
flat_df = (flat_df
    .join(agent_xwalk.select(
            "agent_id",
            "canonical_agent_id", "canonical_agent_name", "canonical_agent_email"),
          "agent_id", "left")
    .withColumn("agent_id",   F.coalesce("canonical_agent_id", "agent_id"))
    .withColumn("agent_name", F.coalesce("canonical_agent_name", "agent_name"))
    .withColumn("agent_email",F.coalesce("canonical_agent_email","agent_email"))
    .drop("canonical_agent_id","canonical_agent_name","canonical_agent_email"))

print("Agent identities normalized.")


# CELL ********************

# --- classify event_type and outcome ---
classified_df = flat_df.withColumn(
    "event_type",
    F.when(F.col("direction").isin("msg_inbound", "msg_outbound"), "sms")
     .when(F.col("direction") == "form", "form_fill")
     .when(F.col("direction") == "chat", "chat")
     .when(F.col("direction").isin("inbound", "outbound"), "voice")
     .otherwise("other")
).withColumn(
    "is_voice",
    F.col("event_type") == "voice"
).withColumn(
    "outcome",
    # answered: status=answered AND talk_time > 0
    F.when(
        (F.col("event_type") == "voice") &
        (F.col("status") == "answered") &
        (F.col("talk_time") > 0),
        "answered"
    )
    # voicemail: status=answered but no talk
    .when(
        (F.col("event_type") == "voice") &
        (F.col("status") == "answered") &
        (F.col("talk_time") == 0),
        "voicemail"
    )
    # quick_hangup: inbound hangup with short ring
    .when(
        (F.col("event_type") == "voice") &
        (F.col("direction") == "inbound") &
        (F.col("status") == "hangup") &
        (F.col("ring_time") < 10),
        "quick_hangup"
    )
    # abandoned_on_hold: inbound hangup with longer ring
    .when(
        (F.col("event_type") == "voice") &
        (F.col("direction") == "inbound") &
        (F.col("status") == "hangup") &
        (F.col("ring_time") >= 10),
        "abandoned_on_hold"
    )
    # missed: inbound no-connect
    .when(
        (F.col("event_type") == "voice") &
        (F.col("direction") == "inbound") &
        (F.col("status").isin("no answer", "busy", "canceled", "failed", "unreachable")),
        "missed"
    )
    # no_contact: outbound no-connect (distinct from inbound missed)
    .when(
        (F.col("event_type") == "voice") &
        (F.col("direction") == "outbound") &
        (F.col("status").isin("no answer", "busy", "canceled", "failed", "unreachable")),
        "no_contact"
    )
    .otherwise(F.lit("other"))
).withColumn(
    "is_inbound_missed",
    F.col("outcome").isin("missed", "voicemail", "abandoned_on_hold")
).withColumn(
    "needs_callback",
    F.col("is_inbound_missed")   # same rule for now; excludes quick_hangup
)

# CELL ********************

# --- load facility mapping from xlsx ---
# Fabric lakehouse files are accessible via the /lakehouse/default/Files mount
import pandas as pd

mapping_local_path = "/lakehouse/default/Files/CTM Mappings/CTM_Facility_Mapping_FINAL.xlsx"
mapping_pd = pd.read_excel(mapping_local_path, sheet_name="ctm_facility_mapping")

# Normalize
mapping_pd["source_name"] = mapping_pd["source_name"].fillna("").astype(str).str.strip()
mapping_pd["facility"] = mapping_pd["facility"].astype(str).str.strip()
mapping_pd["account_id"] = mapping_pd["account_id"].astype(int)

mapping_df = spark.createDataFrame(mapping_pd).select(
    F.col("account_id").cast("int"),
    "account_name",
    F.col("source_name").cast("string"),
    F.col("num_tracking_numbers").cast("int"),
    "facility",
    "notes",
)

print(f"Facility mapping rules: {mapping_df.count()}")
mapping_df.groupBy("facility").count().orderBy(F.col("count").desc()).show(25, truncate=False)

# Persist the mapping as a Delta table
mapping_df.write.mode("overwrite").format("delta").saveAsTable("ctm_facility_mapping")

# CELL ********************

# --- join facility mapping ---
# Left join on (account_id, source_name). Calls with null/unmatched source -> facility = NULL
calls_with_facility = classified_df.alias("c").join(
    mapping_df.select(
        F.col("account_id").alias("m_account_id"),
        F.col("source_name").alias("m_source_name"),
        F.col("facility").alias("m_facility"),
    ).alias("m"),
    (F.col("c.account_id") == F.col("m.m_account_id")) &
    (F.coalesce(F.col("c.source"), F.lit("")) == F.col("m.m_source_name")),
    "left",
).select(
    "c.*",
    F.col("m.m_facility").alias("facility"),
)

# --- facility_resolved: re-attribute outbound calls via tracking_label parsing ---
# Outbound labels like "RNGA Outbound", "GLR Outbound", "LW Outbound" etc.
# Try to match the prefix against a list of known facility codes.
# Outbound re-attribution patterns. Matched case-insensitively against
# tracking_label by parse_facility_from_label_expr (which uppercases both
# sides). \b word boundaries mean these match "RNGA Outbound", "RNGA -
# Outbound", and "(Labeled Potential Spam) RNGA - Outbound" alike.
# NOTE: facility-coded labels only. Geographic labels (Main Line, 904
# Outbound, "North Carolina Outbound", state names) are intentionally left
# unmapped -> they fall through to "Outbound Calls" pending a deliberate rule.
LABEL_PATTERNS = [
    (r"\bRNGA\b",                 "RNGA"),
    (r"\bTED\b",                  "Tides Edge"),
    (r"\b(?:LB|Longbranch)\b",    "Longbranch"),
    (r"\bEDTC\b",                 "EDTC"),
    (r"\b(?:LW|Lotus)\b",         "Lotus"),
    (r"\b(?:CRC|Chattanooga)\b",  "Chattanooga"),
    (r"\b(?:GLR|Graceland)\b",    "Graceland"),
    (r"\b(?:GAW|Green Acres)\b",  "Green Acres"),   # was MISSING - 606 calls
    (r"\b(?:BR|Beaches)\b",       "Tides Edge"),    # Beaches rebranded
    (r"\b(?:WW|Widespread)\b",    "Widespread"),
]

def parse_facility_from_label_expr(label_col):
    """Chain of when() for label regex matching."""
    expr = F.lit(None).cast("string")
    # Build in reverse so earlier patterns take precedence via nested when()
    for pattern, facility in reversed(LABEL_PATTERNS):
        expr = F.when(
            F.upper(F.coalesce(label_col, F.lit(""))).rlike(pattern.upper()),
            F.lit(facility)
        ).otherwise(expr)
    return expr

calls_with_facility = calls_with_facility.withColumn(
    "facility_resolved",
    F.when(
        F.col("facility") == "Outbound Calls",
        F.coalesce(parse_facility_from_label_expr(F.col("tracking_label")), F.lit("Outbound Calls"))
    ).otherwise(F.col("facility"))
)

# CELL ********************

# --- compute callback metrics ---
# Strategy: for every inbound call that needs_callback, find the FIRST outbound
# voice call to the same caller_number within CALLBACK_LOOKAHEAD_DAYS.
#
# We partition by caller_number and walk forward in time.

missed_calls = calls_with_facility.filter(F.col("needs_callback") == True).select(
    F.col("id").alias("m_id"),
    F.col("caller_number").alias("m_caller_number"),
    F.col("called_at").alias("m_called_at"),
    F.col("unix_time").alias("m_unix_time"),
    F.col("agent_id").alias("m_agent_id"),
)

# Candidate callbacks: outbound voice, talk+ring >= CALLBACK_MIN_DURATION_SEC
outbound_candidates = calls_with_facility.filter(
    (F.col("event_type") == "voice") &
    (F.col("direction") == "outbound") &
    ((F.coalesce(F.col("talk_time"), F.lit(0)) + F.coalesce(F.col("ring_time"), F.lit(0))) >= CALLBACK_MIN_DURATION_SEC)
).select(
    F.col("id").alias("cb_id"),
    F.col("caller_number").alias("cb_caller_number"),
    F.col("called_at").alias("cb_called_at"),
    F.col("unix_time").alias("cb_unix_time"),
    F.col("agent_id").alias("cb_agent_id"),
    F.col("outcome").alias("cb_outcome"),
)

# Join: missed + outbound on caller_number where outbound is after missed and within window
lookahead_seconds = CALLBACK_LOOKAHEAD_DAYS * 24 * 60 * 60

joined = missed_calls.join(
    outbound_candidates,
    (F.col("m_caller_number") == F.col("cb_caller_number")) &
    (F.col("cb_unix_time") > F.col("m_unix_time")) &
    (F.col("cb_unix_time") <= F.col("m_unix_time") + F.lit(lookahead_seconds)),
    "left"
)

# Take the EARLIEST callback per missed call
callback_w = Window.partitionBy("m_id").orderBy(F.col("cb_unix_time").asc())
callbacks_first = joined.withColumn("_rn", F.row_number().over(callback_w)).filter("_rn = 1").drop("_rn")

# Build the callback attributes
now_unix = int(datetime.now(timezone.utc).timestamp())
pending_seconds = PENDING_HOURS * 3600

callback_attrs = callbacks_first.select(
    F.col("m_id").alias("id"),
    F.col("cb_id").alias("callback_call_id"),
    F.col("cb_agent_id").alias("callback_by_agent_id"),
    F.col("m_agent_id"),
    F.col("cb_unix_time"),
    F.col("m_unix_time"),
    F.col("cb_outcome"),
).withColumn(
    "callback_status",
    F.when(F.col("callback_call_id").isNotNull(), F.lit("called_back"))
     .when(F.col("m_unix_time") > F.lit(now_unix - pending_seconds), F.lit("pending"))
     .otherwise(F.lit("not_called_back"))
).withColumn(
    "callback_minutes",
    F.when(
        F.col("callback_call_id").isNotNull(),
        ((F.col("cb_unix_time") - F.col("m_unix_time")) / 60.0).cast("int")
    )
).withColumn(
    "callback_by_same_agent",
    F.when(
        F.col("callback_call_id").isNotNull() & F.col("callback_by_agent_id").isNotNull() & F.col("m_agent_id").isNotNull(),
        F.col("callback_by_agent_id") == F.col("m_agent_id")
    )
).withColumn(
    "callback_bucket",
    F.when(F.col("callback_status") == "pending", F.lit("pending"))
     .when(F.col("callback_status") == "not_called_back", F.lit("not_called_back"))
     .when(F.col("callback_minutes") < 15, F.lit("<15min"))
     .when(F.col("callback_minutes") < 60, F.lit("15min-1hr"))
     .when(F.col("callback_minutes") < 1440, F.lit("1hr-24hr"))
     .otherwise(F.lit("1d-7d"))
).withColumn(
    "callback_connected",
    F.when(F.col("callback_call_id").isNotNull(), F.col("cb_outcome") == F.lit("answered"))
).select(
    "id",
    "callback_status", "callback_call_id", "callback_minutes",
    F.col("callback_by_agent_id"),
    "callback_by_same_agent",
    "callback_bucket",
    "callback_connected",
)

# Merge callback attrs into the main dataframe
ctm_calls_final = calls_with_facility.alias("c").join(
    callback_attrs.alias("cb"),
    "id", "left"
).withColumn(
    "callback_status",
    F.coalesce(F.col("callback_status"), F.lit("no_callback_needed"))
)

# Move partition column to be stable and cache
ctm_calls_final = ctm_calls_final.repartition("call_date")
ctm_calls_final.cache()
print(f"Final row count: {ctm_calls_final.count():,}")

# CELL ********************

# --- write ctm_calls_raw ---
ctm_calls_final.write.mode("overwrite") \
    .format("delta") \
    .partitionBy("call_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ctm_calls_raw")

print("ctm_calls_raw written")

# CELL ********************

# --- daily facility stats ---
facility_stats = ctm_calls_final.groupBy("call_date", "facility_resolved", "account_slug").agg(
    F.count("*").alias("total_events"),
    F.sum(F.when(F.col("is_voice"), 1).otherwise(0)).alias("voice_total"),
    F.sum(F.when(F.col("outcome") == "answered", 1).otherwise(0)).alias("voice_answered"),
    F.sum(F.when(F.col("outcome") == "missed", 1).otherwise(0)).alias("voice_missed"),
    F.sum(F.when(F.col("outcome") == "voicemail", 1).otherwise(0)).alias("voice_voicemail"),
    F.sum(F.when(F.col("outcome") == "quick_hangup", 1).otherwise(0)).alias("voice_quick_hangup"),
    F.sum(F.when(F.col("outcome") == "abandoned_on_hold", 1).otherwise(0)).alias("voice_abandoned"),
    F.sum(F.when(F.col("direction") == "inbound", 1).otherwise(0)).alias("inbound_total"),
    F.sum(F.when(F.col("direction") == "outbound", 1).otherwise(0)).alias("outbound_total"),
    F.sum(F.when(F.col("event_type") == "sms", 1).otherwise(0)).alias("sms_total"),
    F.sum(F.when(F.col("event_type") == "form_fill", 1).otherwise(0)).alias("form_total"),
    F.sum(F.coalesce(F.col("talk_time"), F.lit(0))).alias("talk_time_seconds"),
    F.sum(F.when(F.col("needs_callback"), 1).otherwise(0)).alias("callbacks_needed"),
    F.sum(F.when(F.col("callback_status") == "called_back", 1).otherwise(0)).alias("callbacks_made_any"),
    F.sum(F.when(F.col("callback_by_same_agent") == True, 1).otherwise(0)).alias("callbacks_made_same_agent"),
).withColumnRenamed("facility_resolved", "facility")

facility_stats = facility_stats.withColumn(
    "answer_rate_pct",
    F.when(F.col("voice_total") > 0,
           F.round(100.0 * F.col("voice_answered") / F.col("voice_total"), 2))
).withColumn(
    "miss_rate_pct",
    F.when(F.col("inbound_total") > 0,
           F.round(100.0 * (F.col("voice_missed") + F.col("voice_voicemail") + F.col("voice_abandoned")) / F.col("inbound_total"), 2))
).withColumn(
    "callback_rate_any_pct",
    F.when(F.col("callbacks_needed") > 0,
           F.round(100.0 * F.col("callbacks_made_any") / F.col("callbacks_needed"), 2))
).withColumn(
    "callback_rate_same_pct",
    F.when(F.col("callbacks_needed") > 0,
           F.round(100.0 * F.col("callbacks_made_same_agent") / F.col("callbacks_needed"), 2))
)

facility_stats.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("ctm_daily_facility_stats")
print("ctm_daily_facility_stats written")

# CELL ********************

# --- daily agent stats ---
# Limit to rows that have a real agent (skip generic IVR routes)
agent_stats_base = ctm_calls_final.filter(
    F.col("agent_id").isNotNull() &
    F.col("is_voice")
)

agent_stats = agent_stats_base.groupBy("call_date", "agent_id", "agent_name", "agent_email").agg(
    F.sum(F.when(F.col("outcome") == "answered", 1).otherwise(0)).alias("calls_answered"),
    F.sum(F.when(F.col("is_inbound_missed"), 1).otherwise(0)).alias("calls_missed_assigned"),
    F.sum(F.coalesce(F.col("talk_time"), F.lit(0))).alias("talk_time_seconds"),
    F.count("*").alias("total_voice_calls"),
)

# Callbacks made, joined separately on callback_by_agent_id
callbacks_by_agent = ctm_calls_final.filter(F.col("callback_by_agent_id").isNotNull()) \
    .groupBy("call_date", F.col("callback_by_agent_id").alias("agent_id")) \
    .agg(F.count("*").alias("callbacks_made"))

agent_stats = agent_stats.alias("a").join(
    callbacks_by_agent.alias("cb"),
    (F.col("a.call_date") == F.col("cb.call_date")) & (F.col("a.agent_id") == F.col("cb.agent_id")),
    "left"
).select(
    "a.call_date", "a.agent_id", "a.agent_name", "a.agent_email",
    "a.calls_answered", "a.calls_missed_assigned",
    F.coalesce(F.col("cb.callbacks_made"), F.lit(0)).alias("callbacks_made"),
    "a.talk_time_seconds", "a.total_voice_calls",
).withColumn(
    "avg_talk_time_seconds",
    F.when(F.col("calls_answered") > 0,
           F.round(F.col("talk_time_seconds") / F.col("calls_answered"), 1))
).withColumn(
    "answer_rate_pct",
    F.when(F.col("total_voice_calls") > 0,
           F.round(100.0 * F.col("calls_answered") / F.col("total_voice_calls"), 2))
)

agent_stats.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("ctm_daily_agent_stats")
print("ctm_daily_agent_stats written")

# CELL ********************

# --- unmapped detection + log ---
# Any call where facility IS NULL is unmapped. Aggregate by (account_id, source).
unmapped_today = ctm_calls_final.filter(F.col("facility").isNull()).groupBy(
    "account_id", "account_slug", "source"
).agg(
    F.count("*").alias("call_count"),
    F.min("call_date").alias("first_seen_date"),
    F.max("call_date").alias("last_seen_date"),
    F.first("tracking_label", ignorenulls=True).alias("sample_tracking_label"),
).withColumn(
    "source_for_mapping",
    F.coalesce(F.col("source"), F.lit(""))
).withColumn(
    "detected_at",
    F.lit(datetime.now(timezone.utc).isoformat())
)

# Load existing unmapped log so we can detect NEW sources
existing = []
try:
    existing_df = spark.read.format("delta").table("ctm_unmapped_log")
    existing = [r["source_for_mapping"] + "||" + str(r["account_id"])
                for r in existing_df.select("source_for_mapping", "account_id").distinct().collect()]
    print(f"Existing unmapped log has {len(existing)} source/account combinations")
except Exception as e:
    print(f"No existing ctm_unmapped_log yet (first run): {e}")

# Split into new vs previously-seen
new_unmapped = unmapped_today.filter(
    ~F.concat(F.col("source_for_mapping"), F.lit("||"), F.col("account_id").cast("string")).isin(existing)
)

new_unmapped_count = new_unmapped.count()
print(f"Unmapped sources total: {unmapped_today.count()} | NEW this run: {new_unmapped_count}")

# Write/merge the log
if unmapped_today.count() > 0:
    unmapped_today.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("ctm_unmapped_log")
    print("ctm_unmapped_log written")

# CELL ********************

# --- SendGrid alert if NEW unmapped sources exist ---
if new_unmapped_count > 0:
    # Fetch API key from Key Vault
    sg_key = notebookutils.credentials.getSecret(KEY_VAULT_URL, "SENDGRID-API-KEY")

    rows = new_unmapped.orderBy(F.col("call_count").desc()).collect()
    table_rows = ""
    for r in rows:
        src_display = r["source"] or "(empty)"
        table_rows += f"""
          <tr>
            <td style='padding:6px;border:1px solid #ccc'>{r['account_slug']}</td>
            <td style='padding:6px;border:1px solid #ccc'>{src_display}</td>
            <td style='padding:6px;border:1px solid #ccc;text-align:right'>{r['call_count']}</td>
            <td style='padding:6px;border:1px solid #ccc'>{r['sample_tracking_label'] or ''}</td>
            <td style='padding:6px;border:1px solid #ccc'>{r['first_seen_date']}</td>
          </tr>"""

    html_body = f"""
    <html><body style='font-family:Arial,sans-serif;font-size:13px'>
      <p><b>CTM pipeline detected {new_unmapped_count} new unmapped source(s)</b> in the latest run.</p>
      <p>These calls will show as facility = NULL (Unmapped) in the dashboard until you add them to <code>CTM_Facility_Mapping_FINAL.xlsx</code>.</p>
      <table style='border-collapse:collapse'>
        <thead>
          <tr style='background:#305496;color:#fff'>
            <th style='padding:6px;border:1px solid #ccc'>Account</th>
            <th style='padding:6px;border:1px solid #ccc'>Source name</th>
            <th style='padding:6px;border:1px solid #ccc'>Call count</th>
            <th style='padding:6px;border:1px solid #ccc'>Sample tracking label</th>
            <th style='padding:6px;border:1px solid #ccc'>First seen</th>
          </tr>
        </thead>
        <tbody>{table_rows}</tbody>
      </table>
      <p style='color:#888;font-size:11px'>Sent by Transform CTM Data notebook in ctm_lakehouse.</p>
    </body></html>
    """

    import requests
    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={
            "Authorization": f"Bearer {sg_key}",
            "Content-Type": "application/json",
        },
        json={
            "personalizations": [{"to": [{"email": ALERT_TO_EMAIL}]}],
            "from": {"email": ALERT_FROM_EMAIL, "name": "CTM Pipeline"},
            "subject": ALERT_SUBJECT,
            "content": [{"type": "text/html", "value": html_body}],
        },
        timeout=15,
    )
    if resp.status_code in (200, 202):
        print(f"SendGrid alert sent to {ALERT_TO_EMAIL}")
    else:
        print(f"SendGrid send failed: {resp.status_code} {resp.text[:300]}")
else:
    print("No new unmapped sources - no alert sent")

# CELL ********************

# --- final summary ---
print("=" * 70)
print("CTM TRANSFORM SUMMARY")
print("=" * 70)
print(f"ctm_calls_raw rows: {ctm_calls_final.count():,}")
print(f"Total unmapped events: {ctm_calls_final.filter(F.col('facility').isNull()).count():,}")
print()
print("Facility distribution:")
ctm_calls_final.groupBy("facility_resolved").count().orderBy(F.col("count").desc()).show(25, truncate=False)

# CELL ********************

# --- data completeness audit ---
from pyspark.sql import functions as F

print("=" * 70)
print("DATE COVERAGE AUDIT")
print("=" * 70)

# Per-account date range + day-level counts
coverage = (
    spark.table("ctm_calls_raw")
    .groupBy("account_slug", "call_date")
    .agg(F.count("*").alias("events"))
    .orderBy("account_slug", "call_date")
)

# Check for gaps: generate expected date sequence per account, left-join actuals
for slug in ["longbranch", "recover_now"]:
    print(f"\n--- {slug} ---")

    account_df = coverage.filter(F.col("account_slug") == slug)

    min_date, max_date = account_df.agg(
        F.min("call_date"), F.max("call_date")
    ).first()

    if min_date is None:
        print(f"  NO DATA for {slug}")
        continue

    print(f"  Date range: {min_date} -> {max_date}")

    # Build expected date sequence
    expected = spark.sql(f"""
        SELECT sequence(date('{min_date}'), date('{max_date}'), interval 1 day) as d
    """).select(F.explode("d").alias("call_date"))

    # Left join to find missing days
    joined = expected.alias("e").join(
        account_df.alias("a"),
        F.col("e.call_date") == F.col("a.call_date"),
        "left"
    ).select(
        F.col("e.call_date").alias("call_date"),
        F.coalesce(F.col("a.events"), F.lit(0)).alias("events"),
    ).orderBy("call_date")

    total_days = joined.count()
    missing_days = joined.filter(F.col("events") == 0).count()
    low_days = joined.filter((F.col("events") > 0) & (F.col("events") < 10)).count()

    print(f"  Total days in range: {total_days}")
    print(f"  Days with zero events: {missing_days}")
    print(f"  Days with < 10 events (suspicious): {low_days}")

    if missing_days > 0:
        print(f"\n  MISSING DAYS:")
        joined.filter(F.col("events") == 0).show(50, truncate=False)

    if low_days > 0:
        print(f"\n  LOW-VOLUME DAYS:")
        joined.filter((F.col("events") > 0) & (F.col("events") < 10)).show(20, truncate=False)

    if missing_days == 0 and low_days == 0:
        print(f"  No gaps or suspiciously low days")

# CELL ********************

# --- DASHBOARD STATE EXPORT ---
# Reads from ctm_calls_final (already in scope) and writes the admissions
# dashboard payload to ctm-bronze/dashboard/admissions_latest.json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import json
import re

REPORT_TZ = ZoneInfo("America/Chicago")

# -- helpers --
def slugify(name):
    """Turn a human-readable name into a stable URL-safe key."""
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-") or "unknown"

def classify_channel(source_name):
    """Map a CTM source name to a marketing channel."""
    if not source_name:
        return "Unknown"
    s = " ".join(source_name.lower().split())  # normalize multi-space ("PPC  LW")

    # Outbound (separate channel - calls placed BY reps, not driven by marketing)
    if s == "outbound calls":
        return "Outbound"

    # Chat
    if "ai agent" in s or "tawk to" in s:
        return "Chat"

    # Operational / parked buckets - small volume, not marketing-driven
    if any(x in s for x in ["billing number", "metro atlanta treatment",
                            "3rd mh", "pre assessments", "other static"]):
        return "Other"
    if s == "mat":
        return "Other"

    # PPC = all paid digital (search + social + display)
    if (s.startswith("ppc ") or s.startswith("ppc-") or s.startswith("meta ") or
        any(x in s for x in ["google ads", "facebook paid", "facebook cta",
                             "fb ads", "geofencing", "linkedin",
                             "pmax", "performance max"])):
        return "PPC"

    # Print = paid offline. Tiny volume; lump with Other for v1.
    if s == "print":
        return "Other"

    # Organic search + GMB
    if any(x in s for x in ["organic", "seo", "gmb", "bing local"]):
        return "Organic"

    # Referrals + intra-network facility transfers
    if any(x in s for x in ["referral", "facility transfer"]):
        return "Referral"

    # Direct (brochures, type-in, branded target numbers)
    if any(x in s for x in ["direct", "target number", "website"]):
        return "Direct"

    # Branded facility names = main published numbers -> Direct
    BRANDED_FACILITY_NAMES = {
        "eating disorder treatment centers", "longbranch recovery",
        "recover now greater atlanta", "chattanooga recovery center",
        "graceland recovery", "green acres wellness", "lotus wellness",
        "tides edge detox", "beaches recovery", "widespread wellness",
        "recover now",
    }
    if s in BRANDED_FACILITY_NAMES:
        return "Direct"

    return "Other"

# Real treatment facilities - used to filter facility pills.
REAL_FACILITIES = {
    "Longbranch", "EDTC", "RNGA", "Tides Edge", "Lotus",
    "Chattanooga", "Graceland", "Green Acres", "Widespread",
}

def fmt_seconds(s):
    if not s or s <= 0:
        return "0m"
    h, rem = divmod(int(s), 3600)
    m, _ = divmod(rem, 60)
    return f"{h}h {m:02d}m" if h else f"{m}m"

def initials(name):
    parts = (name or "").strip().split()
    if not parts: return "-"
    if len(parts) == 1: return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()

def label_short(name):
    """First name + last initial, e.g. 'Sarah Jones' -> 'Sarah J.'"""
    parts = (name or "").strip().split()
    if not parts: return "Unknown"
    if len(parts) == 1: return parts[0]
    return f"{parts[0]} {parts[-1][0]}."

# -- time windows (America/Chicago) --
now_utc = datetime.now(timezone.utc)
now_local = now_utc.astimezone(REPORT_TZ)
today = now_local.date()

# "Latest day with data" = yesterday in our pipeline (puller pulls daily)
latest_day = today - timedelta(days=1)
prior_day  = latest_day - timedelta(days=1)

day_window_start  = latest_day - timedelta(days=13)         # 14 days inclusive
week_window_start = latest_day - timedelta(weeks=12)        # 12 weeks

# -- pull only the rows we need into a Pandas DF for ergonomic aggregation --
rolling_start = (latest_day.replace(day=1) - timedelta(days=365))
df = (
    ctm_calls_final
    .filter(F.col("call_date") >= F.lit(rolling_start))
    .filter(F.col("call_date") <= F.lit(latest_day))
    .select(
        "id", "call_date", "direction", "event_type", "is_voice",
        "outcome", "is_inbound_missed", "needs_callback",
        "callback_status", "callback_minutes", "callback_by_same_agent",
        "talk_time", "ring_time", "is_new_caller",
        "source", "tracking_label",
        "agent_id", "agent_name", "agent_email",
        "facility_resolved",
    )
    .toPandas()
)

import pandas as pd
import numpy as np

if df.empty:
    print("No CTM data available for the rolling window - emitting empty payload")
    payload = {"generated_at": now_utc.isoformat(), "data_through": str(latest_day),
               "error": "no data"}
else:
    df["call_date"] = pd.to_datetime(df["call_date"]).dt.date
    df["channel"] = df["source"].apply(classify_channel)

    # -- HERO KPIs (latest day vs prior day, with 14-day baseline) --
    latest_df = df[df["call_date"] == latest_day]
    prior_df  = df[df["call_date"] == prior_day]
    voice_latest    = latest_df[latest_df["is_voice"] == True]
    inbound_latest  = voice_latest[voice_latest["direction"] == "inbound"]

    calls_latest    = int(len(latest_df))
    calls_prior     = int(len(prior_df))
    calls_14d_avg   = int(round(len(df[df["call_date"] >= day_window_start]) / 14))

    inbound_total_l = int(len(inbound_latest))
    missed_count    = int(inbound_latest["is_inbound_missed"].sum())
    miss_rate_pct   = round(100.0 * missed_count / inbound_total_l, 1) if inbound_total_l else 0

    # Median callback time across missed calls that were called back
    cb_minutes = latest_df.loc[
        latest_df["callback_status"] == "called_back", "callback_minutes"
    ].dropna()
    median_cb_min = int(cb_minutes.median()) if len(cb_minutes) else None
    callbacks_made   = int((latest_df["callback_status"] == "called_back").sum())
    callbacks_needed = int(latest_df["needs_callback"].sum())

    # PPC volume (latest day)
    ppc_latest = latest_df[latest_df["channel"] == "PPC"]
    ppc_count  = int(len(ppc_latest))
    ppc_pct    = round(100.0 * ppc_count / calls_latest, 1) if calls_latest else 0

    # PPC last 7 vs prior 7
    last7_start  = latest_day - timedelta(days=6)
    prior7_start = latest_day - timedelta(days=13)
    prior7_end   = latest_day - timedelta(days=7)
    ppc_last7   = int(len(df[(df["call_date"] >= last7_start) & (df["channel"] == "PPC")]))
    ppc_prior7  = int(len(df[(df["call_date"] >= prior7_start) & (df["call_date"] <= prior7_end) & (df["channel"] == "PPC")]))
    ppc_wow_pct = round(100.0 * (ppc_last7 - ppc_prior7) / ppc_prior7, 1) if ppc_prior7 else 0

    # PPC miss rate (latest day, inbound only)
    ppc_inbound = ppc_latest[(ppc_latest["is_voice"]) & (ppc_latest["direction"] == "inbound")]
    ppc_miss_rate = round(100.0 * ppc_inbound["is_inbound_missed"].sum() / len(ppc_inbound), 1) if len(ppc_inbound) else 0

    hero = {
        "data_through":          str(latest_day),
        "calls_today":           calls_latest,
        "calls_today_delta_pct": round(100.0 * (calls_latest - calls_prior) / calls_prior, 1) if calls_prior else 0,
        "calls_14d_avg":         calls_14d_avg,
        "miss_rate_pct":         miss_rate_pct,
        "miss_rate_target":      10,
        "missed_count":          missed_count,
        "inbound_total":         inbound_total_l,
        "median_callback_min":   median_cb_min,
        "callback_target_min":   60,
        "callbacks_made":        callbacks_made,
        "callbacks_needed":      callbacks_needed,
        "ppc_calls":             ppc_count,
        "ppc_pct_of_total":      ppc_pct,
        "ppc_wow_pct":           ppc_wow_pct,
        "ppc_miss_rate_pct":     ppc_miss_rate,
    }

    # -- SOURCE TRENDS (day, week, month) --
    def make_day_series(sub):
        days = [day_window_start + timedelta(days=i) for i in range(14)]
        s = sub.groupby("call_date").size()
        return [int(s.get(d, 0)) for d in days]

    def make_week_series(sub):
        sub = sub.copy()
        sub["week_end"] = sub["call_date"].apply(
            lambda d: d + timedelta(days=(6 - d.weekday()) % 7)
        )
        last_we = latest_day + timedelta(days=(6 - latest_day.weekday()) % 7)
        weeks = [last_we - timedelta(weeks=(11 - i)) for i in range(12)]
        s = sub.groupby("week_end").size()
        return [int(s.get(w, 0)) for w in weeks]

    def make_month_series(sub):
        sub = sub.copy()
        sub["month_end"] = sub["call_date"].apply(
            lambda d: (pd.Timestamp(d).to_period("M").to_timestamp() + pd.offsets.MonthEnd(0)).date()
        )
        latest_month_end = (pd.Timestamp(latest_day).to_period("M").to_timestamp()
                            + pd.offsets.MonthEnd(0)).date()
        months = []
        cursor = latest_month_end
        for i in range(12):
            months.append(cursor)
            first_of_this = cursor.replace(day=1)
            cursor = first_of_this - timedelta(days=1)
        months = list(reversed(months))
        s = sub.groupby("month_end").size()
        return [int(s.get(m, 0)) for m in months]

    # -- FACILITY TRENDS --
    last14 = df[df["call_date"] >= day_window_start]
    facility_volumes = (
        last14[last14["facility_resolved"].isin(REAL_FACILITIES)]
        .groupby("facility_resolved").size().sort_values(ascending=False)
    )

    source_data  = {}
    source_pills = [{"key": "all", "label": "All facilities", "alarm": False}]

    source_data["all"] = {
        "day":   make_day_series(df),
        "week":  make_week_series(df),
        "month": make_month_series(df),
    }

    for fac_name in facility_volumes.index:
        key = slugify(fac_name)
        sub = df[df["facility_resolved"] == fac_name]
        day_series  = make_day_series(sub)
        week_series = make_week_series(sub)
        last7  = sum(day_series[-7:])
        prior7 = sum(day_series[:7])
        is_alarm = prior7 > 0 and (last7 - prior7) / prior7 < -0.15
        source_data[key] = {
            "day":   day_series,
            "week":  week_series,
            "month": make_month_series(sub),
        }
        source_pills.append({
            "key":   key,
            "label": fac_name,
            "alarm": bool(is_alarm),
        })

    # -- REP TRENDS (day, week, month) --
    rep_df = df[df["agent_id"].notna() & (df["is_voice"] == True) &
                (df["outcome"] == "answered")]
    top_reps = (
        rep_df.groupby(["agent_id", "agent_name", "agent_email"]).size()
              .sort_values(ascending=False).head(10).reset_index()
    )

    rep_data  = {}
    rep_pills = [{"key": "all", "label": "All reps", "alarm": False}]

    rep_data["all"] = {
        "day":   make_day_series(rep_df),
        "week":  make_week_series(rep_df),
        "month": make_month_series(rep_df),
    }

    for _, row in top_reps.iterrows():
        agent_id   = row["agent_id"]
        agent_name = row["agent_name"] or "Unknown"
        key = slugify(agent_name) or f"agent-{agent_id}"
        sub = rep_df[rep_df["agent_id"] == agent_id]
        day_series = make_day_series(sub)
        last7  = sum(day_series[-7:])
        prior7 = sum(day_series[:7])
        is_alarm = prior7 > 0 and (last7 - prior7) / prior7 < -0.15
        rep_data[key] = {
            "day":   day_series,
            "week":  make_week_series(sub),
            "month": make_month_series(sub),
        }
        rep_pills.append({
            "key":     key,
            "label":   label_short(agent_name),
            "alarm":   bool(is_alarm),
        })

    # -- REP TABLE (today) --
    today_reps_voice = latest_df[latest_df["agent_id"].notna() & latest_df["is_voice"]]
    week_reps_voice  = df[(df["call_date"] >= last7_start) &
                          df["agent_id"].notna() & df["is_voice"]]
    rep_table = []
    for _, row in top_reps.iterrows():
        agent_id   = row["agent_id"]
        agent_name = row["agent_name"] or "Unknown"
        today_sub = today_reps_voice[today_reps_voice["agent_id"] == agent_id]
        week_sub  = week_reps_voice[week_reps_voice["agent_id"] == agent_id]
        answered_today = int((today_sub["outcome"] == "answered").sum())
        answered_week  = int((week_sub["outcome"]  == "answered").sum())
        missed_today   = int(today_sub["is_inbound_missed"].sum())
        talk_seconds   = int(today_sub["talk_time"].fillna(0).sum())
        answered_talk  = today_sub.loc[today_sub["outcome"] == "answered", "talk_time"].fillna(0)
        avg_handle_sec = int(answered_talk.mean()) if len(answered_talk) > 0 else 0
        callbacks_made = int((today_sub["callback_status"] == "called_back").sum())
        same_agent_cb  = int((today_sub["callback_by_same_agent"] == True).sum())
        total_voice    = int(len(today_sub))
        answer_rate    = round(100.0 * answered_today / total_voice, 1) if total_voice else 0
        rep_table.append({
            "name":          label_short(agent_name),
            "initials":      initials(agent_name),
            "answered":      answered_today,
            "answered_week": answered_week,
            "missed":        missed_today,
            "talk_time_str": fmt_seconds(talk_seconds),
            "avg_handle_str": fmt_seconds(avg_handle_sec),
            "callbacks_made": callbacks_made,
            "same_agent_cb":  same_agent_cb,
            "answer_rate_pct": answer_rate,
        })
    rep_table.sort(key=lambda r: r["answered"], reverse=True)
    if rep_table:
        rep_table[0]["leader"] = True

    # -- TOP SOURCES list (latest day) --
    today_sources = (
        latest_df[latest_df["source"].notna() & (latest_df["source"] != "")]
        .groupby(["source", "channel"]).size().sort_values(ascending=False).head(10)
        .reset_index(name="count")
    )
    max_src = today_sources["count"].max() if not today_sources.empty else 1
    top_sources_list = []
    for _, r in today_sources.iterrows():
        sub = latest_df[(latest_df["source"] == r["source"]) & latest_df["is_voice"] &
                        (latest_df["direction"] == "inbound")]
        miss_pct = round(100.0 * sub["is_inbound_missed"].sum() / len(sub), 1) if len(sub) else 0
        top_sources_list.append({
            "name":       r["source"],
            "channel":    r["channel"],
            "count":      int(r["count"]),
            "bar_pct":    round(100.0 * r["count"] / max_src, 1),
            "miss_rate_pct": miss_pct,
        })

    # -- CHANNEL MIX (latest day) --
    channel_mix_raw = latest_df.groupby("channel").size().to_dict()
    total_for_pct = sum(channel_mix_raw.values()) or 1
    channel_order = ["PPC", "Organic", "Direct", "Referral", "Outbound", "Chat", "Other", "Unknown"]
    channel_mix = []
    for ch in channel_order:
        if ch in channel_mix_raw:
            channel_mix.append({
                "label": ch, "count": int(channel_mix_raw[ch]),
                "pct":   round(100.0 * channel_mix_raw[ch] / total_for_pct, 1),
            })

    # -- RULE-BASED COMMENTARY --
    src_drops = [p for p in source_pills if p.get("alarm")]
    rep_drops = [p for p in rep_pills if p.get("alarm")]

    src_commentary = None
    if src_drops:
        bits = []
        for p in src_drops[:3]:
            d = source_data[p["key"]]["day"]
            last7  = sum(d[-7:])
            prior7 = sum(d[:7])
            pct    = round(100.0 * (last7 - prior7) / prior7) if prior7 else 0
            bits.append(f"{p['label']} {pct:+d}%")
        word = "facility" if len(src_drops) == 1 else "facilities"
        src_commentary = f"{len(src_drops)} {word} down sharply week over week - " + " . ".join(bits)

    rep_commentary = None
    if rep_drops:
        bits = []
        for p in rep_drops[:3]:
            d = rep_data[p["key"]]["day"]
            last7  = sum(d[-7:])
            prior7 = sum(d[:7])
            pct    = round(100.0 * (last7 - prior7) / prior7) if prior7 else 0
            zero_days = sum(1 for x in d[-7:] if x == 0)
            zero_note = f" (out {zero_days} day{'s' if zero_days != 1 else ''})" if zero_days else ""
            bits.append(f"{p['label']} {pct:+d}%{zero_note}")
        word = "rep" if len(rep_drops) == 1 else "reps"
        rep_commentary = f"{len(rep_drops)} {word} below baseline week over week - " + " . ".join(bits)

    payload = {
        "generated_at":  now_utc.isoformat(),
        "data_through":  str(latest_day),
        "hero":          hero,
        "source_data":   source_data,
        "source_pills":  source_pills,
        "rep_data":      rep_data,
        "rep_pills":     rep_pills,
        "rep_table":     rep_table,
        "top_sources":   top_sources_list,
        "channel_mix":   channel_mix,
        "commentary":    {"source": src_commentary, "rep": rep_commentary},
    }

# -- upload --
import notebookutils
from azure.storage.blob import BlobServiceClient

KEY_VAULT_URL   = "https://kv-kipu1.vault.azure.net/"
STORAGE_ACCOUNT = "stkipu001"
CONTAINER       = "ctm-bronze"
BLOB_PATH       = "dashboard/admissions_latest.json"

storage_conn = notebookutils.credentials.getSecret(KEY_VAULT_URL, "STORAGE-CONNECTION-STRING")
svc = BlobServiceClient.from_connection_string(storage_conn)
container = svc.get_container_client(CONTAINER)
container.get_blob_client(BLOB_PATH).upload_blob(
    json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8"),
    overwrite=True,
)
print(f"Uploaded admissions dashboard payload "
      f"({len(json.dumps(payload, default=str))/1024:.1f} KB) to "
      f"{STORAGE_ACCOUNT}/{CONTAINER}/{BLOB_PATH}")

# --- typed views: clean separation of event types ---
# So downstream reports never accidentally mix phone calls with texts/forms.
spark.sql("CREATE OR REPLACE VIEW ctm_voice_calls AS "
          "SELECT * FROM ctm_calls_raw WHERE event_type = 'voice'")
spark.sql("CREATE OR REPLACE VIEW ctm_text_messages AS "
          "SELECT * FROM ctm_calls_raw WHERE event_type = 'sms'")
spark.sql("CREATE OR REPLACE VIEW ctm_form_fills AS "
          "SELECT * FROM ctm_calls_raw WHERE event_type = 'form_fill'")
spark.sql("CREATE OR REPLACE VIEW ctm_chats AS "
          "SELECT * FROM ctm_calls_raw WHERE event_type = 'chat'")

# Quick counts so you can see the split
print("=== Event type split ===")
spark.table("ctm_calls_raw").groupBy("event_type").count() \
     .orderBy(F.col("count").desc()).show()

# CELL ********************

# --- form fills quick check ---
from pyspark.sql import functions as F
ctm = spark.read.table("ctm_lakehouse.ctm_calls_raw")
forms = ctm.filter(F.col("event_type") == "form_fill")
print(f"Form fills (all time): {forms.count():,}")
forms.select("call_date","source","caller_number","caller_number_bare","tracking_label","city","state") \
     .show(15, truncate=False)
print("Form fills WITH a usable phone number:")
print(forms.filter(F.col("caller_number_bare").isNotNull() | F.col("caller_number").isNotNull()).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
