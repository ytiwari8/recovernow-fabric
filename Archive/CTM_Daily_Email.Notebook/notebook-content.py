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
# META         },
# META         {
# META           "id": "d697189c-e81b-4074-89e8-86c1adfee2a6"
# META         },
# META         {
# META           "id": "68cab2d5-d6ec-47a8-a3ce-904a41379bf5"
# META         },
# META         {
# META           "id": "7d8e32d8-17fe-4c76-bb9a-3c2893720aa6"
# META         },
# META         {
# META           "id": "98a8d990-4a33-4845-b492-8359b66e5259"
# META         }
# META       ]
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

# # CTM Daily Email (7 AM CT)
#
# Aggregates the PRIOR day of CTM calls vs a same-weekday baseline (same
# weekday, prior 4 weeks), runs rule-based flags, asks Claude for an ops
# narrative, and emails via SendGrid. Runs 12:00 UTC = 7 AM Central.
#
# Sections: daily snapshot (volume, miss%, callback rate, CALLBACK SPEED,
# forms) + REP TABLE (inbound answer rate, callback rate) + daily flags +
# forms-not-called-back + AI summary.
#
# Privacy: aggregates only (counts/rates, rep names, sources, states, campaign
# labels). No caller phone numbers, names, cities, or transcripts.
#
# Secrets (kv-kipu1): ANTHROPIC-API-KEY, SENDGRID-API-KEY
# Reads: ctm_lakehouse.ctm_calls_raw   (writes nothing)

import json, requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
import notebookutils

# ── config ──
CTM_TABLE        = "ctm_lakehouse.ctm_calls_raw"
KV_URL           = "https://kv-kipu1.vault.azure.net/"
ANTHROPIC_SECRET = "ANTHROPIC-API-KEY"
SENDGRID_SECRET  = "SENDGRID-API-KEY"
ANTHROPIC_MODEL  = "claude-sonnet-4-6"   # cheap+fast for a daily job; claude-opus-4-7 for deeper analysis
FROM_EMAIL = "data@recovernow.com"
TO_EMAIL   = "data@recovernow.com"
REPORT_TZ  = ZoneInfo("America/Chicago")

DOW_LOOKBACK_WEEKS = 4     # same-weekday baseline window
FORM_CALLBACK_WINDOW_DAYS = 2
DROP_RATIO   = 0.40        # yesterday <= 40% of baseline -> flag
MIN_BASE_VOL = 2           # ignore tiny-volume noise

# ── time anchors (Central) ──
now_local  = datetime.now(timezone.utc).astimezone(REPORT_TZ)
report_day = now_local.date() - timedelta(days=1)        # yesterday
dow_dates  = [str(report_day - timedelta(weeks=w)) for w in range(1, DOW_LOOKBACK_WEEKS+1)]
load_start = report_day - timedelta(weeks=DOW_LOOKBACK_WEEKS+1)   # enough for baseline + callback lookups
print(f"report_day={report_day}  dow_baseline_dates={dow_dates}")

# ── channel classifier (mirrors classify_channel in the CTM transform) ──
def channel_expr(src):
    s = F.lower(F.regexp_replace(F.trim(F.coalesce(src, F.lit(""))), r"\s+", " "))
    return (F.when(s=="", F.lit("Unknown"))
        .when(s=="outbound calls", F.lit("Outbound"))
        .when(s.contains("ai agent")|s.contains("tawk to"), F.lit("Chat"))
        .when(s.startswith("ppc ")|s.startswith("ppc-")|s.startswith("meta ")
              |s.contains("google ads")|s.contains("facebook paid")|s.contains("facebook cta")
              |s.contains("fb ads")|s.contains("geofencing")|s.contains("linkedin")
              |s.contains("pmax")|s.contains("performance max"), F.lit("PPC"))
        .when(s.contains("organic")|s.contains("seo")|s.contains("gmb")|s.contains("bing local"), F.lit("Organic"))
        .when(s.contains("referral")|s.contains("facility transfer"), F.lit("Referral"))
        .when(s.contains("direct")|s.contains("target number")|s.contains("website"), F.lit("Direct"))
        .otherwise(F.lit("Other")))

# ── load window ──
ctm = (spark.read.table(CTM_TABLE)
       .filter(F.col("call_date") >= F.lit(str(load_start)))
       .withColumn("channel", channel_expr(F.col("source")))
       .withColumn("is_inbound_voice", F.col("is_voice") & (F.col("direction")=="inbound")))
ctm.cache()
day = ctm.filter(F.col("call_date")==F.lit(str(report_day)))

def rows(df, cols, n=200):
    return [ {c: r[c] for c in cols} for r in df.limit(n).collect() ]

# ════════ DAILY SNAPSHOT ════════
tot_all   = day.count()
tot_voice = day.filter(F.col("is_voice")).count()
tot_inb   = day.filter(F.col("is_inbound_voice")).count()
tot_forms = day.filter(F.col("event_type")=="form_fill").count()
inb_missed= day.filter(F.col("is_inbound_voice") & F.col("is_inbound_missed")).count()
miss_pct  = round(100.0*inb_missed/tot_inb,1) if tot_inb else 0.0
cb_needed = day.filter(F.col("needs_callback")).count()
cb_made   = day.filter(F.col("callback_status")=="called_back").count()
cb_rate   = round(100.0*cb_made/cb_needed,1) if cb_needed else None

# same-weekday baseline (avg of same weekday over prior 4 weeks)
dow_df = ctm.filter(F.col("call_date").isin(dow_dates))
n_dow  = max(len(dow_dates),1)
def dow_avg(cond=None):
    d = dow_df if cond is None else dow_df.filter(cond)
    return round(d.count()/n_dow,1)
base_all = dow_avg()
base_inb = dow_avg(F.col("is_inbound_voice"))

# callback speed (yesterday)
median_cb = day.filter(F.col("callback_status")=="called_back").approxQuantile("callback_minutes",[0.5],0.05)
median_cb = round(median_cb[0],0) if median_cb and median_cb[0] is not None else None
bucket_mix = rows(day.filter(F.col("callback_bucket").isNotNull())
                  .groupBy("callback_bucket").agg(F.count("*").alias("n")).orderBy("callback_bucket"),
                  ["callback_bucket","n"])

# by source vs DoW
src_day = day.groupBy("source","channel").agg(F.count("*").alias("calls"))
src_dow = dow_df.groupBy("source").agg((F.count("*")/F.lit(n_dow)).alias("base_daily"))
by_source = (src_day.join(src_dow,"source","left")
             .withColumn("base_daily",F.round(F.coalesce(F.col("base_daily"),F.lit(0.0)),1)).orderBy(F.col("calls").desc()))
by_source_list = rows(by_source, ["source","channel","calls","base_daily"], 40)

# PPC by campaign vs DoW
ppc_day = day.filter(F.col("channel")=="PPC").groupBy("source").agg(F.count("*").alias("calls"))
ppc_dow = dow_df.filter(F.col("channel")=="PPC").groupBy("source").agg((F.count("*")/F.lit(n_dow)).alias("base_daily"))
ppc_join = (ppc_day.join(ppc_dow,"source","outer")
            .withColumn("calls",F.coalesce(F.col("calls"),F.lit(0)))
            .withColumn("base_daily",F.round(F.coalesce(F.col("base_daily"),F.lit(0.0)),1)).orderBy(F.col("base_daily").desc()))
ppc_list = rows(ppc_join, ["source","calls","base_daily"], 60)

# states vs DoW
st_day = day.filter(F.col("state").isNotNull()).groupBy("state").agg(F.count("*").alias("calls"))
st_dow = dow_df.filter(F.col("state").isNotNull()).groupBy("state").agg((F.count("*")/F.lit(n_dow)).alias("base_daily"))
st_join = (st_day.join(st_dow,"state","outer")
           .withColumn("calls",F.coalesce(F.col("calls"),F.lit(0)))
           .withColumn("base_daily",F.round(F.coalesce(F.col("base_daily"),F.lit(0.0)),1)).orderBy(F.col("base_daily").desc()))
states_list = rows(st_join, ["state","calls","base_daily"], 60)

channel_mix = rows(day.groupBy("channel").agg(F.count("*").alias("calls")).orderBy(F.col("calls").desc()), ["channel","calls"])
forms_by_src= rows(day.filter(F.col("event_type")=="form_fill").groupBy("source","state").agg(F.count("*").alias("forms")).orderBy(F.col("forms").desc()), ["source","state","forms"], 40)

# REP TABLE
#  - Ans      = inbound answered
#  - Miss     = inbound missed (missed / voicemail / abandoned_on_hold)
#  - QckHng   = inbound quick-hangups (caller hung up <10s before pickup; not the rep's fault)
#  - Ans%     = Ans / (Ans + Miss)  -> 0 missed always reads 100%; quick-hangups excluded
#  - Out      = outbound voice calls placed
#  - Out-conn%= outbound that connected (answered) / total outbound
#  - CB%      = callbacks made / callbacks needed
reps_day = (day.filter(F.col("is_voice") & F.col("agent_name").isNotNull()).groupBy("agent_name").agg(
        F.sum((F.col("is_inbound_voice") & (F.col("outcome")=="answered")).cast("int")).alias("answered"),
        F.sum(F.col("is_inbound_missed").cast("int")).alias("missed"),
        F.sum((F.col("is_inbound_voice") & (F.col("outcome")=="quick_hangup")).cast("int")).alias("quick_hangup"),
        F.sum((F.col("direction")=="outbound").cast("int")).alias("outbound"),
        F.sum(((F.col("direction")=="outbound") & (F.col("outcome")=="answered")).cast("int")).alias("outbound_connected"),
        F.round(F.sum(F.coalesce(F.col("talk_time"),F.lit(0)))/3600.0,1).alias("talk_hrs"),
        F.sum((F.col("callback_status")=="called_back").cast("int")).alias("callbacks_made"),
        F.sum(F.col("needs_callback").cast("int")).alias("callbacks_needed"),
    )
    .withColumn("answer_rate_pct",
        F.when((F.col("answered")+F.col("missed"))>0,
               F.round(100.0*F.col("answered")/(F.col("answered")+F.col("missed")),1)))
    .withColumn("outbound_conn_pct",
        F.when(F.col("outbound")>0, F.round(100.0*F.col("outbound_connected")/F.col("outbound"),1)))
    .withColumn("callback_rate_pct",
        F.when(F.col("callbacks_needed")>0, F.round(100.0*F.col("callbacks_made")/F.col("callbacks_needed"),1))))
reps_dow = (dow_df.filter(F.col("is_voice") & F.col("agent_name").isNotNull()).groupBy("agent_name")
            .agg(F.round(F.sum((F.col("is_inbound_voice") & (F.col("outcome")=="answered")).cast("int"))/F.lit(n_dow),1).alias("base_answered")))
reps_join = reps_day.join(reps_dow,"agent_name","left").withColumn("base_answered",F.coalesce(F.col("base_answered"),F.lit(0.0))).orderBy(F.col("answered").desc())
reps_list = rows(reps_join, ["agent_name","answered","missed","quick_hangup","answer_rate_pct","outbound","outbound_conn_pct","callback_rate_pct","callbacks_made","callbacks_needed","talk_hrs","base_answered"], 40)

# forms not called back (yesterday)
forms_y = day.filter(F.col("event_type")=="form_fill").select(
    F.col("caller_number_bare").alias("f_num"), F.col("unix_time").alias("f_unix"), "source","state","tracking_label")
win = FORM_CALLBACK_WINDOW_DAYS*24*3600
outbound = ctm.filter((F.col("event_type")=="voice") & (F.col("direction")=="outbound")).select(
    F.col("caller_number_bare").alias("o_num"), F.col("unix_time").alias("o_unix"))
fj = forms_y.join(outbound, (F.col("f_num")==F.col("o_num")) & (F.col("o_unix")>=F.col("f_unix")) & (F.col("o_unix")<=F.col("f_unix")+F.lit(win)), "left")
form_status = fj.groupBy("f_num","source","state","tracking_label","f_unix").agg(F.max(F.col("o_unix").isNotNull().cast("int")).alias("was_called"))
not_called = form_status.filter(F.col("was_called")==0)
not_called_list = rows(not_called.select("source","state","tracking_label").orderBy("source"), ["source","state","tracking_label"], 50)
forms_total_y = form_status.count(); forms_dropped_y = not_called.count()

print(f"Yesterday: {tot_all} events, {tot_inb} inbound voice, miss {miss_pct}%, {tot_forms} forms ({forms_dropped_y} not called back)")

# ════════ DAILY FLAGS (tightened: fire only on real, sustained signal) ════════
# A source/state drop fires only if EITHER:
#   (a) high-volume: same-weekday baseline >= HIGH_VOL_BASELINE (one day is meaningful), OR
#   (b) sustained: below 60% of its 3-week same-weekday avg for the last SUSTAINED_DAYS days running.
# This stops single-day dips on small sources from crying wolf.
HIGH_VOL_BASELINE = 10
SUSTAINED_DAYS    = 3
SUSTAINED_RATIO   = 0.60

# Per-day counts for the last SUSTAINED_DAYS days, by source and by state, plus a
# same-weekday 3-week average to compare each day against.
recent_days = [str(report_day - timedelta(days=i)) for i in range(SUSTAINED_DAYS)]
def _sustained_down(dim_col):
    # avg per same-weekday over prior 3 weeks, per dim value
    base3 = (ctm.filter(F.col("call_date").isin(dow_dates[:3]) & F.col(dim_col).isNotNull())
                .groupBy(dim_col).agg((F.count("*")/F.lit(min(3,len(dow_dates)))).alias("wk_avg")))
    recent = (ctm.filter(F.col("call_date").isin(recent_days) & F.col(dim_col).isNotNull())
                 .groupBy(dim_col,"call_date").agg(F.count("*").alias("c")))
    j = recent.join(base3, dim_col, "inner").filter(F.col("wk_avg") >= MIN_BASE_VOL)
    # a dim value is "sustained down" if EVERY one of the last N days is below ratio*wk_avg
    flagged = (j.withColumn("below", (F.col("c") <= F.col("wk_avg")*SUSTAINED_RATIO).cast("int"))
                 .groupBy(dim_col).agg(F.sum("below").alias("days_below"), F.count("*").alias("days_seen"),
                                       F.round(F.first("wk_avg"),1).alias("wk_avg"))
                 .filter((F.col("days_below")>=SUSTAINED_DAYS) & (F.col("days_seen")>=SUSTAINED_DAYS)))
    return {r[dim_col]: r["wk_avg"] for r in flagged.collect()}

sustained_states = _sustained_down("state")
sustained_src    = _sustained_down("source")

flags = []
# (a) high-volume single-day drops — meaningful on their own
for s in states_list:
    if s["base_daily"]>=HIGH_VOL_BASELINE and s["calls"]<=s["base_daily"]*DROP_RATIO:
        flags.append(f"State {s['state']}: {s['calls']} vs {s['base_daily']}/day baseline (high-volume drop)")
for p in ppc_list:
    if p["base_daily"]>=HIGH_VOL_BASELINE and p["calls"]<=p["base_daily"]*DROP_RATIO:
        flags.append(f"PPC '{p['source']}': {p['calls']} vs {p['base_daily']}/day baseline (high-volume drop)")
# (b) sustained multi-day drops — real trends on any source
for st, wk in sorted(sustained_states.items(), key=lambda kv:-kv[1]):
    flags.append(f"State {st}: below baseline {SUSTAINED_DAYS} days running (~{wk}/day avg)")
for sc, wk in sorted(sustained_src.items(), key=lambda kv:-kv[1]):
    flags.append(f"Source '{sc}': below baseline {SUSTAINED_DAYS} days running (~{wk}/day avg)")
# always-actionable daily signals
for r in reps_list:
    if (r["callbacks_needed"] or 0) >= 2 and (r["callback_rate_pct"] or 0) == 0:
        flags.append(f"Rep {r['agent_name']}: 0% callback rate ({r['callbacks_needed']} needed, none done)")
if miss_pct>=15: flags.append(f"Miss rate {miss_pct}% (>15%)")
if median_cb is not None and median_cb>60: flags.append(f"Median callback {int(median_cb)} min (>60)")
if cb_needed and cb_rate is not None and cb_rate < 50:
    flags.append(f"Callback rate {cb_rate}% ({cb_made}/{cb_needed}) — below 50%")
if forms_dropped_y>0: flags.append(f"{forms_dropped_y} of {forms_total_y} forms not yet called back")

# ════════ CLAUDE NARRATIVE (aggregates only) ════════
agg = {
  "report_day": str(report_day), "dow_baseline_weeks": DOW_LOOKBACK_WEEKS,
  "totals": {"all_events":tot_all,"inbound_voice":tot_inb,"baseline_all_dow":base_all,"baseline_inbound_dow":base_inb,
             "miss_pct":miss_pct,"callbacks_needed":cb_needed,"callbacks_made":cb_made,"callback_rate_pct":cb_rate,
             "median_callback_min":median_cb,"forms":tot_forms},
  "callback_bucket_mix": bucket_mix,
  "by_source": by_source_list, "ppc_by_campaign": ppc_list, "by_state": states_list,
  "channel_mix": channel_mix, "forms_by_source": forms_by_src,
  "forms_not_called_back": {"count":forms_dropped_y,"of_total":forms_total_y,"list":not_called_list},
  "reps": reps_list,
}
claude_html = "<p style='color:#888'>(AI narrative unavailable.)</p>"
try:
    api_key = notebookutils.credentials.getSecret(KV_URL, ANTHROPIC_SECRET)
    prompt = (
        "You are an operations analyst for a behavioral-health call center. Below is YESTERDAY's call "
        "data as aggregates plus a same-weekday baseline (same weekday, prior 4 weeks). Write a concise "
        "morning brief (<=200 words). Order: (1) overall volume vs same-weekday baseline + miss & callback "
        "rates + median callback speed; (2) flag states or PPC campaigns notably below their same-weekday "
        "baseline; (3) forms not called back; (4) one-line rep-performance read (answer-rate / callback-rate "
        "outliers). Be specific with numbers. Plain, direct. Return simple HTML (<p>,<ul>,<li>,<b> only)."
        "\n\nDATA:\n" + json.dumps(agg, default=str)
    )
    resp = requests.post("https://api.anthropic.com/v1/messages",
        headers={"x-api-key":api_key,"anthropic-version":"2023-06-01","content-type":"application/json"},
        json={"model":ANTHROPIC_MODEL,"max_tokens":1024,"messages":[{"role":"user","content":prompt}]}, timeout=60)
    if resp.status_code==200:
        claude_html = "".join(b.get("text","") for b in resp.json().get("content",[]) if b.get("type")=="text")
        print("Claude narrative: OK")
    else:
        claude_html = f"<p style='color:#b00'>(AI narrative failed: HTTP {resp.status_code}.)</p>"
        print(f"Anthropic error {resp.status_code}: {resp.text[:300]}")
except Exception as e:
    print(f"Claude narrative skipped: {e}")

# ════════ EMAIL ════════
def tile(label,val,sub=""):
    return (f"<td style='padding:10px 14px;border:1px solid #e0e0e0;border-radius:6px;text-align:center'>"
            f"<div style='font-size:22px;font-weight:bold;color:#1F3864'>{val}</div>"
            f"<div style='font-size:11px;color:#666'>{label}</div><div style='font-size:10px;color:#999'>{sub}</div></td>")
def esc(x): return "" if x is None else str(x)

flags_html = "".join(f"<li>{f}</li>" for f in flags) or "<li>No daily flags.</li>"
def pct(v): return (f"{v}%" if v is not None else "—")
rep_rows = "".join(
    f"<tr><td style='padding:3px 8px;border:1px solid #eee'>{esc(r['agent_name'])}</td>"
    f"<td style='padding:3px 8px;border:1px solid #eee;text-align:right'>{r['answered']}</td>"
    f"<td style='padding:3px 8px;border:1px solid #eee;text-align:right'>{r['missed']}</td>"
    f"<td style='padding:3px 8px;border:1px solid #eee;text-align:right;color:#999'>{r['quick_hangup']}</td>"
    f"<td style='padding:3px 8px;border:1px solid #eee;text-align:right'>{pct(r['answer_rate_pct'])}</td>"
    f"<td style='padding:3px 8px;border:1px solid #eee;text-align:right'>{r['outbound']}</td>"
    f"<td style='padding:3px 8px;border:1px solid #eee;text-align:right'>{pct(r['outbound_conn_pct'])}</td>"
    f"<td style='padding:3px 8px;border:1px solid #eee;text-align:right'>{pct(r['callback_rate_pct'])}</td>"
    f"<td style='padding:3px 8px;border:1px solid #eee;text-align:right'>{r['talk_hrs']}</td>"
    f"<td style='padding:3px 8px;border:1px solid #eee;text-align:right;color:#888'>{r['base_answered']}</td></tr>"
    for r in reps_list) or "<tr><td colspan=10 style='padding:4px 8px;color:#888'>No rep activity.</td></tr>"
ncb_rows = "".join(
    f"<tr><td style='padding:4px 8px;border:1px solid #eee'>{esc(r['source'])}</td>"
    f"<td style='padding:4px 8px;border:1px solid #eee'>{esc(r['state'])}</td>"
    f"<td style='padding:4px 8px;border:1px solid #eee'>{esc(r['tracking_label'])}</td></tr>"
    for r in not_called_list) or "<tr><td colspan=3 style='padding:4px 8px;color:#888'>All forms called back.</td></tr>"
bucket_str = " · ".join(f"{b['callback_bucket']}: {b['n']}" for b in bucket_mix) or "—"

html = f"""<html><body style="font-family:Arial,sans-serif;font-size:13px;color:#222">
  <h2 style="color:#1F3864;margin-bottom:2px">CTM Daily Brief — {report_day}</h2>
  <div style="color:#888;font-size:11px;margin-bottom:14px">vs same-weekday baseline (prior {DOW_LOOKBACK_WEEKS} wks) · 7 AM CT</div>
  <table style="border-collapse:separate;border-spacing:6px;margin-bottom:16px"><tr>
    {tile("Total events", tot_all, f"baseline {base_all}")}
    {tile("Inbound voice", tot_inb, f"baseline {base_inb}")}
    {tile("Miss rate", f"{miss_pct}%", "target &lt;10%")}
    {tile("Callback rate", f"{cb_rate}%" if cb_rate is not None else "—")}
    {tile("Median callback", f"{int(median_cb)}m" if median_cb is not None else "—", bucket_str)}
    {tile("Forms", tot_forms, f"{forms_dropped_y} open")}
  </tr></table>

  <h3 style="color:#2E75B6;margin-bottom:4px">AI Summary</h3>
  <div style="background:#f7f9fc;border-left:3px solid #2E75B6;padding:8px 14px;margin-bottom:16px">{claude_html}</div>

  <h3 style="color:#2E75B6;margin-bottom:4px">Daily flags</h3>
  <ul style="margin-top:0">{flags_html}</ul>

  <h3 style="color:#2E75B6;margin-bottom:4px">Rep performance (yesterday)</h3>
  <table style="border-collapse:collapse;font-size:12px;margin-bottom:16px">
    <tr style="background:#305496;color:#fff">
      <th style="padding:3px 8px;border:1px solid #ccc">Rep</th><th style="padding:3px 8px;border:1px solid #ccc">Ans</th>
      <th style="padding:3px 8px;border:1px solid #ccc">Miss</th><th style="padding:3px 8px;border:1px solid #ccc">QckHng</th>
      <th style="padding:3px 8px;border:1px solid #ccc">Ans%</th><th style="padding:3px 8px;border:1px solid #ccc">Out</th>
      <th style="padding:3px 8px;border:1px solid #ccc">Out-conn%</th><th style="padding:3px 8px;border:1px solid #ccc">CB%</th>
      <th style="padding:3px 8px;border:1px solid #ccc">Talk h</th><th style="padding:3px 8px;border:1px solid #ccc">Base ans/day</th></tr>
    {rep_rows}
  </table>

  <h3 style="color:#2E75B6;margin-bottom:4px">Forms not yet called back ({forms_dropped_y})</h3>
  <table style="border-collapse:collapse;font-size:12px;margin-bottom:16px">
    <tr style="background:#305496;color:#fff"><th style="padding:4px 8px;border:1px solid #ccc">Source</th>
      <th style="padding:4px 8px;border:1px solid #ccc">State</th><th style="padding:4px 8px;border:1px solid #ccc">Tracking label</th></tr>
    {ncb_rows}
  </table>
  <p style="color:#aaa;font-size:10px">Aggregates only; no caller PII sent to AI. Source: ctm_calls_raw.</p>
</body></html>"""

try:
    sg = notebookutils.credentials.getSecret(KV_URL, SENDGRID_SECRET)
    r = requests.post("https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization":f"Bearer {sg}","Content-Type":"application/json"},
        json={"personalizations":[{"to":[{"email":TO_EMAIL}]}],
              "from":{"email":FROM_EMAIL,"name":"CTM Daily Brief"},
              "subject":f"CTM Daily Brief — {report_day} ({tot_inb} inbound, {miss_pct}% missed, {forms_dropped_y} forms open)",
              "content":[{"type":"text/html","value":html}]}, timeout=20)
    print("SendGrid:", "sent" if r.status_code in (200,202) else f"FAILED {r.status_code} {r.text[:200]}")
except Exception as e:
    print(f"SendGrid send failed: {e}")
print("Done.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
