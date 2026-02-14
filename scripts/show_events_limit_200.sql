.headers on
.mode column
SELECT
  ts_local,
  export_costs,
  want_pct,
  reason,
  json_extract(data_json,'$.sources.alpha.soc_pct') AS soc,
  json_extract(data_json,'$.sources.alpha.batt_state') AS batt_state,
  json_extract(data_json,'$.sources.alpha.pbat_w') AS pbat_w,
  json_extract(data_json,'$.sources.alpha.pgrid_w') AS pgrid_w,
  json_extract(data_json,'$.sources.alpha.pload_w') AS pload_w
FROM events
WHERE json_extract(data_json,'$.sources.alpha.soc_pct') >= 85
ORDER BY ts_epoch_ms DESC
LIMIT 200;
