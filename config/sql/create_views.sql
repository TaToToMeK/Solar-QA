CREATE OR REPLACE VIEW V_STATS_N_DAYS AS
WITH ranked AS (
  SELECT
    sn,
    system_time,
    cumulative_production_active_kwh_,
    ROW_NUMBER() OVER (PARTITION BY sn ORDER BY system_time ASC) AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY sn ORDER BY system_time DESC) AS rn_desc
  FROM SOLARMAN_DATA
  WHERE system_time >= CURDATE() - INTERVAL 60 DAY
    AND system_time < CURDATE()
    AND cumulative_production_active_kwh_ IS NOT NULL
    AND cumulative_production_active_kwh_ > 1
),

mint AS (
  SELECT sn, system_time AS min_time, cumulative_production_active_kwh_ AS prod_begin
  FROM ranked
  WHERE rn_asc = 1
),

maxt AS (
  SELECT sn, system_time AS max_time, cumulative_production_active_kwh_ AS prod_end
  FROM ranked
  WHERE rn_desc = 1
)

SELECT
  mint.sn,
  mint.min_time,
  maxt.max_time,
  mint.prod_begin,
  maxt.prod_end,
  maxt.prod_end - mint.prod_begin AS delta_kwh,
  (maxt.prod_end - mint.prod_begin) / sum(maxt.prod_end - mint.prod_begin) OVER ()  AS coefficient
FROM mint
JOIN maxt USING (sn);

select * from V_STATS_N_DAYS ;