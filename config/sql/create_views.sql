-- Widok ten jestużywany do porównania instalacji w celu określenia ich wydajności w okresie ostatnich 60 dni
-- w stosunku do sumy produkcji wszystkich instalacji
-- podaje współczynniki coefficient do normowania mocy każdej instalacji:
-- # sn	min_time	max_time	prod_begin	prod_end	delta_kwh	coefficient
-- SS1ES120P4U121	2025-06-06 03:58:28	2025-08-04 20:58:49	32108.10	37419.00	5310.90	0.158126
-- SS1ES122M5G764	2025-06-06 03:56:56	2025-08-04 20:56:50	35054.60	41095.00	6040.40	0.179846
-- SS3ES125P38069	2025-06-06 03:59:13	2025-08-04 20:34:40	44323.40	51887.30	7563.90	0.225206
-- SS3ES150NAT230	2025-06-06 03:50:04	2025-08-04 20:57:09	89211.90	103883.30	14671.40	0.436823

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