SET SEARCH_PATH to supply;
BEGIN;
DROP TABLE IF EXISTS stg_base_daily_rpt_supply_district_breakdown;
CREATE TABLE stg_base_daily_rpt_supply_district_breakdown AS
with daily as (
SELECT
bd.date
,dd.year_month
,dd.year
,district_id
,listings_started
,approved_boats
-- ,instant_bookable
-- ,instant_bookable_denominator
,live_boats
,active_boats
FROM supply.stg_rpt_supply_district_break_down bd
LEFT JOIN mdm.dim_date dd
ON bd.date = dd.date
WHERE 1=1
AND metric_type = 'daily'
)

,rolling as (
    SELECT date
         , year_month
         , year
         , district_id
         , SUM(listings_started)
           OVER (partition by district_id order by date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)                     as listings_started_T7
         , SUM(approved_boats)
           OVER (partition by district_id order by date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)                     as approved_boats_T7
--          , SUM(instant_bookable)
--            OVER (partition by district_id order by date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)                     as instant_bookable_T7
--          , SUM(instant_bookable_denominator)
--            OVER (partition by district_id order by date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)                     as instant_bookable_denominator_T7
         , MAX(live_boats)
           OVER (partition by district_id order by date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)                     as live_boats_T7
         , MAX(active_boats)
           OVER (partition by district_id order by date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)                     as active_boats_T7
         , SUM(listings_started)
           OVER (partition by district_id order by date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)                    as listings_started_T28
         , SUM(approved_boats)
           OVER (partition by district_id order by date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)                    as approved_boats_T28
--          , SUM(instant_bookable)
--            OVER (partition by district_id order by date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)                    as instant_bookable_T28
--          , SUM(instant_bookable_denominator)
--            OVER (partition by district_id order by date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)                    as instant_bookable_denominator_T28
         , MAX(live_boats)
           OVER (partition by district_id order by date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)                    as live_boats_T28
         , MAX(active_boats)
           OVER (partition by district_id order by date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)                    as active_boats_T28
         , SUM(listings_started)
           OVER (partition by district_id, year_month order by date ROWS UNBOUNDED PRECEDING)                         as listings_started_mtd
         , SUM(approved_boats)
           OVER (partition by district_id, year_month order by date ROWS UNBOUNDED PRECEDING)                         as approved_boats_mtd
--          , SUM(instant_bookable)
--            OVER (partition by district_id, year_month order by date ROWS UNBOUNDED PRECEDING)                         as instant_bookable_mtd
--          , SUM(instant_bookable_denominator)
--            OVER (partition by district_id, year_month order by date ROWS UNBOUNDED PRECEDING)                         as instant_bookable_denominator_mtd
         , MAX(live_boats)
           OVER (partition by district_id, year_month order by date ROWS UNBOUNDED PRECEDING)                         as live_boats_mtd
         , MAX(active_boats)
           OVER (partition by district_id, year_month order by date ROWS UNBOUNDED PRECEDING)                         as active_boats_mtd
         , SUM(listings_started)
           OVER (partition by district_id, year order by date ROWS UNBOUNDED PRECEDING)                               as listings_started_ytd
         , SUM(approved_boats)
           OVER (partition by district_id, year order by date ROWS UNBOUNDED PRECEDING)                               as approved_boats_ytd
--          , SUM(instant_bookable)
--            OVER (partition by district_id, year order by date ROWS UNBOUNDED PRECEDING)                               as instant_bookable_ytd
--          , SUM(instant_bookable_denominator)
--            OVER (partition by district_id, year order by date ROWS UNBOUNDED PRECEDING)                               as instant_bookable_denominator_ytd
         , MAX(live_boats)
           OVER (partition by district_id, year order by date ROWS UNBOUNDED PRECEDING)                               as live_boats_ytd
         , MAX(active_boats)
           OVER (partition by district_id, year order by date ROWS UNBOUNDED PRECEDING)                               as active_boats_ytd
    FROM daily
    WHERE 1 = 1
)

    SELECT
date
,district_id
,listings_started_t7
,approved_boats_t7
,live_boats_t7
,active_boats_t7
,listings_started_t28
,approved_boats_t28
,live_boats_t28
,active_boats_t28
,listings_started_mtd
,approved_boats_mtd
,live_boats_mtd
,active_boats_mtd
,listings_started_ytd
,approved_boats_ytd
,live_boats_ytd
,active_boats_ytd
,listings_started_t7_pw
,approved_boats_t7_pw
,live_boats_t7_pw
,active_boats_t7_pw
,listings_started_t7_py
,approved_boats_t7_py
,live_boats_t7_py
,active_boats_t7_py
,listings_started_t28_py
,approved_boats_t28_py
,live_boats_t28_py
,active_boats_t28_py
,listings_started_ytd_py
,approved_boats_ytd_py
,live_boats_ytd_py
,active_boats_ytd_py
,listings_started_mtd_py
,approved_boats_mtd_py
,live_boats_mtd_py
,active_boats_mtd_py
--      ,CASE WHEN instant_bookable_denominator_t7 > 0 THEN
--      instant_bookable_t7 / instant_bookable_denominator_t7 :: DECIMAL(8,2) END as instant_bookable_t7
--      ,CASE WHEN instant_bookable_denominator_t28 > 0 THEN
--      instant_bookable_t28 / instant_bookable_denominator_t28 :: DECIMAL(8,2) END as instant_bookable_t28
--      ,CASE WHEN instant_bookable_denominator_mtd > 0 THEN
--      instant_bookable_mtd / instant_bookable_denominator_mtd :: DECIMAL(8,2) END as instant_bookable_mtd
--      ,CASE WHEN instant_bookable_denominator_ytd > 0 THEN
--      instant_bookable_ytd / instant_bookable_denominator_ytd :: DECIMAL(8,2) END as instant_bookable_ytd
--      ,CASE WHEN instant_bookable_denominator_T7_pw > 0 THEN
--      instant_bookable_T7_pw / instant_bookable_denominator_T7_pw :: DECIMAL(8,2) END as instant_bookable_t7_pw
--      ,CASE WHEN instant_bookable_denominator_T7_py > 0 THEN
--      instant_bookable_T7_py/ instant_bookable_denominator_T7_py :: DECIMAL(8,2) END as instant_bookable_T7_py
--      ,CASE WHEN instant_bookable_denominator_T28_py> 0 THEN
--      instant_bookable_T28_py/ instant_bookable_denominator_T28_py :: DECIMAL(8,2) END as instant_bookable_T28_py
--      ,CASE WHEN instant_bookable_denominator_ytd_py > 0 THEN
--      instant_bookable_ytd_py / instant_bookable_denominator_ytd_py :: DECIMAL(8,2) END as instant_bookable_ytd_py
--      ,CASE WHEN instant_bookable_denominator_mtd_py > 0 THEN
--      instant_bookable_ytd_py / instant_bookable_denominator_mtd_py :: DECIMAL(8,2) END as instant_bookable_mtd_py
FROM (
    SELECT r.date
         , r.district_id
         , r.listings_started_t7
         , r.approved_boats_t7
--          , r.instant_bookable_t7
--          , r.instant_bookable_denominator_t7
         , r.live_boats_t7
         , r.active_boats_t7
         , r.listings_started_t28
         , r.approved_boats_t28
--          , r.instant_bookable_t28
--          , r.instant_bookable_denominator_t28
         , r.live_boats_t28
         , r.active_boats_t28
         , r.listings_started_mtd
         , r.approved_boats_mtd
--          , r.instant_bookable_mtd
--          , r.instant_bookable_denominator_mtd
         , r.live_boats_mtd
         , r.active_boats_mtd
         , r.listings_started_ytd
         , r.approved_boats_ytd
--          , r.instant_bookable_ytd
--          , r.instant_bookable_denominator_ytd
         , r.live_boats_ytd
         , r.active_boats_ytd
         , lag(r.listings_started_T7, 7) OVER (partition by r.district_id ORDER BY r.date) as listings_started_T7_pw
         , lag(r.approved_boats_T7, 7) OVER (partition by r.district_id ORDER BY r.date) as   approved_boats_T7_pw
--          , lag(r.instant_bookable_T7, 7) OVER (partition by r.district_id ORDER BY r.date) as instant_bookable_T7_pw
--          , lag(r.instant_bookable_denominator_T7, 7)
--            OVER (partition by r.district_id ORDER BY r.date) as                               instant_bookable_denominator_T7_pw
         , lag(r.live_boats_T7, 7) OVER (partition by r.district_id ORDER BY r.date) as       live_boats_T7_pw
         , lag(r.active_boats_T7, 7) OVER (partition by r.district_id ORDER BY r.date) as     active_boats_T7_pw
         ,prior_year.listings_started_T7 as listings_started_T7_py
         ,prior_year.approved_boats_T7 as approved_boats_T7_py
--           ,prior_year.instant_bookable_T7 as instant_bookable_T7_py
--         ,prior_year.instant_bookable_denominator_T7 as instant_bookable_denominator_T7_py
        ,prior_year.live_boats_T7 as live_boats_T7_py
        ,prior_year.active_boats_T7 as active_boats_T7_py
         ,prior_year.listings_started_T28 as listings_started_T28_py
        ,prior_year.approved_boats_T28 as approved_boats_T28_py
--         ,prior_year.instant_bookable_T28 as instant_bookable_T28_py
--     ,prior_year.instant_bookable_denominator_T28 as instant_bookable_denominator_T28_py
        ,prior_year.live_boats_T28 as live_boats_T28_py
        ,prior_year.active_boats_T28 as active_boats_T28_py
         , prior_year.listings_started_ytd as                                                 listings_started_ytd_py
         , prior_year.approved_boats_ytd as                                                   approved_boats_ytd_py
--          , prior_year.instant_bookable_ytd as                                                 instant_bookable_ytd_py
--          , prior_year.instant_bookable_denominator_ytd as                                     instant_bookable_denominator_ytd_py
         , prior_year.live_boats_ytd as                                                       live_boats_ytd_py
         , prior_year.active_boats_ytd as                                                     active_boats_ytd_py
         , prior_year.listings_started_mtd as                                                 listings_started_mtd_py
         , prior_year.approved_boats_mtd as                                                   approved_boats_mtd_py
--          , prior_year.instant_bookable_mtd as                                                 instant_bookable_mtd_py
--          , prior_year.instant_bookable_denominator_mtd as                                     instant_bookable_denominator_mtd_py
         , prior_year.live_boats_mtd as                                                       live_boats_mtd_py
         , prior_year.active_boats_mtd as                                                     active_boats_mtd_py
    FROM rolling r
             LEFT JOIN rolling prior_year
                       ON date_add('year', -1, r.date) = prior_year.date
                           AND r.district_id = prior_year.district_id
    ORDER BY 2, 1 DESC)
    WHERE 1=1
    AND date = TRUNC(getdate()) -1;;
--     AND date = '2019-03-01' :: DATE;
COMMIT;

