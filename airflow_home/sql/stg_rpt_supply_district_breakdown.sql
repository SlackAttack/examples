SET SEARCH_PATH to supply;
BEGIN;
DROP TABLE IF EXISTS stg_rpt_supply_district_break_down;
CREATE TABLE stg_rpt_supply_district_break_down AS
with top_twenty_five_districts_by_rev_rolling_twelve_months as (
SELECT
NVL(d.id , 2099) as district_id
       ,NVL(d.name , 'NA') as district
       ,SUM((b.price_with_service_fee_cents - b.sales_tax_cents - b.owner_payout_cents - b.towing_cost_cents)/100) as revenue
FROM bs4.boats bt
LEFT JOIN bs4.addresses a
	ON a.addressable_id = bt.id
	AND a.addressable_type = 'Boat'
    AND a.deleted_at IS NULL
LEFT JOIN bs4.districts d
	ON d.id = a.district_id
LEFT JOIN bs4.packages p
ON bt.id = p.boat_id
LEFT JOIN bs4.bookings b
ON p.id = b.package_id
WHERE 1=1
AND b.trip_start BETWEEN TRUNC(date_trunc('month' , TRUNC(getdate()-'1 day'::INTERVAL)) - '12 months' :: INTERVAL)
           AND  TRUNC(date_trunc('month' , TRUNC(getdate()-'1 day'::INTERVAL)) - '1 day' :: INTERVAL)
	AND b.state IN ('aboard','ashore','concluded','disputed','approved')
GROUP BY 1,2
ORDER BY 3 DESC
)

,district_date_cartesian_join as (
    SELECT
    dd.date
    ,dis.district_id
    FROM mdm.dim_date dd
    ,top_twenty_five_districts_by_rev_rolling_twelve_months dis
    WHERE 1=1
    AND dd.date BETWEEN TRUNC(date_trunc('year' , getdate()-'5 years'::INTERVAL))
    AND  TRUNC(getdate()-'1 day'::INTERVAL)
    ORDER BY 2,1
)

,district_month_cartesian_join as (
    SELECT
    DISTINCT
    dd.month_start_date
    ,dis.district_id
    FROM mdm.dim_date dd
    ,top_twenty_five_districts_by_rev_rolling_twelve_months dis
    WHERE 1=1
    AND dd.date BETWEEN TRUNC(date_trunc('year' , getdate()-'5 years'::INTERVAL))
    AND  TRUNC(date_trunc('month' , TRUNC(getdate())) - '1 month' :: INTERVAL)
    ORDER BY 2,1
)


,boat_base as (
    SELECT DISTINCT b.id
         ,NVL(dis.district_id , 2099) as district_id
                  -----*** commenting out instant bookable for now - need to create historic table **** ---- doesnt properly show when its status changed ----------
--          , CASE WHEN f.searchable=True AND
--                      f.captained_instant_book_enabled=True OR
--                      bareboat_instant_book_enabled=True THEN b.id ELSE NULL END as instant_bookable
--          , CASE WHEN f.searchable=True THEN b.id ELSE NULL END as instant_bookable_denominator
         , b.created_at :: DATE
         , b.approval_at :: DATE
    FROM bs4.boats b
             LEFT JOIN bs4.addresses a
                        ON a.addressable_id = b.id
                        AND a.addressable_type = 'Boat'
                        AND a.deleted_at IS NULL
             LEFT JOIN top_twenty_five_districts_by_rev_rolling_twelve_months dis
                        ON a.district_id = dis.district_id
             LEFT JOIN bs4.fleets f
	            ON f.boat_id = b.id
    WHERE 1 = 1
)



,rpt_base as (
    SELECT ddcj.date
         , ddcj.district_id
         , COUNT(DISTINCT list.id) as listings_started
         , COUNT(DISTINCT appr.id) as approved_boats
--          , COUNT(DISTINCT list.instant_bookable) as instant_bookable
--          , COUNT(DISTINCT list.instant_bookable_denominator) as instant_bookable_denominator
    FROM district_date_cartesian_join ddcj
             LEFT JOIN boat_base list
                       ON ddcj.date = list.created_at
                           AND ddcj.district_id = list.district_id
             LEFT JOIN boat_base appr
                       ON ddcj.date = appr.approval_at
                           AND ddcj.district_id = appr.district_id
    WHERE 1=1
    GROUP BY 1,2
    ORDER BY 1,2
)


,active_boats_by_day_utilized_churn_base as (
    SELECT DISTINCT foo.boat_id
                  ,bb.district_id
                  , dd.date
                  ,foo.platform
    FROM (
             SELECT distinct bt.id                                       as boat_id
                           , to_char(b.trip_start, 'YYYY-MM-DD') :: DATE as trip_date
                           , b.id                                        as booking_id
                           ,'bs4'                                        as platform
             FROM bs4.bookings b
                      JOIN bs4.packages p
                           ON p.id = b.package_id

                      JOIN bs4.boats bt
                           ON bt.id = p.boat_id

                      JOIN bs4.fleets f
                           ON f.boat_id = bt.id

             WHERE 1=1
--              AND to_char(b.trip_start, 'YYYY-mm-dd') >=
--                    TRUNC(date_trunc('year', getdate() - '3 years'::INTERVAL))
               AND b.state IN ('disputed', 'concluded')
               --------boat state comes from the bs4_aux_active_boats_table -------------

             UNION

             SELECT distinct bt.id                                       as boat_id
                           , to_char(b.trip_start, 'YYYY-MM-DD') :: DATE as trip_date
                           , b.legacy_booking_id                         as booking_id
                           ,'bs3'                                        as platform
             FROM bs3.legacy_bookings b

                      JOIN bs4.boats bt
                           ON bt.bs_id = b.boat_id

                      JOIN bs4.fleets f
                           ON f.boat_id = bt.id

             WHERE 1=1
--                     AND to_char(b.trip_start, 'YYYY-mm-dd') >=
--                    TRUNC(date_trunc('year', getdate() - '5 years'::INTERVAL))
               ----Going to have to stick with the static field for now -----------
               AND b.state IN ('disputed', 'concluded')
         ) as foo
             INNER JOIN mdm.dim_date dd
                        ON foo.trip_date <= dd.date
     INNER JOIN boat_base bb
     ON bb.id = foo.boat_id
      WHERE 1 = 1
      AND dd.date < TRUNC(DATE_ADD('year', 1, foo.trip_date))
    ORDER BY 1, 2
)

,returned_boats as (
    SELECT
           foobar.date
         , foobar.district_id
         , SUM(gap_flag_lag) as returned_boats
    FROM (
             SELECT foo.boat_id
                  , foo.date
                  , foo.district_id
                  , foo.dcbase_date
                  , foo.gap_flag
                  , lag(foo.gap_flag, 1)
                    OVER (partition by foo.boat_id ORDER BY foo.boat_id, foo.date) as gap_flag_lag ---update
             FROM (
                      SELECT mmdp.boat_id
                           , mmdp.date
                           , dcbase.district_id
                           , dcbase.date                                        dcbase_date
                           , CASE WHEN dcbase.date IS NULL THEN 1 ELSE 0 END as gap_flag
                      FROM (
                               SELECT date_comp.boat_id
                                    , dd.date
                               FROM mdm.dim_date dd
                                        LEFT JOIN
                                    (
                                        SELECT boat_id
                                             , MIN(date)                                as min_date
                                             , MAX(date)                                as max_date
                                             , COUNT(date)                              as date_count
                                             , date_diff('day', min_date, max_date) + 1 as dif_min_max
                                        FROM (
                                                 SELECT DISTINCT boat_id
                                                               , district_id
                                                               , date
                                                 FROM active_boats_by_day_utilized_churn_base abbase) distbase
                                        GROUP BY 1
                                    ) as date_comp
                                    ON dd.date >= date_comp.min_date
                                        AND dd.date <= date_comp.max_date
                               WHERE 1 = 1
                                 AND date_comp.dif_min_max != date_comp.date_count
                               ORDER BY 1, 2) as mmdp
                               LEFT JOIN
                           (
                               SELECT DISTINCT abbase2.boat_id
                                             , abbase2.district_id
                                             , abbase2.date
                               FROM active_boats_by_day_utilized_churn_base abbase2
                           ) as dcbase
                           ON mmdp.boat_id = dcbase.boat_id
                               AND mmdp.date = dcbase.date
                  ) as foo
             ORDER BY 1, 2
         ) as foobar
    WHERE 1 = 1
      AND foobar.gap_flag = 0
      AND gap_flag_lag = 1
    GROUP BY 1,2
)



,active_boats_by_day as (
    SELECT dabbase.date
         , dabbase.district_id
         , COUNT(dabbase.boat_id) as active_boats
    FROM (
    SELECT
    DISTINCT boat_id
    ,district_id
    ,date
    FROM active_boats_by_day_utilized_churn_base abbase)
        as dabbase
    GROUP BY 1,2
)

---churn rate = churn / ((Customers1 + Customers2)/2)
,churn_base as (
SELECT
DISTINCT
churn.boat_id
,churn.district_id
,ddms.month_start_date
,ddme.month_end_date
FROM active_boats_by_day_utilized_churn_base churn
LEFT JOIN mdm.dim_date ddms
ON churn.date = ddms.month_start_date
LEFT JOIN mdm.dim_date ddme
ON churn.date = ddme.month_end_date
WHERE 1=1
AND churn.platform = 'bs4'
ORDER BY 1,3)

SELECT
*
FROM (
         SELECT
                'monthly' as metric_type
              , dmcj.month_start_date as date
              , dmcj.district_id
              , COUNT(CASE WHEN me.me_customer IS NULL THEN ms.ms_customer ELSE NULL END) as churn
              , COUNT(ms.ms_customer)                                                     as ms_customer_count
              , COUNT(me.me_customer)                                                     as me_customer_count
               ,NULL                                                                      as listings_started
         , NULL                                                                           as approved_boats
--          , NULL                                                                           as instant_bookable
--          , NULL                                                                           as instant_bookable_denominator
         , NULL                                                                            as live_boats
         , NULL                                                                             as active_boats
         , NULL                                                                            as returned_boats
         FROM district_month_cartesian_join dmcj
                  LEFT JOIN
              (
                  SELECT cb.district_id
                       , cb.month_start_date
                       , cb.boat_id as ms_customer
                  FROM churn_base cb
                  WHERE 1 = 1
                    AND month_start_date IS NOT NULL
              ) as ms
              ON ms.month_start_date = dmcj.month_start_date
                  AND ms.district_id = dmcj.district_id
                  FULL OUTER JOIN
              (
                  SELECT cb2.district_id
                       , cb2.month_end_date
                       , cb2.boat_id as me_customer
                  FROM churn_base cb2
                  WHERE 1 = 1
                    AND month_end_date IS NOT NULL
              ) as me
              ON to_char(ms.month_start_date, 'YYYY-MM') = to_char(me.month_end_date, 'YYYY-MM')
                  AND ms.ms_customer = me.me_customer
         WHERE 1 = 1
           AND dmcj.district_id IS NOT NULL
         GROUP BY 1, 2 ,3

UNION ALL

    SELECT 'daily'                              as metric_type
         , rb.date
         , rb.district_id
         , NULL                                                                           as churn
         , NULL                                                                           as ms_customer_count
         , NULL                                                                           as me_customer_count
         , SUM(rb.listings_started)             as listings_started
         , SUM(rb.approved_boats)               as approved_boats
--          , SUM(rb.instant_bookable)             as instant_bookable
--          , SUM(rb.instant_bookable_denominator) as instant_bookable_denominator
         , SUM(lb2.live_boats)                  as live_boats
         , SUM(abbd.active_boats)               as active_boats
         , SUM(rbt.returned_boats)              as returned_boats
    FROM rpt_base rb
             LEFT JOIN (
        SELECT lb.date
             , bb2.district_id
             , COUNT(lb.id) as live_boats
        FROM bs4_aux.live_boats lb
                 INNER JOIN boat_base bb2
                            ON lb.id = bb2.id
                 GROUP BY 1,2
    ) as lb2
                       ON rb.date = lb2.date
                           AND rb.district_id = lb2.district_id
             LEFT JOIN active_boats_by_day abbd
                       ON rb.date = abbd.date
                           AND rb.district_id = abbd.district_id
             LEFT JOIN returned_boats rbt
                       ON rb.date = rbt.date
                       AND rb.district_id = rbt.district_id
    WHERE 1 = 1
    GROUP BY 1,2,3
)
ORDER BY 1,3,2;
COMMIT ;
