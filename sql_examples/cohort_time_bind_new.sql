with base as (
                        SELECT kr.date                              as opportunity_created_date
                                , NVL(elapsed_days_opp_creation, 0)    as elapsed_days_opp_creation
                                , NVL(count_of_ops, 0)                 as ops_created
                                , NVL(count_of_successful_bookings, 0) as bookings_created
                           FROM marketing.keyword_report_v_tbind_test kr
                                -----PAID
                           WHERE 1 = 1
                             AND opportunity_created_date BETWEEN '2019-01-01' AND '2019-09-30'
                             AND (source in ('bing', 'Google')
-----this or statement literally makes not sense but its what makes the numbers match
                               OR keyword in ('boatsetters', 'boatsetter com', 'boatsetter', 'boatbound', 'Boat setter',
                                              '+boatsetter', '+boatbound'))
                                )

,total_ops as (SELECT SUM(ops_created) as total_ops
    FROM base)




SELECT
elapsed_days_opp_creation
,MAX(total_ops) as total_ops
,SUM(bookings_created) as bookings_created
,SUM(bookings_created) over ( ORDER BY
    elapsed_days_opp_creation ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) as cumulative_bookings_created
FROM (
SELECT elapsed_days_opp_creation
,total_ops
, SUM(bookings_created)  as bookings_created
FROM base b
,total_ops
WHERE 1 = 1
AND elapsed_days_opp_creation < 90
GROUP BY 1,2
ORDER BY 1
    ) as foo
GROUP BY elapsed_days_opp_creation , bookings_created
ORDER BY 1




----------elapsed days bound
SELECT
opportunity_created_date
,opportunity_booking_rate
,avg(opportunity_booking_rate) over () as avg_opp_booking_rate
,opportunity_booking_rate_30D
,avg(opportunity_booking_rate_30D)  over () as avg_opp_booking_rate_opportunity_booking_rate_30D
FROM (
         SELECT opportunity_created_date
-- ,bookings_created
              , CASE
                    WHEN ops_created > 0 THEN
                        bookings_created / ops_created :: DECIMAL(8, 2) END         as opportunity_booking_rate
              , CASE
                    WHEN ops_created > 0 THEN
                        seven_d_bookings_created / ops_created :: DECIMAL(8, 2) END as opportunity_booking_rate_30D
         FROM (
                  SELECT dd.first_day_of_week as opportunity_created_date
                       , SUM(ops_created)       as ops_created
                       , SUM(bookings_created)  as bookings_created
                       , SUM(CASE
                                 WHEN elapsed_days_opp_creation <= 29
                                     AND opportunity_created_date <=
                                         TRUNC(convert_timezone('EST', TRUNC(getdate())) - interval '31 day')
                                     THEN bookings_created
                                 ELSE NULL END) as seven_d_bookings_created
                  FROM (
                           SELECT kr.date                              as opportunity_created_date
                                , NVL(elapsed_days_opp_creation, 0)    as elapsed_days_opp_creation
                                , NVL(count_of_ops, 0)                 as ops_created
                                , NVL(count_of_successful_bookings, 0) as bookings_created
                           FROM marketing.keyword_report_v_tbind_test kr
                                -----PAID
                           WHERE 1 = 1
                             AND (source in ('bing', 'Google')
-----this or statement literally makes not sense but its what makes the numbers match
                               OR keyword in ('boatsetters', 'boatsetter com', 'boatsetter', 'boatbound', 'Boat setter',
                                              '+boatsetter', '+boatbound'))) as base
                  LEFT JOIN mdm.dim_date dd
         ON base.opportunity_created_date = dd.date
                  WHERE 1 = 1
                    AND opportunity_created_date >= '2019-06-01'
                  GROUP BY 1
              ) as foo
                 WHERE 1=1
     )
GROUP BY
opportunity_created_date
,opportunity_booking_rate
,opportunity_booking_rate_30D
ORDER BY 1





-----------------------------------NOW LETS SEE THAT SAME CHART BUT T7

SELECT
opportunity_created_date
,CASE WHEN opportunity_created_date > '2019-06-07' THEN opportunity_booking_rate_T7 ELSE NULL END as opportunity_booking_rate_T7
,avg_opp_booking_rate
,CASE WHEN opportunity_created_date > '2019-06-07' THEN opportunity_booking_rate_30D_T7 ELSE NULL END as opportunity_booking_rate_30D_T7

,avg_seven_d_opp_booking_rate
FROM (
         SELECT opportunity_created_date
              , AVG(opportunity_booking_rate)
                OVER (order by opportunity_created_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as opportunity_booking_rate_T7
              , avg(opportunity_booking_rate) over ()                                             as avg_opp_booking_rate
              , AVG(opportunity_booking_rate_30D)
                OVER (order by opportunity_created_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as opportunity_booking_rate_30D_T7
              , avg(opportunity_booking_rate_30D) over ()                                         as avg_seven_d_opp_booking_rate
         FROM (
                  SELECT opportunity_created_date
-- ,bookings_created
                       , CASE
                             WHEN ops_created > 0 THEN
                                 bookings_created / ops_created :: DECIMAL(8, 2) END         as opportunity_booking_rate
                       , CASE
                             WHEN ops_created > 0 THEN
                                 seven_d_bookings_created / ops_created :: DECIMAL(8, 2) END as opportunity_booking_rate_30D
                  FROM (
                           SELECT opportunity_created_date
                                , SUM(ops_created)       as ops_created
                                , SUM(bookings_created)  as bookings_created
                                , SUM(CASE
                                          WHEN elapsed_days_opp_creation <= 29
                                              AND opportunity_created_date <=
                                                  TRUNC(convert_timezone('EST', TRUNC(getdate())) - interval '32 day')
                                              THEN bookings_created
                                          ELSE NULL END) as seven_d_bookings_created
                           FROM (
                                    SELECT kr.date                              as opportunity_created_date
                                         , NVL(elapsed_days_opp_creation, 0)    as elapsed_days_opp_creation
                                         , NVL(count_of_ops, 0)                 as ops_created
                                         , NVL(count_of_successful_bookings, 0) as bookings_created
                                    FROM marketing.keyword_report_v_tbind_test kr
                                         -----PAID
                                    WHERE 1 = 1
                                      AND (source in ('bing', 'Google')
-----this or statement literally makes not sense but its what makes the numbers match in tableau
                                        OR keyword in
                                           ('boatsetters', 'boatsetter com', 'boatsetter', 'boatbound', 'Boat setter',
                                            '+boatsetter', '+boatbound'))) as base
                           WHERE 1 = 1
                             AND opportunity_created_date >= '2019-06-01'
                           GROUP BY 1
                       ) as foo
                  WHERE 1 = 1
              )
         GROUP BY opportunity_created_date, opportunity_booking_rate, opportunity_booking_rate_30D
     )
ORDER BY 1