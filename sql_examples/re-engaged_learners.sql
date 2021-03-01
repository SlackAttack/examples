

with skill_completed_correction as (
    SELECT v.learner_id
         , v.skill_id
         , MIN(TRUNC(v.created_at)) as skill_completed_date
    FROM zeus.videos v
    WHERE 1 = 1
    GROUP BY 1, 2
)

,skills_completed_by_date as (
         SELECT skill_count.learner_id
              , skill_count.skill_completed_date
              , SUM(skill_count.skill_completed_count) OVER
             (PARTITION BY learner_id
                  ORDER BY skill_completed_date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_skill_count
         ,MIN(skill_count.skill_completed_date) OVER (PARTITION BY learner_id
             ) as first_skill_completed_date
         FROM (
                  SELECT scc.learner_id
                       , scc.skill_completed_date
                       , count(scc.skill_id) as skill_completed_count
                  FROM skill_completed_correction scc
                  WHERE 1 = 1
                  GROUP BY 1, 2
              ) as skill_count
         WHERE 1 = 1
         GROUP BY skill_count.learner_id
                , skill_count.skill_completed_date
                , skill_count.skill_completed_count
     )

---for first skill
,activity as (
    SELECT
    scbd2.learner_id
    ,scbd2.skill_completed_date as activity_date
    FROM skills_completed_by_date scbd2
    LEFT JOIN zeus.learners l2
    ON scbd2.learner_id = l2.id
    WHERE 1=1
    AND l2.state = 'CO'
--     AND [l2.state =state_code]
)

,active_learners_by_day_base as (
    SELECT DISTINCT base.learner_id
                  , dd.date as active_dates
    FROM (
             SELECT learner_id
                  , activity_date
             FROM activity
             WHERE 1 = 1
-- AND learner_id IN (5762,5763,5766)
             ORDER BY 1, 2
         ) as base
             INNER JOIN mdm.dim_date dd
                        ON base.activity_date <= dd.date
    WHERE 1 = 1
      AND dd.date < TRUNC(DATE_ADD('days', 14, base.activity_date))
    ORDER BY 1, 2
)


,reengaged_learners as (
    SELECT
           foobar.date
         , SUM(gap_flag_lag) as reengaged_learners
    FROM (
    SELECT foo.learner_id
         , foo.date
         , foo.dcbase_date
         , foo.gap_flag
         , lag(foo.gap_flag, 1)
           OVER (partition by foo.learner_id ORDER BY foo.learner_id, foo.date) as gap_flag_lag ---update
    FROM (
             SELECT mmdp.learner_id
                  , mmdp.date
                  , dcbase.active_dates                                        dcbase_date
                  , CASE WHEN dcbase.active_dates IS NULL THEN 1 ELSE 0 END as gap_flag
             FROM (
                      SELECT date_comp.learner_id
                           , dd.date
                      FROM mdm.dim_date dd
                               LEFT JOIN (
                          SELECT albdb.learner_id
                               , MIN(albdb.active_dates)                  as min_date
                               , MAX(albdb.active_dates)                  as max_date
                               , COUNT(albdb.active_dates)                as date_count
                               , date_diff('day', min_date, max_date) + 1 as dif_min_max
                          FROM active_learners_by_day_base albdb
                          GROUP BY 1)
                          as date_comp
                                         ON dd.date >= date_comp.min_date
                                             AND dd.date <= date_comp.max_date
                      WHERE 1 = 1
                        AND date_comp.dif_min_max != date_comp.date_count
                      ORDER BY 1, 2
                  ) as mmdp
                      LEFT JOIN
                  (
                      SELECT DISTINCT abbase2.learner_id
                                    , abbase2.active_dates
                      FROM active_learners_by_day_base abbase2
                  ) as dcbase
                  ON mmdp.learner_id = dcbase.learner_id
                      AND mmdp.date = dcbase.active_dates) as foo
    ORDER BY 1, 2
    ) as foobar
WHERE 1 = 1
AND foobar.gap_flag = 0
AND gap_flag_lag = 1
GROUP BY 1
)

----------group me by weeks or months
SELECT
dd.date
,NVL(rl.reengaged_learners,0)
FROM mdm.dim_date dd
LEFT JOIN reengaged_learners rl
ON dd.date = rl.date
WHERE 1=1
AND dd.date BETWEEN '2019-11-04'
AND TRUNC(getdate() - interval '1 day')
ORDER BY 1
