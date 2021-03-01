with skill_completed_correction as (
    SELECT v.learner_id
         , v.skill_id
         , MIN(TRUNC(v.created_at)) as skill_completed_date
    FROM zeus.videos v
    WHERE 1 = 1
    GROUP BY 1, 2
)

,skill_number_completed_date as (
    SELECT
    foo.learner_id
    ,CASE WHEN foo.skill_completed_number >= 140 THEN 140 ELSE
        foo.skill_completed_number END as skill_completed_number
    ,MIN(foo.skill_completed_date) as skill_completed_date
    FROM (
         SELECT skill_count.learner_id
              , skill_count.skill_completed_date
              , COUNT(skill_count.skill_completed_count) OVER
             (PARTITION BY learner_id
            ORDER BY skill_completed_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as skill_completed_number
         FROM (
                  SELECT scc.learner_id
                       , scc.skill_completed_date
                       , scc.skill_id as skill_completed_count
                  FROM skill_completed_correction scc
                  WHERE 1 = 1
              ) as skill_count
         WHERE 1 = 1
         GROUP BY skill_count.learner_id
                , skill_count.skill_completed_date
                , skill_count.skill_completed_count
          ORDER BY 1,2) as
          foo
    GROUP BY 1,2
    )

,elapsed_days as (
    SELECT l.id as learner_id
         ,skill_completed_number
         , CASE
               WHEN lfc.skill_completed_date < TRUNC(l.enrolled_at) THEN
                   TRUNC(created_at)
               ELSE TRUNC(l.enrolled_at) END as enrolled_date
         , date_diff('days', enrolled_date, skill_completed_date)
                                             as elapsed_days_skill_completed
             FROM zeus.learners l
             LEFT JOIN skill_number_completed_date lfc
                       ON l.id = lfc.learner_id
    WHERE 1 = 1
    AND enrolled_date < TRUNC(getdate() - '91 days'::INTERVAL)
    AND l.state = 'CO'
--  AND l.state =state_code
)

,total_learners as (
    SELECT COUNT(DISTINCT et.learner_id) as total_enrolled_learners
    FROM elapsed_days et
    WHERE 1 = 1
)


SELECT
pf.skills_completed_number
,CASE WHEN pf.total_enrolled_learners > 0 THEN
    pf.learner_skill_completed / pf.total_enrolled_learners :: DECIMAL(8,2)
    END as enrolled_learner_skill_completed_rate
,CASE WHEN pf.learner_skill_completed_lag > 0 THEN
    pf.learner_skill_completed / pf.learner_skill_completed_lag :: DECIMAL(8,2)
    END as prior_skill_skill_completed_rate
FROM (
         SELECT base.skills_completed_number
              , total_learners.total_enrolled_learners
              , CASE
                    WHEN base.skills_completed_number = 0
                        THEN total_learners.total_enrolled_learners
                    ELSE base.learner_skill_completed
             END  as learner_skill_completed
              , LAG(CASE
                        WHEN base.skills_completed_number = 0
                            THEN total_learners.total_enrolled_learners
                        ELSE base.learner_skill_completed END, 1)
                OVER (order by skills_completed_number) as learner_skill_completed_lag
         FROM (
             SELECT seq                                 as skills_completed_number
                  , COUNT(elapsed_days_skill_completed) as learner_skill_completed
             FROM mdm.seed_series ss
                      LEFT JOIN elapsed_days ed2
                                ON ss.seq = ed2.skill_completed_number
                                    AND ed2.elapsed_days_skill_completed < 90
             WHERE 1 = 1
               AND ss.seq <= 140
               ----Restric on elapsed_days_skill_complete
             GROUP BY 1
             ORDER BY 1
         ) as base
            , total_learners
         ORDER BY 1
     ) as pf
-- ORDER BY 2 ASC LIMIT 10



----Add average days to complete skills at different percentage points?