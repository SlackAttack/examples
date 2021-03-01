with registered_learners as (
    SELECT dd.month_start_date
         , COUNT(l.id) as registered_learners
    FROM zeus.learners l
             LEFT JOIN mdm.dim_date dd
                       ON dd.date = TRUNC(l.created_at)
    WHERE 1 = 1
    GROUP BY 1
)
   -------update me in the future with the corrected enrolled date from dim_learner-------
,enrolled_learners as (
        SELECT dd.month_start_date
         ,COUNT(l.id) as enrolled_learners
         ,COUNT(CASE WHEN state = 'CO' THEN l.id ELSE NULL END) as co_enrolled_learners
    FROM zeus.learners l
    LEFT JOIN mdm.dim_date dd
    ON dd.date = TRUNC(l.enrolled_at)
    WHERE 1 = 1
    GROUP BY 1
)



SELECT
months.month_start_date
,NVL(registered_learners,0) as registered_learners
,NVL(enrolled_learners,0) as enrolled_learners
,NVL(co_enrolled_learners,0) as co_enrolled_learners
FROM (
         SELECT DISTINCT month_start_date
         FROM mdm.dim_date dd
         WHERE 1 = 1
           AND month_start_date BETWEEN '2019-06-01'
             AND TRUNC(date_trunc('month', getdate() - '1 day' :: interval))
     ) as months
LEFT JOIN registered_learners rl
ON months.month_start_date = rl.month_start_date
LEFT JOIN enrolled_learners el
ON months.month_start_date = el.month_start_date
WHERE 1=1
ORDER BY 1

