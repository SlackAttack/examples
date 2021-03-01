SET SEARCH_PATH to supply;
BEGIN;
DROP TABLE IF EXISTS stg_base_monthly_rpt_supply_district_breakdown;
CREATE TABLE stg_base_monthly_rpt_supply_district_breakdown AS
with monthly_base as (
SELECT
bd.date
,dd.year_month
,dd.year
,bd.district_id
,bd.churn
,bd.ms_customer_count
,bd.me_customer_count
FROM supply.stg_rpt_supply_district_break_down bd
LEFT JOIN mdm.dim_date dd
ON bd.date = dd.date
WHERE 1=1
AND metric_type = 'monthly'
)

,pre_final as (
    SELECT mb.date
         , mb.year_month
         , mb.district_id
         , mb.churn
         , mb.ms_customer_count
         , mb.me_customer_count
         , py.churn             as py_churn
         , py.ms_customer_count as py_ms_customer_count
         , py.me_customer_count as py_me_customer_count
         , pm.churn             as pm_churn
         , pm.ms_customer_count as pm_ms_customer_count
         , pm.me_customer_count as pm_me_customer_count
    FROM monthly_base mb
             LEFT JOIN monthly_base py
                   ON to_char(date_add('month', -1 ,date_add('year', -1, mb.date)) ,'YYYY-MM') = to_char(py.date,'YYYY-MM')
                           AND mb.district_id = py.district_id
             LEFT JOIN monthly_base pm
                       ON to_char(date_add('month', -2, mb.date),'YYYY-MM') = to_char(pm.date,'YYYY-MM')
                           AND mb.district_id = pm.district_id
)


SELECT
pf.date
,pf.year_month
,pf.district_id
,pf.churn as churned_boats
,pf.ms_customer_count
,pf.me_customer_count
,CASE WHEN (pf.ms_customer_count + pf.me_customer_count) <= 0
THEN NULL
ELSE churn / ((pf.ms_customer_count + pf.me_customer_count) / 2) :: DECIMAL(5,2) end as churn_rate
,pf.py_churn as py_churned_boats
,pf.py_ms_customer_count
,pf.py_me_customer_count
,CASE WHEN (pf.py_ms_customer_count + pf.py_me_customer_count) <= 0
THEN NULL
ELSE pf.py_churn / ((pf.py_ms_customer_count + pf.py_me_customer_count) / 2) :: DECIMAL(5,2) end as py_churn_rate
,pf.pm_churn as pm_churned_boats
,pf.pm_ms_customer_count
,pf.pm_me_customer_count
,CASE WHEN (pf.pm_ms_customer_count + pf.pm_me_customer_count) <= 0
THEN NULL
ELSE pf.pm_churn / ((pf.pm_ms_customer_count + pf.pm_me_customer_count) / 2) :: DECIMAL(5,2) end as pm_churn_rate
FROM pre_final pf
WHERE 1=1
AND pf.date = TRUNC(date_trunc('month' , (convert_timezone('EST', TRUNC(getdate())) - interval '1 day' - interval '1 month')))
ORDER BY 3;
COMMIT;


