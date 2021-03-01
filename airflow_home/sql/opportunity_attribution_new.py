pageview_sql = (
"""
WITH tb as
(SELECT

	p.received_at time_stamp
	,CASE
		WHEN p.context_utm_medium IS NULL THEN p.context_campaign_medium
		ELSE p.context_utm_medium
	END medium
	,CASE
		WHEN p.context_utm_source IS NULL THEN p.context_campaign_source
		ELSE p.context_utm_source
	END source
	,CASE
		WHEN p.context_utm_campaign IS NULL THEN p.context_campaign_name
		ELSE p.context_utm_campaign
	END campaign
	,CASE
		WHEN p.context_utm_term IS NULL THEN p.context_campaign_term
		ELSE p.context_utm_term
	END term
	,CASE
		WHEN p.context_utm_adgroup IS NULL THEN p.context_campaign_adgroup
		ELSE p.context_utm_adgroup
	END adgroup

FROM astronomer_production.page p

WHERE
    p.user_id IN (%s)
    AND (p.context_campaign_source IS NOT NULL OR p.context_utm_source IS NOT NULL)
	AND (p.context_utm_campaign IS NOT NULL OR p.context_utm_campaign IS NOT NULL)
	AND p.received_at < '%s'

GROUP BY
    p.received_at
    ,medium
    ,source
    ,campaign
    ,term
    ,adgroup

UNION ALL

SELECT

	p.received_at
	,p.context_campaign_medium
	,p.context_campaign_source
	,p.context_campaign_name
	,p.context_campaign_term
	,p.context_campaign_adgroup adgroup

FROM astronomer_production_global.page p

WHERE
    (p.user_id IN (%s) or p.anonymous_id IN (%s))
    AND p.context_campaign_source IS NOT NULL
	AND p.received_at < '%s'

GROUP BY
	p.received_at
	,p.context_campaign_medium
	,p.context_campaign_source
	,p.context_campaign_name
	,p.context_campaign_term
	,p.context_campaign_adgroup
)

SELECT * FROM tb ORDER BY time_stamp DESC;
"""
)


bs4_bookings = (
"""
SELECT user_id
FROM astronomer_production.booking_inquiry_created
WHERE booking_id = %s;
"""
)

bs4_astronomer_ids = (
"""
WITH bs_ids as (
SELECT bs_user_id
FROM astronomer_production.identify i
WHERE user_id = '%s'
GROUP BY bs_user_id)
SELECT DISTINCT anonymous_id
FROM
(
SELECT anonymous_id
FROM astronomer_production.identify
WHERE bs_user_id IN (SELECT * FROM bs_ids)
UNION ALL
SELECT anonymous_id
FROM astronomer_production.page
WHERE user_id = '%s'
)
"""
)
