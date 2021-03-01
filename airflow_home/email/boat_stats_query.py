sql=(
"""
WITH won_ops AS (
SELECT
	b.opportunity_id op_id
	,b.id successful_bk_id
	,p.boat_id
	,bt.primary_manager_id bt_owner_id
FROM bs4.bookings b
JOIN bs4.packages p
	ON p.id = b.package_id
JOIN bs4.boats bt
	ON bt.id = p.boat_id
WHERE b.state IN ('concluded', 'disputed', 'aboard', 'ashore', 'approved')
	AND b.trip_start::DATE BETWEEN '%s' AND '%s'
GROUP BY op_id, successful_bk_id, p.boat_id, bt_owner_id
),

boat_earnings AS (
SELECT
	bt.id
	,ROUND(sum(b.owner_payout_cents)/100::DECIMAL,2) total_earnings
FROM bs4.bookings b
JOIN bs4.packages p
	ON p.id = b.package_id
JOIN bs4.boats bt
	ON bt.id = p.boat_id
WHERE
	b.trip_start::DATE BETWEEN '%s' AND '%s'
	AND b.state IN ('concluded', 'disputed', 'aboard', 'ashore', 'approved')
GROUP BY bt.id
),

lost_earnings AS
(
SELECT
	bt.id
	,ROUND(SUM(b.owner_payout_cents)/100::DECIMAL,2) lost_earnings
FROM bs4.bookings b
JOIN bs4.packages p
	ON p.id = b.package_id
JOIN bs4.boats bt
	ON bt.id = p.boat_id
JOIN won_ops w
	ON w.op_id = b.opportunity_id
	AND w.bt_owner_id != bt.primary_manager_id
WHERE w.op_id IS NOT NULL
GROUP BY bt.id
),

search_score as

(with district_search_scores as
(SELECT
	a.district_id
	,bt.search_score
	,row_number() OVER (PARTITION BY district_id ORDER BY f.search_score DESC)
FROM bs4.boats bt
JOIN bs4.fleets f
	ON f.boat_id=bt.id
JOIN bs4.addresses a
	ON a.addressable_id=bt.id
	AND a.addressable_type='Boat'
WHERE f.searchable = True
	AND bt.state = 'approved'
	AND bt.search_score IS NOT NULL
	AND a.country_code = 'US'
	)
SELECT
	CASE
		WHEN d.district_id IS NULL THEN 0
		ELSE d.district_id
	END as district_id
	,CASE
		WHEN tb.search_score IS NULL THEN AVG(d.search_score)
		ELSE tb.search_score
	END as search_threshold
FROM district_search_scores d
LEFT JOIN
(SELECT district_id, search_score FROM district_search_scores WHERE row_number = 20) tb
on tb.district_id = d.district_id
GROUP BY
	d.district_id
	,tb.search_score
ORDER BY district_id
)

SELECT
	u.email
	,bt.id boat_id
	,CASE
		WHEN bt.make_and_model IS NULL THEN bt.length::TEXT+' '+bt.make
		ELSE bt.make_and_model
	END as b_name
	,f.boat_image
	,u.first_name
	,COUNT(DISTINCT v.user_id) boat_views
	,COUNT(DISTINCT b.opportunity_id) opportunities
	,COUNT(
		DISTINCT CASE
			WHEN b.state IN ('aboard', 'ashore', 'concluded', 'disputed', 'approved') THEN b.id
		END) as completed_bookings
	,CASE
		WHEN e.total_earnings IS NULL THEN 0
		ELSE e.total_earnings
	END as earnings
	,CASE
		WHEN l.lost_earnings is null then 0
		ELSE l.lost_earnings
	END as money_lost
	,bt.public_id
	,CASE
		WHEN bt.search_score < search_threshold THEN True
		ELSE False
	END as views_tip
	,CASE
		WHEN COUNT(DISTINCT v.user_id) = 0 THEN False
		ELSE
			CASE
				WHEN COUNT(DISTINCT b.opportunity_id)/COUNT(DISTINCT v.user_id)::DECIMAL < .03 THEN True
				ELSE False
			END
	END ops_tip

FROM bs4.boats bt
JOIN bs4.addresses a
	ON a.addressable_type='Boat'
	AND a.addressable_id=bt.id
	AND a.country_code = 'US'
JOIN bs4.users u
	ON bt.primary_manager_id = u.id
JOIN bs4.fleets f
	ON f.boat_id = bt.id
LEFT JOIN bs4.packages p
	ON p.boat_id = bt.id
LEFT JOIN bs4.bookings b
	ON b.package_id = p.id
	AND b.trip_start::DATE BETWEEN '%s' AND '%s'
LEFT JOIN astronomer_production.viewed_boat_listing v
	ON SPLIT_PART(v.context_page_path, '/',3) = bt.public_id
	AND v.received_at::DATE BETWEEN '%s' AND '%s'
LEFT JOIN lost_earnings l
	ON l.id = bt.id
LEFT JOIN boat_earnings e
	ON e.id = bt.id
JOIN search_score ss
	ON ss.district_id = a.district_id
WHERE f.searchable = True
AND bt.state = 'approved'

GROUP BY
	u.email
	,u.first_name
	,earnings
	,b_name
	,f.boat_image
	,bt.id
	,bt.public_id
	,money_lost
	,bt.search_score
	,views_tip

ORDER BY earnings DESC, opportunities DESC

"""
)
