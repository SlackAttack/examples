bs3=(
"""
SELECT
    r.id
    ,r.check_out
    ,CASE
        WHEN r.status_id IN (4,5) THEN 'approved'
        WHEN r.status_id IN (9) THEN 'concluded'
    END as booking_state
    ,CASE
        WHEN id.source IS NULL THEN 'Direct'
        ELSE id.source
    END as source
    ,r.boat_id
    ,i.total gmv
    ,(CASE WHEN p.payout IS NULL THEN 0 ELSE p.payout END) -- owner payout
    +(CASE WHEN c.payout IS NULL THEN 0 ELSE c.payout END) -- captain payout
    +(CASE WHEN tax.value IS NULL THEN 0 ELSE tax.value END) -- taxes paid to owner
         as total_payout
    ,CASE
        WHEN towing.value IS NULL THEN 0
        ELSE towing.value
    END AS towing_cost
    ,gmv-total_payout-towing_cost revenue
    ,sum(CASE
        WHEN ins.value IS NULL THEN 0
        ELSE ins.value
    END) as insurance
    ,CASE WHEN w.reserve_id IS NULL THEN (gmv*.029)+.3 ELSE 0 END AS cc_fees
    ,ROUND(SUM(CASE WHEN a.payout_amount IS NULL THEN 0 ELSE a.payout_amount END),2) as aff_payout
    ,revenue-insurance-cc_fees-aff_payout gp

FROM bs3.reserves r
JOIN bs3.invoices i
    ON i.reserve_id=r.id
LEFT JOIN bs3_aux.owner_payouts p
    ON p.reserve_id=r.id
LEFT JOIN bs3_aux.captain_payouts c
    ON c.reserve_id=r.id
LEFT JOIN bs3.invoice_details tax
    ON tax.invoice_id=i.id
    AND tax.fee_type_key='tax'
LEFT JOIN bs3.invoice_details ins
    ON ins.invoice_id=i.id
    AND ins.fee_type_key IN (
        'boatInsurance'
        ,'captPassengerInsurancePremium'
        ,'captainInsurance')
LEFT JOIN bs3.invoice_details towing
    ON towing.invoice_id=i.id
    AND towing.fee_type_key='towing'
LEFT JOIN bs3_aux.wire_transfer_reserves w
    ON w.reserve_id=r.id
LEFT JOIN random.affiliate_payouts a
    ON a.platform='BS3'
    AND a.booking_id=r.id
LEFT JOIN random.indirect_rentals id
    ON id.platform='BS3'
    AND id.booking_id=r.id

WHERE to_char(r.check_out, 'YYYY') >= '%s'
    AND r.status_id IN (5,4,9)
    AND i.deposit = 0

GROUP BY
    r.id
    ,i.total
    ,tax.value
    ,towing.value
    ,r.status_id
    ,id.source
    ,r.check_out
    ,r.boat_id
    ,total_payout
    ,cc_fees
    ,id.affiliate_payout;
""")

bs4=(
"""
SELECT
    b.id
    ,b.trip_start
    ,b.state booking_state
    ,CASE
        WHEN id.source IS NULL THEN 'Direct'
        ELSE id.source
    END AS source
    ,bt.id boat_id
    ,CASE
    	WHEN b.boat_pass_price_adjustment_cents < 0 THEN
    	ROUND(
    	(package_price_cents
		+ date_price_adjustment_cents
		+ owner_price_adjustment_cents
		+ coupon_price_adjustment_cents
		+ boating_credit_adjustment_cents
		+ service_fee_cents
		+ captain_price_cents
		+ towing_cost_cents)/100::DECIMAL,2 )
		WHEN c.giftcard_id IS NOT NULL THEN
		 ROUND(
    	(package_price_cents
		+ date_price_adjustment_cents
		+ owner_price_adjustment_cents
		+ boating_credit_adjustment_cents
		+ service_fee_cents
		+ captain_price_cents
		+ towing_cost_cents)/100::DECIMAL,2 )
		ELSE
		ROUND((b.price_with_service_fee_cents-b.sales_tax_cents)/100::DECIMAL, 2)
	END as gmv


    ,ROUND(
    	((CASE
    		WHEN adj.actual_payout_cents IS NOT NULL THEN adj.actual_payout_cents
    		ELSE b.owner_payout_cents
    	END)
        +
        (CASE
            WHEN b.captain_payout_cents IS NULL THEN 0
            ELSE b.captain_payout_cents
        END))/100::DECIMAL,2) owner_payout
    ,ROUND(b.towing_cost_cents/100::DECIMAL,2) towing_cost

    ,(gmv
        -owner_payout
        -towing_cost) revenue

    ,CASE
        WHEN ip.amount_cents IS NULL THEN ROUND(b.insurance_cost_cents/100::NUMERIC,2)
        ELSE ROUND(regexp_replace(amount_cents,'([^0-9.])','')/100::DECIMAL,2)
     END AS insurance_costs
    ,(ROUND(b.price_with_service_fee_cents/100::DECIMAL)*.027)+.3 cc_fees
    ,ROUND(
        CASE
            WHEN a.payout_amount IS NULL THEN 0
            ELSE a.payout_amount
        END,2) aff_payout
    ,(revenue-aff_payout-insurance_costs-cc_fees) gross_profit

FROM bs4.bookings b
LEFT JOIN bs4.insurance_payouts ip
    ON ip.booking_id = b.id
JOIN bs4.packages p
    ON p.id=b.package_id
JOIN bs4.boats bt
    ON bt.id=p.boat_id
LEFT JOIN random.indirect_rentals id
    ON id.platform='BS4' AND id.booking_id=b.id
LEFT JOIN random.affiliate_payouts a
    ON a.platform='BS4' AND a.booking_id=b.id
LEFT JOIN bs4_aux.owner_payout_adjustments adj
	ON adj.booking_id = b.id
LEFT JOIN bs4.coupon_redemptions cr
	ON cr.booking_id = b.id
LEFT JOIN bs4.coupons c
	ON c.id = cr.coupon_id
WHERE 1=1
    AND to_char(b.trip_start, 'YYYY') >= %s
    AND b.state IN ('concluded', 'disputed', 'aboard', 'ashore', 'approved')
    AND gmv > -10000
""")

cg_global = (
"""
WITH other_items as (
SELECT
	o.order_id
	,oi.parentorderitem_id
	,SUM(
		CASE
			WHEN (oi.systemcode != 1001
					AND (commissionoption IN (0,1) OR commissionoption IS NULL)) THEN oi.netamount
			WHEN (oi.systemcode != 1001 AND commissionoption =2) THEN oi.netamount*(1-(commissionrate/100))
		END) net_payout
	,sum(CASE WHEN oi.totalamount IS NULL THEN 0 ELSE oi.totalamount END) total_amount
	,sum(CASE WHEN oi.netamount IS NULL THEN 0 ELSE oi.netamount END) netamount

FROM charter_genius.order o
JOIN charter_genius.orderitem oi
	ON oi.t_order_id = o.order_id
LEFT JOIN charter_genius.orderitem parentoi
	ON oi.parentorderitem_id = parentoi.orderitem_id
LEFT JOIN charter_genius.stockitem si
	ON si.item_id = parentoi.t_stockitem_id
LEFT JOIN charter_genius.stockitemaccountlink slink
	ON slink.t_stockitem_id=si.item_id
LEFT JOIN charter_genius.orderitempricing oip
	ON oip.id = oi.orderitem_id
WHERE o.t_issuer_id = 11178
	AND o.orderstatus IN (5,7)
	AND oi.netamount > 0
	AND oi.systemcode != 1
	AND oi.systemcode IS NOT NULL
	AND oi.netamount > 0
GROUP BY o.order_id, oi.parentorderitem_id
ORDER BY
	order_id DESC),

bookings as (
SELECT
	o.order_id
	,oi.orderitem_id
	,o.orderstatus
	,si.item_id item_id
	,sum(oi.totalamount) totalamount
	,sum(oi.netamount) netamount
FROM charter_genius.order o
JOIN charter_genius.orderitem oi
	ON oi.t_order_id = o.order_id
JOIN charter_genius.stockitem si
	ON si.item_id = oi.t_stockitem_id
	AND si.item_id IS NOT NULL
WHERE o.t_issuer_id = 11178
	AND o.orderstatus IN (5,7)
	AND oi.netamount > 0
	AND oi.systemcode = 1
GROUP BY o.order_id, o.orderstatus, oi.orderitem_id, si.item_id),

stripe_rates as (
SELECT
    order_id
    ,sum(fees)/sum(amount)::DECIMAL rate
FROM stripe.charter_genius_eu_charges
WHERE status = 'succeeded'
AND refunded = False
GROUP BY order_id
UNION ALL
SELECT order_id, sum(fees)/sum(amount)::DECIMAL rate
FROM stripe.charter_genius_us_charges
WHERE status = 'succeeded'
AND refunded = False
GROUP BY order_id
)

SELECT
	b.order_id
	,et.startson
	,CASE
		WHEN o.orderstatus = 5 THEN 'approved'
		ELSE 'concluded'
	END as state
	,CASE
		WHEN ir.source IS NULL THEN 'Direct'
		ELSE ir.source
	END as source_
	,b.item_id

	,ROUND(
        b.netamount
        +CASE
            WHEN r.netamount IS NULL THEN 0
            ELSE r.netamount
        END,2) gmv

	,ROUND(
    b.netamount
    *(1
    -
    (CASE
		WHEN slink.commissionrate > 1 THEN slink.commissionrate/100
		ELSE slink.commissionrate
	END))
    +CASE
        WHEN r.net_payout IS NULL THEN 0
        ELSE r.net_payout
    END,2) as total_payout

	,gmv-total_payout revenue

	,ROUND(
        (b.totalamount
        +CASE
            WHEN r.total_amount IS NULL THEN 0
            ELSE r.total_amount
        END)
		*CASE
            WHEN sf.rate IS NULL THEN 0
            ELSE sf.rate
        END,2) cc_fees

	,CASE
		WHEN ap.payout_amount IS NULL THEN 0
		ELSE ap.payout_amount
	END AS aff_payout
    ,o.currency
    ,CASE
		WHEN ex.rate IS NULL THEN
            (SELECT rate FROM random.euro_to_usd ORDER BY date DESC LIMIT 1)
		ELSE ex.rate
	END as ex_rate

FROM bookings b
JOIN charter_genius.order o
	ON o.order_id = b.order_id
LEFT JOIN charter_genius.stockitemaccountlink slink
	ON slink.t_stockitem_id=b.item_id
LEFT JOIN other_items r
	ON r.order_id = o.order_id
	AND r.parentorderitem_id = b.orderitem_id
LEFT JOIN charter_genius.orderitemevent oie
	ON oie.t_orderitem_id = b.orderitem_id
LEFT JOIN charter_genius.event e
	ON e.event_id = oie.t_event_id
LEFT JOIN charter_genius.eventtime et
	ON et.t_event_id = e.event_id
LEFT JOIN random.indirect_rentals ir
	ON ir.platform = 'Charter Genius'
	AND ir.booking_id = o.order_id
LEFT JOIN stripe_rates sf
	ON sf.order_id = o.order_id
LEFT JOIN random.affiliate_payouts ap
	ON ap.platform = 'Charter Genius'
	AND ap.booking_id = o.order_id
LEFT JOIN random.euro_to_usd ex
	ON ex.date = et.startson::DATE
LEFT JOIN charter_genius.stockitemaccountlink sl
	ON sl.t_stockitem_id = b.item_id
LEFT JOIN charter_genius.id agent_id
	ON agent_id.entity_id = sl.t_accountentity_id

WHERE agent_id.entity_id != '12039'
AND TO_CHAR(et.startson, 'YYYY') > '1900'

GROUP BY
	b.order_id
	,et.startson
	,state
	,source_
	,item_id
	,gmv
	,total_payout
	,revenue
	,cc_fees
	,aff_payout
	,currency
	,ex_rate
	,name;
"""
)

saas=(
"""
WITH stripe_charges as
(SELECT
	c.order_id
	,c.currency
	,sum(c.amount) charge_amount
	,ROUND(sum(c.fees)/sum(c.amount)::DECIMAL,4) fee_rate
FROM stripe.charter_genius_eu_charges c
WHERE c.status = 'succeeded'
	AND c.refunded = False
GROUP BY c.order_id, c.currency
UNION ALL
SELECT
	c.order_id
	,c.currency
	,sum(c.amount) charge_amount
	,sum(c.fees)/sum(c.amount)::DECIMAL fee_rate
FROM stripe.charter_genius_us_charges c
WHERE c.status = 'succeeded'
	AND c.refunded = False
GROUP BY c.order_id, c.currency
),

stripe_transfers as
(SELECT
	c.order_id
	,sum(c.amount) transfer_amount
FROM stripe.charter_genius_eu_transfers c
GROUP BY c.order_id
UNION ALL
SELECT
	c.order_id
	,sum(c.amount) transfer_amount
FROM stripe.charter_genius_us_transfers c
GROUP BY c.order_id
),

tb as (
SELECT
	s.order_id
	,s.charge_amount
	,s.currency
	,s.fee_rate
	,transfer_amount/charge_amount::DECIMAL payout_rate
FROM stripe_charges s
JOIN stripe_transfers t
	ON t.order_id = s.order_id)

SELECT
	sb.order_id
	,sb.startson
	,operator_name source
	,ROUND(sb.gmv_usd,2) gmv
	,ROUND(sb.gmv_usd*tb.payout_rate,2) totalpayout
	,ROUND((gmv_usd - totalpayout),2) revenue
	,CASE
		WHEN tb.currency = 'eur' THEN
            ROUND(((tb.charge_amount/100::DECIMAL)*tb.fee_rate)*con.rate,2)
		ELSE ROUND((tb.charge_amount/100::DECIMAL)*tb.fee_rate,2)
	END as stripe_fees
	,CASE
		WHEN sb.orderstatus = 5 THEN 'approved'
		ELSE 'concluded'
	END
	,oi.t_stockitem_id

FROM charter_genius_aux.saas_bookings sb
JOIN tb
	ON tb.order_id = sb.order_id
JOIN random.euro_to_usd con
	ON con.date = startson::DATE
JOIN charter_genius.orderitem oi
	ON oi.orderitem_id = sb.orderitem_id
WHERE sb.orderstatus IN (5,7)
	AND startson::DATE < getdate()::DATE;
"""
)


sci=(
"""
WITH trips_per_booking as (
SELECT
	o.order_id
	,oi.orderitem_id
	,COUNT(DISTINCT et.startson) trips
FROM charter_genius.order o
JOIN charter_genius.orderitem oi
	ON oi.systemcode = 1
	AND oi.t_order_id = o.order_id
LEFT JOIN charter_genius.orderitemevent oie
	ON oie.t_orderitem_id = oi.orderitem_id
LEFT JOIN charter_genius.event e
	ON e.event_id = oie.t_event_id
LEFT JOIN charter_genius.eventtime et
	ON et.t_event_id = e.event_id
WHERE o.t_issuer_id = 9
AND oi.systemcode = 1 AND oi.type = 2
GROUP BY
	o.order_id
	,oi.orderitem_id
HAVING trips > 0),

order_addons as (
SELECT
	oi.t_order_id
	,ROUND(b.netamount/(b.netamount-sum(oi.netamount)),6) rate
FROM charter_genius.orderitem oi
JOIN charter_genius.order b
	ON b.order_id=oi.t_order_id
WHERE (oi.systemcode != 1 OR oi.systemcode IS NULL)
	AND oi.parentorderitem_id IS NULL
	AND b.totalamount != 0
	AND b.t_issuer_id = 9
    AND oi.type != 5
GROUP BY oi.t_order_id, b.netamount
HAVING (b.netamount-sum(oi.netamount)) != 0),

order_discounts as (
SELECT
	oi.t_order_id
	,ROUND(b.netamount/(b.netamount-sum(oi.netamount)),6) rate
FROM charter_genius.orderitem oi
JOIN charter_genius.order b
	ON b.order_id=oi.t_order_id
WHERE (oi.systemcode != 1 OR oi.systemcode IS NULL)
	AND oi.parentorderitem_id IS NULL
	AND b.totalamount != 0
	AND b.t_issuer_id = 9
    AND oi.type = 5
GROUP BY oi.t_order_id, b.netamount
HAVING (b.netamount-sum(oi.netamount)) != 0),

stripe_fees as (
SELECT order_id, SUM(fees) fees
FROM
(SELECT
	order_id
	,sum(fees) fees
FROM smart_charter_ibiza.stripe_charges
WHERE status='succeeded'
AND refunded = False
GROUP BY order_id
HAVING sum(amount) > 0
UNION ALL
SELECT
    order_id
    ,sum(fees) fees
FROM stripe.charter_genius_eu_charges
WHERE status = 'succeeded'
AND refunded = False
GROUP BY order_id
HAVING sum(amount) > 0)
GROUP BY order_id
)

SELECT

	o.order_id
	,et.startson
	,CASE
		WHEN o.orderstatus = 5 THEN 'approved'
		ELSE 'concluded'
	END state
	,oi.t_stockitem_id boat_id


	,(oi.totalamount
	+SUM(CASE
		WHEN bd.totalamount IS NULL THEN 0
		ELSE bd.totalamount
	END)
	+CASE
		WHEN fuel.totalamount IS NULL THEN 0
		ELSE fuel.totalamount
	END
	+SUM(CASE
		WHEN addon.totalamount IS NULL THEN 0
		ELSE addon.totalamount
	END))/tpb.trips::DECIMAL totalorderitem

	,ROUND((oi.netamount
	+SUM(CASE
		WHEN bd.netamount IS NULL THEN 0
		ELSE bd.netamount
	END))/tpb.trips::DECIMAL,2) netboat

	,ROUND((CASE
		WHEN fuel.netamount IS NULL THEN 0
		ELSE fuel.netamount
	END)/tpb.trips::DECIMAL,2) netfuel

	,SUM(CASE
		WHEN addon.netamount IS NULL THEN 0
		ELSE addon.netamount
	END)/tpb.trips::DECIMAL netaddon
	,CASE
		WHEN od.rate IS NULL THEN 1
		ELSE ROUND(od.rate,6)
	END orderdiscountrate
	,CASE
		WHEN rt.rate IS NULL THEN 1
		ELSE ROUND(rt.rate,6)
	END orderaddonrate

	,CASE
        WHEN stripe.fees IS NULL THEN 0
        ELSE ROUND(stripe.fees/100::DECIMAL,2)
    END as fees

    ,CASE
        WHEN ap.net_payout_usd IS NULL THEN 0
        ELSE ap.net_payout_usd
    END

	,CASE
		WHEN ex.rate IS NULL THEN (SELECT rate FROM random.euro_to_usd ORDER BY date DESC LIMIT 1)
		ELSE ex.rate
	END as ex_rate

FROM charter_genius.order o
JOIN charter_genius.orderitem oi
	ON oi.systemcode = 1
	AND oi.t_order_id = o.order_id
LEFT JOIN charter_genius.orderitem bd
	ON bd.parentorderitem_id = oi.orderitem_id
	AND bd.type = 5
LEFT JOIN charter_genius.orderitem fuel
	ON fuel.parentorderitem_id = oi.orderitem_id
	AND fuel.systemcode IN (3)
LEFT JOIN charter_genius.orderitem addon
	ON addon.parentorderitem_id = oi.orderitem_id
	AND (addon.systemcode IN (0,2) OR addon.systemcode IS NULL)
	AND addon.type != 5
LEFT JOIN charter_genius.orderitemevent oie
	ON oie.t_orderitem_id = oi.orderitem_id
LEFT JOIN charter_genius.event e
	ON e.event_id = oie.t_event_id
LEFT JOIN charter_genius.eventtime et
	ON et.t_event_id = e.event_id
LEFT JOIN order_addons rt
	ON rt.t_order_id = o.order_id
LEFT JOIN trips_per_booking tpb
	ON tpb.order_id = o.order_id
	AND tpb.orderitem_id = oi.orderitem_id
LEFT JOIN random.euro_to_usd ex
	ON ex.date=et.startson::DATE
LEFT JOIN stripe_fees stripe
	ON stripe.order_id = o.order_id
LEFT JOIN order_discounts od
    ON od.t_order_id = o.order_id
LEFT JOIN smart_charter_ibiza.affiliate_payouts ap
	ON ap.order_id = o.order_id
	AND et.startson::DATE = ap.trip_date
	AND oi.t_stockitem_id = ap.boat_id
WHERE o.t_issuer_id = 9
	AND to_char(et.startson, 'YYYY') >= %s
	AND o.orderstatus IN (5,7)

GROUP BY
	o.order_id
	,oi.orderitem_id
	,oi.t_stockitem_id
	,o.orderstatus
	,et.startson
	,ex_rate
	,netfuel
	,orderdiscountrate
	,orderaddonrate
	,fees
	,oi.totalamount
	,tpb.trips
	,oi.netamount
	,fuel.totalamount
	,ap.net_payout_usd
ORDER BY startson;
"""
)
