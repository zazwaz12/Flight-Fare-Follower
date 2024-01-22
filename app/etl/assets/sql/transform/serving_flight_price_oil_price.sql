with avg_price_per_date as (
	select "viewedAt", avg(CAST("cheapestPrice" AS NUMERIC)) avg_price
	from flight_price
	group by "viewedAt"
)
select ap."viewedAt", 
	ROUND(ap.avg_price, 2) as average_price, 
	ROUND(CAST(op.price AS NUMERIC), 2) as oil_price 
from avg_price_per_date as ap 
join oil_price as op 
on ap."viewedAt" = op."viewedAt"