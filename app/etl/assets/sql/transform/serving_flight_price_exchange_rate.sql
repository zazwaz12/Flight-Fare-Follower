
with avg_price_per_destination as (
	select "viewedAt", destination, avg(CAST("cheapestPrice" AS NUMERIC)) avg_price
	from flight_price
	group by "viewedAt", destination
)
select ap."viewedAt", 
	ap.destination, 
	ROUND(ap.avg_price, 2) as average_price, 
	ROUND(CAST(ep.value AS NUMERIC), 2) as exchange_rate 
from avg_price_per_destination as ap 
join exchange_price as ep 
on ap."viewedAt" = ep."viewedAt"
and ap.destination = ep."airportCode"