with max_date as (
	select max("viewedAt") as "maxDate"
	from flight_price
)
select min("cheapestPrice") as "cheapestPrice", 
	max("cheapestPrice") as "mostExpensivePrice"
from flight_price as fp 
inner join max_date as md 
on fp."viewedAt" = md."maxDate"
where destination = 'HKG'
and duration = 9 