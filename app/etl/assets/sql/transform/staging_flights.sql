with newflight_price as (
	SELECT DATE_PART('year', DATE("departureDate")) as departureYear
	, DATE_PART('month', DATE("departureDate")) as departureMonth
	, DATE_PART('day', DATE("departureDate")) as departureDay
	, EXTRACT(isodow from DATE("departureDate")) as depatureDayOfWeek
	-- 1 is Monday, 7 is Sunday 
	, CAST("cheapestPrice" AS NUMERIC) as price
	, CAST(duration AS NUMERIC)
	, *
	FROM public.flight_price
						 )
						 
-- Table 1						 
select count(*)as numberofflight, round(avg(price),1) as averageprice, depatureDayOfWeek, origin, destination
	from newflight_price
	group by depatureDayOfWeek, origin, destination
	order by destination
	
-- Table 2
select count(*)as numberofflight, round(avg(price),1) as averageprice, departureMonth, origin, destination
	from newflight_price
	group by departureMonth, origin, destination
	order by destination