
SELECT DATE_PART('year', DATE("departureDate")) as departureYear
	, DATE_PART('month', DATE("departureDate")) as departureMonth
	, DATE_PART('day', DATE("departureDate")) as departureDay
	, CASE EXTRACT(isodow from DATE("departureDate"))
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    	END as departureDayOfWeek
	-- 1 is Monday, 7 is Sunday 
	, CAST("cheapestPrice" AS NUMERIC) as price
	, CAST(duration AS NUMERIC) as duration
	, "viewedAt"
	, origin
	, destination
	, "departureDate"
	, "returnDate"
FROM flight_price