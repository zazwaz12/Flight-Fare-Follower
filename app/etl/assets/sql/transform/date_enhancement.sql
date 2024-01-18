SELECT DATE_PART('year', DATE("departureDate")) as departureYear
	, DATE_PART('month', DATE("departureDate")) as departureMonth
	, DATE_PART('day', DATE("departureDate")) as departureDay
	, EXTRACT(isodow from DATE("departureDate")) as depatureDayOfWeek
	-- 1 is Monday, 7 is Sunday 
	, *
	FROM public.flight_price