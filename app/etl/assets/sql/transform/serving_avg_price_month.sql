select count(*) as numberOfFlight
    , round(avg(price),1) as averagePrice
    , departureMonth
    , origin
    , destination
from staging_flights
group by departureMonth, origin, destination
order by destination