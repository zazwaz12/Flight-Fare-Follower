select count(*) as numberOfFlight
    , round(avg(price),1) as averagePrice
    , departureDayOfWeek
    , origin
    , destination
from staging_flights
group by departureDayOfWeek, origin, destination
order by destination