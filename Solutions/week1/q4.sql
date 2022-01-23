select tip_amount,tpep_pickup_datetime::date from yellow_taxi_data where extract(month from tpep_pickup_datetime)=1 order by tip_amount desc limit 1 
