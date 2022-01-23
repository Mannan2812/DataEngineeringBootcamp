select count(*) from yellow_taxi_data where extract(month from tpep_pickup_datetime)=1 and extract(day from tpep_pickup_datetime)=15 
