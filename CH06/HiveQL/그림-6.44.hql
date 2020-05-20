create external table if not exists SmartCar_Status_Info (
reg_date string,
car_number string,
tire_fl string,
tire_fr string,
tire_bl string,
tire_br string,
light_fl string,
light_fr string,
light_bl string,
light_br string,
engine string,
break string,
battery string
)
partitioned by( wrk_date string )
row format delimited
fields terminated by ','
stored as textfile
location '/pilot-pjt/collect/car-batch-log/'