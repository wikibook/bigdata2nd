create external table if not exists SmartCar_Drive_Info_2 (
  r_key string, 
  r_date string, 
  car_number string, 
  speed_pedal string, 
  break_pedal string, 
  steer_angle string, 
  direct_light string, 
  speed string, 
  area_number string
)
partitioned by( wrk_date string )
row format delimited
fields terminated by ','
stored as textfile
location '/pilot-pjt/collect/drive-log/'