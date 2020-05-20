create table if not exists Managed_SmartCar_Drive_Info (
car_number string,
sex string,
age string,
marriage string,
region string,
job string,
car_capacity string,
car_year string,
car_model string,
  
speed_pedal string, 
break_pedal string, 
steer_angle string, 
direct_light string, 
speed string, 
area_number string,
reg_date string
)
partitioned by( biz_date string )
row format delimited
fields terminated by ','
stored as textfile;