create table if not exists Managed_SmartCar_Emergency_Check_Info (
car_number string,
tire_check string,
light_check string,
engine_check string,
break_check string,
battery_check string,
biz_date string
)
row format delimited
fields terminated by ','
stored as textfile;
