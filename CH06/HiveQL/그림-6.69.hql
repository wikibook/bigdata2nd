create table if not exists Managed_SmartCar_Status_Info (
car_number string,
sex string,
age string,
marriage string,
region string,
job string,
car_capacity string,
car_year string,
car_model string,
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
battery string,
reg_date string
)
partitioned by( biz_date string )
row format delimited
fields terminated by ','
stored as textfile;