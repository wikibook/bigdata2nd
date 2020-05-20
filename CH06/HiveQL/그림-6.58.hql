CREATE EXTERNAL TABLE SmartCar_Master (
car_number string,
sex string,
age string,
marriage string,
region string,
job string,
car_capacity string,
car_year string,
car_model string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/pilot-pjt/collect/car-master';