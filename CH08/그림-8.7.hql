CREATE EXTERNAL TABLE SmartCar_Master2Income (
car_number string,
sex string,
age string,
marriage string,
region string,
job string,
car_capacity int,
car_year string,
car_model string,
income int
)
row format delimited
fields terminated by '|'
stored as textfile
tblproperties ("skip.header.line.count"="1");