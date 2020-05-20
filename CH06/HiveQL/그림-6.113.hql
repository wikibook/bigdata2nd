create table if not exists Managed_SmartCar_Item_BuyList_Info (
 car_number string,
 sex string,
 age string,
 marriage string,
 region string,
 job string,
 car_capacity string,
 car_year string,
 car_model string,
 item string,
 score string
)
partitioned by( biz_month string )
row format delimited
fields terminated by ','
stored as textfile;