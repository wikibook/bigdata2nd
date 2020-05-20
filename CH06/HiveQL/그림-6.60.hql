
CREATE EXTERNAL TABLE SmartCar_Item_BuyList (
car_number string,
Item string,
score string,
month string
)
row format delimited
fields terminated by ','
stored as textfile
location '/pilot-pjt/collect/buy-list';