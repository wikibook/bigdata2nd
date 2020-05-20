insert overwrite local directory '/home/pilot-pjt/mahout-data/recommendation/input'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
select hash(car_number ), hash(item), score from managed_smartcar_item_buylist_info