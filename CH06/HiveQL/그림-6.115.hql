insert overwrite local directory '/home/pilot-pjt/item-buy-list'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
select car_number, concat_ws("," , collect_set(item))
from managed_smartcar_item_buylist_info
group by car_number