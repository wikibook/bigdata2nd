set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict; 

insert overwrite table Managed_SmartCar_Item_BuyList_Info partition(biz_month)  
select 
   t1.car_number,
   t1.sex,
   t1.age,
   t1.marriage,
   t1.region,
   t1.job,
   t1.car_capacity,
   t1.car_year,
   t1.car_model,
   t2.item,
   t2.score,
   t2.month as biz_month
 from 
  SmartCar_Master_Over18 t1 join SmartCar_Item_Buylist t2 
on 
  t1.car_number = t2.car_number
where
  t2.month = '202003'
