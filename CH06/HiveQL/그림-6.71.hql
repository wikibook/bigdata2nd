set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict; 

insert overwrite table Managed_SmartCar_Status_Info partition(biz_date)  
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
 t2.tire_fl,
 t2.tire_fr,
 t2.tire_bl,
 t2.tire_br,
 t2.light_fl,
 t2.light_fr,
 t2.light_bl,
 t2.light_br,
 t2.engine,
 t2.break,
 t2.battery,
 t2.reg_date,
 substring(t2.reg_date, 0, 8) as biz_date 
from  SmartCar_Master_Over18 t1 join SmartCar_Status_Info t2 
on t1.car_number = t2.car_number and t2.wrk_date = '${working_day}';
