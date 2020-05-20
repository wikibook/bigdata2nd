set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict; 

insert overwrite table Managed_SmartCar_drive_Info partition(biz_date)  
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
 t2.speed_pedal, 
 t2.break_pedal, 
 t2.steer_angle, 
 t2.direct_light , 
 t2.speed , 
 t2.area_number ,
 t2.r_date,
 substring(t2.r_date, 0, 8) as biz_date 
from  SmartCar_Master_Over18 t1 join SmartCar_Drive_Info_2 t2 
on t1.car_number = t2.car_number and substring(t2.r_date,0,8) = '${working_day}';