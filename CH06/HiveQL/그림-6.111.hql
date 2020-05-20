insert into table Managed_SmartCar_Emergency_Check_Info 

select
          t1.car_number,
          t2.symptom as tire_symptom,
          t3.symptom as light_symptom,
          t4.symptom as engine_symptom,
          t5.symptom as break_symptom,
          t6.symptom as battery_symptom,
          t1.biz_date
from 
         (select distinct car_number as car_number, biz_date from managed_smartcar_status_info  where biz_date = '${working_day}') t1 

left outer join (   select 
                       car_number,
                       avg(tire_fl) as tire_fl_avg , 
                       avg(tire_fr) as tire_fr_avg ,
                       avg(tire_bl) as tire_bl_avg ,
                       avg(tire_br) as tire_br_avg ,
                       'Tire Check' as symptom
                    from managed_smartcar_status_info where biz_date ='${working_day}'
                    group by car_number
                    having  tire_fl_avg < 80 or tire_fr_avg < 80 or  tire_bl_avg < 80 or tire_br_avg < 80 ) t2  
on t1.car_number = t2.car_number

left outer join (  select 
                        distinct car_number,
                        'Light Check' as symptom
                    from managed_smartcar_status_info 
                    where biz_date = '${working_day}' and (light_fl = '2' or light_fr = '2' or light_bl = '2' or light_br = '2')) t3 
on t1.car_number = t3.car_number

left outer join (   select 
                       distinct car_number,
                      'Engine Check' as symptom
                    from managed_smartcar_status_info 
                    where biz_date = '${working_day}' and engine = 'C' ) t4
on t1.car_number = t4.car_number

left outer join (   select 
                        distinct car_number,
                        'Brake Check' as symptom
                    from managed_smartcar_status_info 
                    where biz_date = '${working_day}' and break = 'C' ) t5
     
on t1.car_number = t5.car_number

left outer join (   select 
                        car_number,
                        avg(battery) as battery_avg,
                        'Battery Check' as symptom
                    from managed_smartcar_status_info where biz_date = '${working_day}'
                    group by car_number having battery_avg < 30 ) t6
on t1.car_number = t6.car_number

where t2.symptom is not null or t3.symptom is not null or t4.symptom is not null  or t5.symptom is not null  or t6.symptom is not null