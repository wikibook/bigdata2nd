insert into table Managed_SmartCar_Symptom_Info  
select 
       t1.car_number,
       t1.speed_p_avg_by_carnum,
       case
         when (abs((t1.speed_p_avg_by_carnum - t3.speed_p_avg) / t4.speed_p_std))  >  2 
           then 'abnormal'
         else   'normal'
       end
       as speed_p_symptom_score,
       t1.break_p_avg_by_carnum,
       case
         when (abs((t1.break_p_avg_by_carnum - t3.break_p_avg) / t4.break_p_std))  >  2 
           then 'abnormal'
         else   'normal'
       end
       as break_p_symptom_score,
       t2.steer_a_count,
       case
         when (t2.steer_a_count)  >   2000
           then 'abnormal'
         else   'normal'
       end
       as steer_p_symptom_score,
       t1.biz_date
from 
       (select car_number, biz_date, avg(speed_pedal) as speed_p_avg_by_carnum, avg(break_pedal) as break_p_avg_by_carnum from managed_smartcar_drive_info where biz_date =  '${working_day}'  group by car_number, biz_date) t1
join 
       (select car_number, count(*) as steer_a_count from managed_smartcar_drive_info where steer_angle in ('L2','L3','R2','R3') and biz_date =  '${working_day}'  group by car_number) t2
on 
       t1.car_number = t2.car_number ,
       (select avg(speed_pedal) as speed_p_avg, avg(break_pedal) as break_p_avg from managed_smartcar_drive_info ) t3,
       (select stddev_pop(s.speed_p_avg_by_carnum) as speed_p_std, stddev_pop(s.break_p_avg_by_carnum) as break_p_std from 
                (select car_number, avg(speed_pedal) as speed_p_avg_by_carnum, avg(break_pedal) as break_p_avg_by_carnum from managed_smartcar_drive_info group by car_number) s) t4
                