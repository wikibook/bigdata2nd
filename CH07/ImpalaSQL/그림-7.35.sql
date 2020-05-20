select
   T2.area_number, T2.car_number, T2.speed_avg
from (
       select 
              T1.area_number, 
              T1.car_number, 
              T1.speed_avg,
              rank() over(partition by T1.area_number order by T1.speed_avg desc) as ranking
       from ( 
              select area_number, car_number, avg(cast(speed as int)) as speed_avg
              from  managed_smartcar_drive_info
              group by area_number, car_number
            ) T1
) T2
where ranking = 1