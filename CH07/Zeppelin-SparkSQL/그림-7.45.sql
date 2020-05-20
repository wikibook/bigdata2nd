select T1.area_num, T1.avg_speed     
from  (select area_num, avg(speed) as avg_speed
       from DriveInfo
       group by area_num
       ) T1
order by T1.avg_speed desc
