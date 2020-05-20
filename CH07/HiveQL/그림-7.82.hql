insert overwrite local directory '/home/pilot-pjt/mahout-data/clustering/input'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
select 
  car_number,
  case 
      when (car_capacity < 2000) then '소형'
      when (car_capacity < 3000) then '중형'
      when (car_capacity < 4000) then '대형'
  end as car_capacity,
  case
      when ((2016-car_year) <= 4)  then 'NEW' 
      when ((2016-car_year) <= 8)  then 'NORMAL' 
      else 'OLD'
  end as car_year ,
  car_model,
  sex as owner_sex,
  floor (cast(age as int) * 0.1 ) * 10 as owner_age,
  marriage as owner_marriage,
  job as owner_job,
  region as owner_region
from smartcar_master 