insert overwrite local directory '/home/pilot-pjt/spark-data/classification/input'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
select 
  sex, age, marriage, region, job, car_capacity, car_year, car_model,
  tire_fl, tire_fr, tire_bl, tire_br, light_fl, light_fr, light_bl, light_br,
  engine, break, battery,
  case when ((tire_fl_s  + tire_fr_s  + tire_bl_s  + tire_br_s  + 
              light_fl_s + light_fr_s + light_bl_s + light_br_s + 
              engine_s   + break_s    + battery_s  + 
              car_capacity_s + car_year_s + car_model_s) < 6) 
       then '비정상' else '정상' 
  end as status
from (
  select 
    sex, age, marriage, region, job, car_capacity, car_year, car_model,
    tire_fl, tire_fr, tire_bl, tire_br, light_fl, light_fr, light_bl, light_br,
    engine, break, battery,

    case
	 when (1500 > cast(car_capacity as int)) then -0.3 
        when (2000 > cast(car_capacity as int)) then -0.2 
        else -0.1
    end as car_capacity_s ,

    case
	when (2005 > cast(car_year as int)) then -0.3 
       when (2010 > cast(car_year as int)) then -0.2 
       else -0.1
    end as car_year_s ,

    case
	when ('B' = car_model) then -0.3
       when ('D' = car_model) then -0.3 
       when ('F' = car_model) then -0.3 
       when ('H' = car_model) then -0.3 
       else 0.0
    end as car_model_s ,

    case 
       when (10 > cast(tire_fl as int)) then 0.1 
       when (20 > cast(tire_fl as int)) then 0.2 
       when (40 > cast(tire_fl as int)) then 0.4 
       else 0.5
    end as tire_fl_s ,

    case 
       when (10 > cast(tire_fr as int)) then 0.1 
       when (20 > cast(tire_fr as int)) then 0.2 
       when (40 > cast(tire_fr as int)) then 0.4 
       else 0.5
    end as tire_fr_s ,

    case 
       when (10 > cast(tire_bl as int)) then 0.1 
       when (20 > cast(tire_bl as int)) then 0.2 
       when (40 > cast(tire_bl as int)) then 0.4 
       else 0.5
    end as tire_bl_s ,

    case 
       when (10 > cast(tire_br as int)) then 0.1 
       when (20 > cast(tire_br as int)) then 0.2 
       when (40 > cast(tire_br as int)) then 0.4 
       else 0.5
    end as tire_br_s ,

    case when (cast(light_fl as int) = 2) then 0.0 else 0.5 end as light_fl_s ,
    case when (cast(light_fr as int) = 2) then 0.0 else 0.5 end as light_fr_s , 
    case when (cast(light_bl as int) = 2) then 0.0 else 0.5 end as light_bl_s ,
    case when (cast(light_br as int) = 2) then 0.0 else 0.5 end as light_br_s , 

    case 
       when (engine = 'A') then 1.0 
       when (engine = 'B') then 0.5 
       when (engine = 'C') then 0.0
    end as engine_s ,

    case 
       when (break = 'A') then 1.0 
       when (break = 'B') then 0.5 
       when (break = 'C') then 0.0
    end as break_s ,

    case 
       when (20 > cast(battery as int)) then 0.2 
       when (40 > cast(battery as int)) then 0.4 
       when (60 > cast(battery as int)) then 0.6 
       else 1.0
    end as battery_s 

  from managed_smartcar_status_info ) T1
    
