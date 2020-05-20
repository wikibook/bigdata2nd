create table if not exists Managed_SmartCar_Symptom_Info (
car_number string,
speed_p_avg string,
speed_p_symptom string,
break_p_avg string,
break_p_symptom string,
steer_a_cnt string,
steer_p_symptom string,
biz_date string
)
row format delimited
fields terminated by ','
stored as textfile;
