SELECT
    car_number,
    cast(speed_p_avg as int),
    speed_p_symptom,
    cast(break_p_avg as float),
    break_p_symptom,
    cast(steer_a_cnt as int),
    steer_p_symptom,
    biz_date
FROM managed_smartcar_symptom_info
where biz_date = '20200322'
