select car_number, avg(battery) as battery_avg
from SmartCar_Status_Info
where battery < 60
group by car_number;