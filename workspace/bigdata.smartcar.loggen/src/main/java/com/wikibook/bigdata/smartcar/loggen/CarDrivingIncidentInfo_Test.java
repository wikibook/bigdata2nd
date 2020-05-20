package com.wikibook.bigdata.smartcar.loggen;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CarDrivingIncidentInfo_Test {

	public static Logger logger = LogManager.getLogger("CarDrivingIncidentInfo_Test");

	public static void main(String[] args) {

		System.setProperty("logFilename", "./SmartCar/CarDrivingIncidentInfo_Test.csv");

		org.apache.logging.log4j.core.LoggerContext ctx =
				(org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		ctx.reconfigure();

		int accPoss = 0;
		int wea = 0;
		int temp = 0;
		int hum = 0;
		int arrOthCar =0;
		int time = 0;
		int acciArea = 0;
		int drvCond = 0;
		int innCond = 0;
		int carSpd = 0;
		int steeAng = 0;
		
		int conTot = 0;

		int count = 30;
		for(int j=0; j <= count; j++) {
			
			
			if(j==0) {
				logger.info(count+",10,wea,temp,hum,arrCar,timeSlot,acciArea,drvCond,innCond,carSpd,steeAng,acciPos"); 
				continue;
			}
			
			
			
			accPoss = 0;
			
			wea = getCarExtRand("2");
			temp = getCarExtRand("1");
			hum = getCarExtRand("1");
			
			
			arrOthCar =getCarExtRand("2");
			time = getCarExtRand("2");
			
			acciArea = getCarExtRand("3");
			drvCond = getCarExtRand("3");
			innCond = getCarExtRand("3");
			
			carSpd = getCarExtRand("3");
			steeAng = getCarExtRand("1");
			
			conTot = (wea + temp + hum + arrOthCar + time + acciArea + drvCond + innCond + carSpd + steeAng);
			
			if (acciArea == 2 || drvCond ==2 || innCond == 2 || carSpd ==2) {
				conTot += 5;
			}
			
			if(conTot > 15) accPoss = 2;
			else if(12 < conTot && conTot <= 15) accPoss = 1;
			else accPoss = 0;

			logger.info(wea + "," + temp + "," + hum + "," + arrOthCar + "," + time + "," + acciArea
					+ "," + drvCond + "," + innCond + "," + carSpd + "," + steeAng + "," + accPoss); 

		}

		System.out.println("########## CarDrivingIncidentInfo_Test LogGen is Finished ##########");

	}
	
	public static int getCarExtRand(String weight) {

		Map<Integer, Double> w = new HashMap<Integer, Double>();
		
		if(weight.equals("1")) {
			w.put(0, 60D);
			w.put(1, 35D);
			w.put(2, 5D);
		}else if(weight.equals("2")) {
			w.put(0, 50D);
			w.put(1, 40D);
			w.put(2, 10D);
		}else if(weight.equals("3")) {
			w.put(0, 30D);
			w.put(1, 30D);
			w.put(2, 40D);
		}

		Random rand = new Random();

		return getWeightedRandom(w, rand);
	}


	public static <E> E getWeightedRandom(Map<E, Double> weights, Random random) { 
		E result = null;  
		double bestValue = Double.MAX_VALUE;    
		for (E element : weights.keySet()) {     
			double value = -Math.log(random.nextDouble()) / weights.get(element);     
			if (value < bestValue) {      
				bestValue = value;       result = element; 
			}  
		}   return result; 
	}

}
