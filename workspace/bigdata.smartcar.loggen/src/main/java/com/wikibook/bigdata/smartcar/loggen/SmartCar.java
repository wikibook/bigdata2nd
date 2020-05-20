package com.wikibook.bigdata.smartcar.loggen;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SmartCar extends Thread{

	private String carNum;
	private String toDay;

	Logger logger = LogManager.getLogger(this.getName());

	CarStatusInfo cInfo = new CarStatusInfo();

	public SmartCar(String carNum, String toDay ) {
		this.carNum = carNum;
		this.toDay = toDay;
		cInfo.setCarNum(carNum);

		System.setProperty("logFilename", "./SmartCar/SmartCarStatusInfo_" + toDay + ".txt");

		org.apache.logging.log4j.core.LoggerContext ctx =
				(org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		ctx.reconfigure();

	}

	@Override    
	public void run() {

		int count = 86400;
		for(int i=0; i <= count ; i++) {
			if(i==0) {
				logger.info("SmartCar Status Information"
						+ ",CarNum"
						
						+ ",TireFL"
						+ ",TireFR"
						+ ",TireBL"
						+ ",TireBR"
						
						+ ",LightFL"
						+ ",LightFR"
						+ ",LightBL"
						+ ",LightBR"
						
						+ ",EngineInfo"
						+ ",BreakInfo"
						+ ",BatteryInfo");
			}
			logger.info(toDay + getSecToTime(i) 
					+ "," + cInfo.getCarNum() 
					
					+ "," + cInfo.getTireFL()
					+ "," + cInfo.getTireFR()
					+ "," + cInfo.getTireBL()
					+ "," + cInfo.getTireBR()
					
					+ "," + cInfo.getLightFL()
					+ "," + cInfo.getLightFR()
					+ "," + cInfo.getLightBL()
					+ "," + cInfo.getLightBR()
					
					+ ","+ cInfo.getEngineInfo()
					+ ","+ cInfo.getBreakInfo()
					+ ","+ cInfo.getBatteryInfo());
			i+=3;
		}

	}

	public String getSecToTime(int inSec) {

		String time = String.valueOf(inSec/3600);
		if(time.length() == 1) time = "0" + time;
		String min = String.valueOf(inSec%3600/60);
		if(min.length() == 1) min = "0" + min;
		String sec = String.valueOf(inSec%3600%60%60);
		if(sec.length() == 1) sec = "0" + sec;

		return time+min+sec;

	}
	
	
	public static String getToDate() {

		long todaytime;
		SimpleDateFormat day;
		String toDay;

		todaytime = System.currentTimeMillis(); 
		day = new SimpleDateFormat("yyyyMMdd");

		toDay =  day.format(new Date(todaytime));

		return toDay;

	}

}
