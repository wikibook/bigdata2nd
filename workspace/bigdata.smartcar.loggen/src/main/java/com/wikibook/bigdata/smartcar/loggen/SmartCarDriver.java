package com.wikibook.bigdata.smartcar.loggen;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SmartCarDriver extends Thread{

	private String carNum;
	private String toDay;

	Logger logger = LogManager.getLogger(this.getName());

	CarDrivingInfo cInfo = new CarDrivingInfo();

	public SmartCarDriver(String carNum, String toDay, boolean isAdd) {
		this.carNum = carNum;
		this.toDay = toDay;
		cInfo.setCarNum(carNum);
		cInfo.setIsAdd(isAdd);
		cInfo.setAreaTypeNum(randomRange(0,5));

		System.setProperty("logFilename", "./driver-realtime-log/SmartCarDriverInfo.log");

		org.apache.logging.log4j.core.LoggerContext ctx =
				(org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		ctx.reconfigure();

	}
	

	@Override    
	public void run() {

		int count = 86400;
		for(int i=0; i <= count ; i++) {
			if(i==0) {
				logger.info("Driver Status Infomation"
						+ ",CarNum"
						
						+ ",AccStep"
						+ ",BrkStep"
						+ ",WheelStep"
						
						+ ",DirLightStep"
						+ ",Speed"
						+ ",AreaNum");
				
			}
			logger.info(toDay + getSecToTime(i) 
					+ "," + cInfo.getCarNum() 
					
					+ "," + cInfo.getAccStep()
					+ "," + cInfo.getBrkStep()
					+ "," + cInfo.getWheelStep()
					
					+ "," + cInfo.getDirLightStep()
					+ "," + cInfo.getSpeed() 
					+ "," + cInfo.getAreaNum());
			try {
				sleep(1 * 100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			i+=1;
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
	
	public int randomRange(int n1, int n2) {  
		return  (int)(Math.random() * (n2 - n1 + 1)) + n1;
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
