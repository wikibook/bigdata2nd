package com.wikibook.bigdata.smartcar.loggen;

import java.text.DecimalFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CarMasterMain {

	public static Logger logger = LogManager.getLogger("CarMaster");

	public static void main(String[] args) {

		System.setProperty("logFilename", "./SmartCar/CarMaster.txt");

		org.apache.logging.log4j.core.LoggerContext ctx =
				(org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		ctx.reconfigure();


		String prefixName = "";
		String postfixNum = "";
		String carNum = "";
		String sex = "";
		for(int i=0; i <= 25 ; i++) {
			prefixName = getCarPrefixName(i);
			for(int j=1; j <= 100; j++) {
				postfixNum = getCarPostFixNum(j);

				carNum = prefixName  + postfixNum;

				sex = getSex();

				logger.info(carNum + "|" + sex + "|" + getAge() + "|" + getMarriage() + "|" + getRegion() + "|" + getJob() 
							+ "|" + getCarSize() + "|" + getCarYear() + "|" + getCarType()); 

			}

		}

		System.out.println("########## CarMaster LogGen is Finished ##########");

	}


	public static String getSex() {

		String[] rData = {"남", "여"}; 
		String result = rData[randomRange(0, 1)] ;

		return result;

	}

	public static String getAge() {

		String result = String.valueOf(randomRange(15, 70));

		return result;

	}

	public static String getRegion() {

		String[] rData = {"경기","서울","강원","인천","충북","충남","대전","경북","경남","대구","전북","전남","울산","부산","광주","제주","세종"}; 
		String result = rData[randomRange(0, 16)] ;

		return result;

	}

	public static String getJob() {

		String[] rData = {"무직","회사원","자영업","공무원","학생"}; 
		String result = rData[randomRange(0, 4)] ;

		return result;

	}
	
	public static String getMarriage() {

		String[] rData = {"기혼", "미혼"}; 
		String result = rData[randomRange(0, 1)] ;

		return result;

	}
	
	public static String getCarSize() {

		String[] rData = {"1000", "1200" ,"1500", "1700", "2000", "2500", "3000", "3500"}; 
		String result = rData[randomRange(0, 7)] ;

		return result;

	}
	
	
public static String getCarYear() {

		
		String result = String.valueOf(randomRange(2000, 2016));

		return result;

	}
	
	public static String getCarType() {
		
		String[] rData = {"A", "B" ,"C", "D", "E", "F", "G", "H", "I", "J"}; 
		String result = rData[randomRange(0, 7)] ;

		return result;
	}



	public static String getCarPrefixName(int num) {

		String[] carNumPrefix = {"A", "B" , "C" , "D" , "E" , "F", "G", "H", "I", "J", "K", "L", "M", "N"
				, "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}; 

		String prefixName = carNumPrefix[num] ;


		return prefixName;
	}

	public static String getCarPostFixNum(int num) {

		DecimalFormat format = new DecimalFormat("0000");

		String carNum = format.format(num);

		return carNum;

	}


	public static int randomRange(int n1, int n2) {  
		return (int)((Math.random() * (n2 - n1 + 1)) + n1);
	} 

}
