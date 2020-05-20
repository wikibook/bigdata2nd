package com.wikibook.bigdata.smartcar.loggen;

import java.text.DecimalFormat;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CarMasterIncomeMain {

	public static Logger logger = LogManager.getLogger("CarMasterIncome");

	public static void main(String[] args) {


		System.setProperty("logFilename", "./SmartCar/CarMaster2Income.txt");

		org.apache.logging.log4j.core.LoggerContext ctx =
				(org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		ctx.reconfigure();


		String prefixName = "";
		String postfixNum = "";
		String carNum = "";
		String sex = "";
		String carSize = "";

		for(int i=0; i <= 25 ; i++) {
			prefixName = getCarPrefixName(i);

			if(i==0) logger.info("carNum|sex|age|marriage|region|job|capacity|year|type|income");
			for(int j=1; j <= 100; j++) {
				postfixNum = getCarPostFixNum(j);

				carNum = prefixName  + postfixNum;

				sex = getSex();
				carSize = getCarSize();



				logger.info(carNum + "|" + sex + "|" + getAge() + "|" + getMarriage() + "|" + getRegion() + "|" + getJob() 
				+ "|" + carSize + "|" + getCarYear() + "|" + getCarType() + "|" + getIncome(carSize)); 

			}

		}

		System.out.println("########## CarMasterIncome LogGen is Finished ##########");

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

		String[] rData = {"회사원","공무원","개인사업","전문직","프리랜서","주부","학생"}; 
		String result = rData[randomRange(0, 6)] ;

		return result;

	}

	public static String getMarriage() {

		String[] rData = {"기혼", "미혼"}; 
		String result = rData[randomRange(0, 1)] ;

		return result;

	}

	public static String getCarSize() {

		Random rValue = new Random();




		int rg = 0;

		rg = (int)(Math.abs(Math.ceil(1.0 * rValue.nextGaussian() + 4.5)));
		if(rg < 0) rg = 0; else if(rg > 9) rg=9;



		String[] rData = {"1000","1500", "2000", "2500", "3000", "3500", "4000", "4500", "5000", "5500"}; 

		String result = rData[rg];

		return result;

	}


	public static int getIncome(String inCome) {

		int tInCome = Integer.parseInt(inCome);

		int rNum = randomRange(0, 4);
		

		tInCome += (tInCome * randomRange2(1, 3));
		
		if(rNum == 0) {
			tInCome += randomRange(100, 1000);
		}

		return tInCome;

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
	
	public static double randomRange2(int n1, int n2) {  
		double num = ((int)(Math.random() * n2 + n1) * 0.1);
		return num;
	} 

}
