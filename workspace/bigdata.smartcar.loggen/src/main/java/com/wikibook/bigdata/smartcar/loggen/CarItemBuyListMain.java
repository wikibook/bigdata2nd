package com.wikibook.bigdata.smartcar.loggen;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CarItemBuyListMain {

	public static Logger logger = LogManager.getLogger("CarItem");

	public static void main(String[] args) {
		
		String buyMonth = "";
		
		if(args != null && args.length > 0) buyMonth = args[0];
		else buyMonth = getToMoth();

		System.setProperty("logFilename", "./SmartCar/CarItemBuyList_"+buyMonth+".txt");

		org.apache.logging.log4j.core.LoggerContext ctx =
				(org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		ctx.reconfigure();


		String carNum = "";
		String itemId = "";
		int score = 0;
		

		int itemList = 100000;

		for(int i=1; i <= itemList ; i++) {
			carNum = getCarNum(randomRange(1, 100));
			itemId = getItem();
			score = randomRange(1, 5);
			
			
			logger.info(carNum + "," + itemId + "," + score + "," + buyMonth); 
		}

		System.out.println("########## CarItemBuyList LogGen is Finished ##########");

	}


	public static String getCarNum(int num) {

		String[] carNumPrefix = {"A", "B" , "C" , "D" , "E" , "F", "G", "H", "I", "J", "K", "L", "M", "N"
				, "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}; 

		String prefixNum = carNumPrefix[randomRange(0, 25)] ;

		DecimalFormat format = new DecimalFormat("0000");

		String carNum = format.format(num);
		return prefixNum + carNum;
	}



	public static String getItem() {

		String[] rData = {	"Item-001","Item-002","Item-003","Item-004","Item-005","Item-006","Item-007","Item-008","Item-009",
							"Item-010","Item-011","Item-012","Item-013","Item-014","Item-015","Item-016","Item-017","Item-018",
							"Item-019","Item-020","Item-021","Item-022","Item-023","Item-024","Item-025","Item-026","Item-027", 
							"Item-028","Item-029","Item-030"}; 
		String result = rData[randomRange(0, 29)] ;

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
	
	public static String getToMoth() {

		long todaytime;
		SimpleDateFormat day;
		String toDay;

		todaytime = System.currentTimeMillis(); 
		day = new SimpleDateFormat("yyyyMM");

		toDay =  day.format(new Date(todaytime));

		return toDay;

	}

}
