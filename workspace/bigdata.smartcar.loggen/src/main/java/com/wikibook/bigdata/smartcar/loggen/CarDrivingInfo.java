package com.wikibook.bigdata.smartcar.loggen;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CarDrivingInfo {


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


	public int randomRange(int n1, int n2) {  
		return  (int)(Math.random() * (n2 - n1 + 1)) + n1;
	} 


	public String getTime() {

		return time;
	}



	public void setTime(String time) {
		this.time = time;
	}
	public String getCarNum() {
		return carNum;
	}
	public void setCarNum(String carNum) {
		this.carNum = carNum;
	}


	public String getAccStep() {


		Map<String, Double> w = new HashMap<String, Double>();

		if(!isAdd) {
			w.put("0", 30D);
			w.put("1", 30D);
			w.put("2", 10D);
			w.put("3", 20D);
			w.put("4", 5D);
			w.put("5", 5D);
		}else {
			w.put("0", 10D);
			w.put("1", 10D);
			w.put("2", 10D);
			w.put("3", 30D);
			w.put("4", 25D);
			w.put("5", 15D);
		}
		Random rand = new Random();

		String result = "";
		result = getWeightedRandom(w, rand);

		if(!isAdd) {
			if(result.equals("0")) this.speed += 0;
			if(result.equals("1")) this.speed += 5;
			if(result.equals("2")) this.speed += 10;
			if(result.equals("3")) this.speed += 15;
			if(result.equals("4")) this.speed += 20;
			if(result.equals("5")) this.speed += 25;
		}else {
			if(result.equals("0")) this.speed += 0;
			if(result.equals("1")) this.speed += 5;
			if(result.equals("2")) this.speed += 10;
			if(result.equals("3")) this.speed += 20;
			if(result.equals("4")) this.speed += 30;
			if(result.equals("5")) this.speed += 40;
		}

		if(this.speed > 250) this.speed = 250;
		
		String areaNum = getAreaNum(); //정체 지역 설정
		if(areaNum.equals("A10") || areaNum.equals("B02") || areaNum.equals("C04") || areaNum.equals("D05") || areaNum.equals("E10") || areaNum.equals("F06") ) {
			double dSpeed;
			dSpeed= (double)speed - ((double)speed * 0.7);
			this.speed = (int)dSpeed;
		}
		
		

		this.accStep = result;

		return result;
	}


	public void setAccStep(String accStep) {
		this.accStep = accStep;
	}


	public String getBrkStep() {

		if(this.accStep.equals("0")) {
			Map<String, Double> w = new HashMap<String, Double>();

			if(!isAdd) {
				w.put("0", 30D);
				w.put("1", 45D);
				w.put("2", 20D);
				w.put("3", 5D);
			}else {
				w.put("0", 10D);
				w.put("1", 10D);
				w.put("2", 50D);
				w.put("3", 30D);
			}

			Random rand = new Random();

			String result = "";
			result = getWeightedRandom(w, rand);


			if(!isAdd) {
				if(result.equals("0")) this.speed -= 0;
				if(result.equals("1")) this.speed -= 10;
				if(result.equals("2")) this.speed -= 20;
				if(result.equals("3")) this.speed -= 30;
			}else {
				if(result.equals("0")) this.speed -= 0;
				if(result.equals("1")) this.speed -= 5;
				if(result.equals("2")) this.speed -= 10;
				if(result.equals("3")) this.speed -= 20;
			}

			if(this.speed < 0) this.speed = 0;

			this.brkStep = result;
			return result;
		}else {
			return this.brkStep = "0";
		}


	}


	public void setBrkStep(String brkStep) {
		this.brkStep = brkStep;
	}


	public String getWheelStep() {
		Map<String, Double> w = new HashMap<String, Double>();

		if(!isAdd) {
			w.put("F", 70D);
			w.put("L1", 5D);
			w.put("L2", 5D);
			w.put("L3", 5D);
			w.put("R1", 5D);
			w.put("R2", 5D);
			w.put("R3", 5D);
		}else {
			w.put("F", 40D);
			w.put("L1", 10D);
			w.put("L2", 10D);
			w.put("L3", 10D);
			w.put("R1", 10D);
			w.put("R2", 10D);
			w.put("R3", 10D);
		}

		Random rand = new Random();

		String result = "";
		result = getWeightedRandom(w, rand);

		String dirPrefix = result.substring(0,1);

		if(dirPrefix.equals("F")) this.dirLightStep = "N";
		else if(dirPrefix.equals("L")) this.dirLightStep = "L";
		else if(dirPrefix.equals("R")) this.dirLightStep = "R";


		this.wheelStep = result;

		return result;
	}


	public void setWheelStep(String wheelStep) {
		this.wheelStep = wheelStep;
	}


	public String getDirLightStep() {

		Map<String, Double> w = new HashMap<String, Double>();

		if(!isAdd) {

			return this.dirLightStep;

		}else {

			w.put("N", 60D);
			w.put("L", 20D);
			w.put("R", 20D);

			Random rand = new Random();

			String result = "";
			result = getWeightedRandom(w, rand);

			this.dirLightStep = result;

			return result;
		}


	}


	public String getAreaNum() {
		
		String areaNum = "";

		String[] areaType1 = {	"A01", "A02" , "A03" , "A04" , "A05" , "A06", "A07", "A08", "A09", "A10" };
		String[] areaType2 = {	"B01", "B02" , "B03" , "B04" , "B05" , "B06", "B07", "B08", "B09", "B10" };
		String[] areaType3 = {	"C01", "C02" , "C03" , "C04" , "C05" , "C06", "C07", "C08", "C09", "C10" }; 
		String[] areaType4 = {	"D01", "D02" , "D03" , "D04" , "D05" , "D06", "D07", "D08", "D09", "D10" };
		String[] areaType5 = {	"E01", "E02" , "E03" , "E04" , "E05" , "E06", "E07", "E08", "E09", "E10" }; 
		String[] areaType6 = {	"F01", "F02" , "F03" , "F04" , "F05" , "F06", "F07", "F08", "F09", "F10" }; 
		
		if(areaTypeNum == 0) {
			areaNum = areaType1[randomRange(0, 9)] ;
		}else if (areaTypeNum == 1) {
			areaNum = areaType2[randomRange(0, 9)] ;
		}else if (areaTypeNum == 2) {
			areaNum = areaType3[randomRange(0, 9)] ;
		}else if (areaTypeNum == 3) {
			areaNum = areaType4[randomRange(0, 9)] ;
		}else if (areaTypeNum == 4) {
			areaNum = areaType5[randomRange(0, 9)] ;
		}else if (areaTypeNum == 5) {
			areaNum = areaType6[randomRange(0, 9)] ;
		}

		return areaNum;


	}


	public void setDirLightStep(String dirLightStep) {
		this.dirLightStep = dirLightStep;
	}

	public int getSpeed() {
		return speed;
	}


	public void setSpeed(int speed) {
		this.speed = speed;
	}


	public void setAreaNum(String areaNum) {
		this.areaNum = areaNum;
	}


	public boolean getIsAdd() {
		return isAdd;
	}


	public void setIsAdd(boolean isAdd) {
		this.isAdd = isAdd;
	}
	
	public void setAreaTypeNum(int areaTypeNum) {
		this.areaTypeNum = areaTypeNum;
	}
	
	
	public int getAreaTypeNum() {
		return areaTypeNum;
	}
	

	

	String time;
	String carNum;
	String accStep;
	String brkStep;
	String wheelStep;
	String dirLightStep;
	String areaNum;
	int areaTypeNum;



	

	int speed = 0;

	boolean isAdd = false;

}
