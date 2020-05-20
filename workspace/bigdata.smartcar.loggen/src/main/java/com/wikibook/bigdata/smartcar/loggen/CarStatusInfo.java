package com.wikibook.bigdata.smartcar.loggen;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CarStatusInfo {


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


	public String randomRange(int n1, int n2) {  
		return String.valueOf( (int)(Math.random() * (n2 - n1 + 1)) + n1);
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
	public String getTireFL() {

		Map<String, Double> w = new HashMap<String, Double>();
		w.put(randomRange(80,100), 80D);
		w.put(randomRange(70,79), 19.9998D);
		w.put(randomRange(0,69), 0.0002D);
		Random rand = new Random();
		
		String result = "";
		
		if(this.tireFL == null)
			result = getWeightedRandom(w, rand);
		else if(Integer.parseInt(this.tireFL) > 69)
			result = getWeightedRandom(w, rand);
		else
			result = this.tireFL;
		
		this.tireFL = result;

		return result;
	}
	public void setTireFL(String tireFL) {
		this.tireFL = tireFL;
	}
	public String getTireFR() {
		Map<String, Double> w = new HashMap<String, Double>();
		w.put(randomRange(80,100), 80D);
		w.put(randomRange(70,79), 19.9998D);
		w.put(randomRange(0,69), 0.0002D);
		Random rand = new Random();

		String result = "";
		
		if(this.tireFR == null)
			result = getWeightedRandom(w, rand);
		else if(Integer.parseInt(this.tireFR) > 69)
			result = getWeightedRandom(w, rand);
		else
			result = this.tireFR;
		
		this.tireFR = result;

		return result;
	}
	public void setTireFR(String tireFR) {
		this.tireFR = tireFR;
	}
	public String getTireBL() {
		Map<String, Double> w = new HashMap<String, Double>();
		w.put(randomRange(80,100), 80D);
		w.put(randomRange(70,79), 19.9998D);
		w.put(randomRange(0,69), 0.0002D);
		Random rand = new Random();

		String result = "";
		
		if(this.tireBL == null)
			result = getWeightedRandom(w, rand);
		else if(Integer.parseInt(this.tireBL) > 69)
			result = getWeightedRandom(w, rand);
		else
			result = this.tireBL;
		
		this.tireBL = result;

		return result;
	}
	public void setTireBL(String tireBL) {
		this.tireBL = tireBL;
	}
	public String getTireBR() {
		Map<String, Double> w = new HashMap<String, Double>();
		w.put(randomRange(80,100), 80D);
		w.put(randomRange(70,79), 19.9998D);
		w.put(randomRange(0,69), 0.0002D);
		Random rand = new Random();

		String result = "";
		
		if(this.tireBR == null)
			result = getWeightedRandom(w, rand);
		else if(Integer.parseInt(this.tireBR) > 69)
			result = getWeightedRandom(w, rand);
		else
			result = this.tireBR;
		
		this.tireBR = result;

		return result;
	}
	public void setTireBR(String tireBR) {
		this.tireBR = tireBR;
	}
	public String getLightFL() {
		Map<String, Double> w = new HashMap<String, Double>();
		w.put("1", 99.9998D);
		w.put("2", 0.0002D);
		Random rand = new Random();

		String result = "";
		
		if(this.lightFL == null)
			result = getWeightedRandom(w, rand);
		else if(Integer.parseInt(this.lightFL) == 1)
			result = getWeightedRandom(w, rand);
		else
			result = this.lightFL;
		
		this.lightFL = result;

		return result;
	}
	public void setLightFL(String lightFL) {
		this.lightFL = lightFL;
	}
	public String getLightFR() {
		Map<String, Double> w = new HashMap<String, Double>();
		w.put("1", 99.9998D);
		w.put("2", 0.0002D);
		Random rand = new Random();

		String result = "";
		
		if(this.lightFR == null)
			result = getWeightedRandom(w, rand);
		else if(Integer.parseInt(this.lightFR) == 1)
			result = getWeightedRandom(w, rand);
		else
			result = this.lightFR;
		
		this.lightFR = result;

		return result;
	}
	public void setLightFR(String lightFR) {
		this.lightFR = lightFR;
	}
	public String getLightBL() {
		Map<String, Double> w = new HashMap<String, Double>();
		w.put("1", 99.9998D);
		w.put("2", 0.0002D);
		Random rand = new Random();

		String result = "";
		
		if(this.lightBL == null)
			result = getWeightedRandom(w, rand);
		else if(Integer.parseInt(this.lightBL) == 1)
			result = getWeightedRandom(w, rand);
		else
			result = this.lightBL;
		
		this.lightBL = result;

		return result;
	}
	public void setLightBL(String lightBL) {
		this.lightBL = lightBL;
	}
	public String getLightBR() {
		Map<String, Double> w = new HashMap<String, Double>();
		w.put("1", 99.9998D);
		w.put("2", 0.0002D);
		Random rand = new Random();

		String result = "";
		
		if(this.lightBR== null)
			result = getWeightedRandom(w, rand);
		else if(Integer.parseInt(this.lightBR) == 1)
			result = getWeightedRandom(w, rand);
		else
			result = this.lightBR;
		
		this.lightBR = result;

		return result;
	}
	public void setLightBR(String lightBR) {
		this.lightBR = lightBR;
	}
	public String getEngineInfo() {

		Map<String, Double> w = new HashMap<String, Double>();
		w.put("A", 80D);
		w.put("B", 19.9998D);
		w.put("C", 0.0002D); 
		Random rand = new Random();

		String result = "";
		
		if(this.engineInfo== null)
			result = getWeightedRandom(w, rand);
		else if(!this.engineInfo.equals("C"))
			result = getWeightedRandom(w, rand);
		else
			result = this.engineInfo;
		
		this.engineInfo = result;

		return result;
	}
	public void setEngineInfo(String engineInfo) {
		this.engineInfo = engineInfo;
	}
	public String getBreakInfo() {
		Map<String, Double> w = new HashMap<String, Double>();
		w.put("A", 80D);
		w.put("B", 19.9998D);
		w.put("C", 0.0002D); 
		Random rand = new Random();

		String result = "";
		
		if(this.breakInfo== null)
			result = getWeightedRandom(w, rand);
		else if(!this.breakInfo.equals("C"))
			result = getWeightedRandom(w, rand);
		else
			result = this.breakInfo;
		
		this.breakInfo = result;

		return result;
	}
	public void setBreakInfo(String breakInfo) {
		this.breakInfo = breakInfo;
	}
	public String getBatteryInfo() {
		Map<String, Double> w = new HashMap<String, Double>();
		w.put(randomRange(80,100), 90D);
		w.put(randomRange(50,79), 9.9998D);
		w.put(randomRange(0,49), 0.0002D);
		Random rand = new Random();

		String result = "";
		
		if(this.batteryInfo== null)
			result = getWeightedRandom(w, rand);
		else if(Integer.parseInt(this.batteryInfo) > 49)
			result = getWeightedRandom(w, rand);
		else
			result = this.batteryInfo;
		
		this.batteryInfo = result;

		return result;
	}
	public void setBatteryInfo(String batteryInfo) {
		this.batteryInfo = batteryInfo;
	}
	
	

	String time;
	String carNum;
	String tireFL;
	String tireFR;
	String tireBL;
	String tireBR;
	String lightFL;
	String lightFR;
	String lightBL;
	String lightBR;
	String engineInfo;
	String breakInfo;
	String batteryInfo;

}
