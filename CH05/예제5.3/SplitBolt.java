package com.wikibook.bigdata.smartcar.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class SplitBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;

	public void execute(Tuple tuple, BasicOutputCollector collector) {

		String tValue = tuple.getString(0);  

		String[] receiveData = tValue.split("\\,");
		//발생일시(14자리), 차량번호, 가속페달, 브레이크페달, 운전대회적각, 방향지시등, 주행속도, 뮤직번호
		
		collector.emit(new Values(	new StringBuffer(receiveData[0]).reverse() + "-" + receiveData[1]  , 
									receiveData[0], receiveData[1], receiveData[2], receiveData[3],
									receiveData[4], receiveData[5], receiveData[6], receiveData[7]));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("r_key"			, "date"		, "car_number", 
									"speed_pedal"	, "break_pedal"	, "steer_angle", 
									"direct_light"	, "speed"		, "area_number"));
	}

}