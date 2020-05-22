package com.wikibook.bigdata.smartcar.storm;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;


public class EsperBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;

	private EPServiceProvider espService;

	private boolean isOverSpeedEvent = false;

	public void prepare(Map stormConf, TopologyContext context) {

		Configuration configuration = new Configuration();
		configuration.addEventType("DriverCarInfoBean", DriverCarInfoBean.class.getName());

		espService = EPServiceProviderManager.getDefaultProvider(configuration);
		espService.initialize();

		int avgOverSpeed = 80;
		int windowTime  = 30;

		String overSpeedEpl =  "SELECT date, carNumber, speedPedal, breakPedal, "
				+ "steerAngle, directLight, speed , areaNumber "
				+ " FROM DriverCarInfoBean.win:time_batch("+windowTime+" sec) "
				+ " GROUP BY carNumber HAVING AVG(speed) > " + avgOverSpeed;

		EPStatement driverCarinfoStmt = espService.getEPAdministrator().createEPL(overSpeedEpl);

		driverCarinfoStmt.addListener((UpdateListener) new OverSpeedEventListener());
	}



	public void execute(Tuple tuple, BasicOutputCollector collector) {

		String tValue = tuple.getString(0); 

		//발생일시(14자리), 차량번호, 가속페달, 브레이크페달, 운전대회적각, 방향지시등, 주행속도, 뮤직번호
		String[] receiveData = tValue.split("\\,");
		
		DriverCarInfoBean driverCarInfoBean =new DriverCarInfoBean();

		driverCarInfoBean.setDate(receiveData[0]);
		driverCarInfoBean.setCarNumber(receiveData[1]);
		driverCarInfoBean.setSpeedPedal(receiveData[2]);
		driverCarInfoBean.setBreakPedal(receiveData[3]);
		driverCarInfoBean.setSteerAngle(receiveData[4]);
		driverCarInfoBean.setDirectLight(receiveData[5]);
		driverCarInfoBean.setSpeed(Integer.parseInt(receiveData[6]));
		driverCarInfoBean.setAreaNumber(receiveData[7]);

		espService.getEPRuntime().sendEvent(driverCarInfoBean); 


		if(isOverSpeedEvent) {
			//발생일시(14자리), 차량번호
			collector.emit(new Values(	driverCarInfoBean.getDate().substring(0,8), 
					driverCarInfoBean.getCarNumber()+"-"+driverCarInfoBean.getDate()));
			isOverSpeedEvent = false;
		}
		

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("date", "car_number"));
	}


	private class OverSpeedEventListener implements UpdateListener
	{
		@Override
		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			if (newEvents != null) {
				try {
					isOverSpeedEvent = true;
				} catch (Exception e) {
					System.out.println("Failed to Listener Update" + e);
				} 
			}
		}
	}

}