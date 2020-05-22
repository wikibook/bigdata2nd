package com.wikibook.bigdata.smartcar.storm;


import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;






public class SmartCarDriverTopology {


	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException, AuthorizationException {  

		StormTopology topology = makeTopology();

		Map<String, String> HBaseConfig = Maps.newHashMap();
		HBaseConfig.put("hbase.rootdir","hdfs://server01.hadoop.com:8020/hbase");

		Config config = new Config();
		config.setDebug(true);
		config.put("HBASE_CONFIG",HBaseConfig);

		//config.put(Config.NIMBUS_HOST, "server02.hadoop.com");
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("server02.hadoop.com"));


		config.put("storm.zookeeper.session.timeout", 20000);
		config.put("storm.zookeeper.connection.timeout", 15000);
		config.put("storm.zookeeper.retry.times", 5);
		config.put("storm.zookeeper.retry.interval", 1000);

		StormSubmitter.submitTopology(args[0], config, topology);

	}  


	private static StormTopology makeTopology() {

		String zkHost = "server02.hadoop.com:2181";
		TopologyBuilder driverCarTopologyBuilder = new TopologyBuilder();
		
		BrokerHosts brkBost = new ZkHosts(zkHost);
		String topicName = "SmartCar-Topic";
		String zkPathName = "/SmartCar-Topic";

		SpoutConfig spoutConf = new SpoutConfig(brkBost, topicName, zkPathName, UUID.randomUUID().toString());
		
		
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.useStartOffsetTimeIfOffsetOutOfRange=true;
		spoutConf.startOffsetTime=kafka.api.OffsetRequest.LatestTime();
		
				 
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
		
		
		driverCarTopologyBuilder.setSpout("kafkaSpout", kafkaSpout, 1);

		// Grouping - SplitBolt & EsperBolt
		driverCarTopologyBuilder.setBolt("splitBolt", new SplitBolt(),1).allGrouping("kafkaSpout");
		driverCarTopologyBuilder.setBolt("esperBolt", new EsperBolt(),1).allGrouping("kafkaSpout");


		// HBase Bolt
		TupleTableConfig hTableConfig = new TupleTableConfig("DriverCarInfo", "r_key");
		hTableConfig.setZkQuorum("server02.hadoop.com");
		hTableConfig.setZkClientPort("2181");
		hTableConfig.setBatch(false);
		hTableConfig.addColumn("cf1", "date");
		hTableConfig.addColumn("cf1", "car_number");
		hTableConfig.addColumn("cf1", "speed_pedal");
		hTableConfig.addColumn("cf1", "break_pedal");
		hTableConfig.addColumn("cf1", "steer_angle");
		hTableConfig.addColumn("cf1", "direct_light");
		hTableConfig.addColumn("cf1", "speed");
		hTableConfig.addColumn("cf1", "area_number");

		HBaseBolt hbaseBolt = new HBaseBolt(hTableConfig);
		driverCarTopologyBuilder.setBolt("HBASE", hbaseBolt, 1).shuffleGrouping("splitBolt");


		// Redis Bolt
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder().setHost("server02.hadoop.com").setPort(6379).build();
		RedisBolt redisBolt = new RedisBolt(jedisPoolConfig);

		driverCarTopologyBuilder.setBolt("REDIS", redisBolt, 1).shuffleGrouping("esperBolt");

		return driverCarTopologyBuilder.createTopology();
	}

}  
