package com.wikibook.bigdata.smartcar.storm;


import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.topology.TopologyBuilder;

import  static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;



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

		TopologyBuilder driverCarTopologyBuilder = new TopologyBuilder();
		
        String bootstrapServers = "server02.hadoop.com:9092";
        
        String topic = "SmartCar-Topic";
        
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setNumWorkers(1);
        
        KafkaSpoutRetryService kafkaSpoutRetryService =  new KafkaSpoutRetryExponentialBackoff(
            KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
            KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2),
            Integer.MAX_VALUE,
            KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
        
        KafkaSpoutConfig spoutConf =  KafkaSpoutConfig.builder(bootstrapServers, topic)
            .setGroupId("smartcar-group1")    
            .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")   
            .setProp(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "50000") 
            .setProp(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000") 
            .setOffsetCommitPeriodMs(10000)    
            .setFirstPollOffsetStrategy(LATEST)   
            .setRetry(kafkaSpoutRetryService)
            .build();
        
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
