package com.wikibook.bigdata.smartcar.storm;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public  class RedisBolt extends AbstractRedisBolt {

	private static final long serialVersionUID = 1L;

	public RedisBolt(JedisPoolConfig config) {
		super(config);
	}

	public RedisBolt(JedisClusterConfig  config) {
		super(config);
	}


	public void execute(Tuple input) {
		
		String date = input.getStringByField("date");
		String car_number = input.getStringByField("car_number");
		
		
		JedisCommands jedisCommands = null;

		try {

			jedisCommands = getInstance();
			jedisCommands.sadd(date, car_number);
			
			jedisCommands.expire(date, 604800);

		} catch (JedisConnectionException e) {
			throw new RuntimeException("Exception occurred to JedisConnection", e);
		} catch (JedisException e) {
			System.out.println("Exception occurred from Jedis/Redis" + e);
		} finally {

			if (jedisCommands != null) {
				returnInstance(jedisCommands);
			}
			this.collector.ack(input);
		}

		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	protected void process(Tuple input) {
		
		
	}


}