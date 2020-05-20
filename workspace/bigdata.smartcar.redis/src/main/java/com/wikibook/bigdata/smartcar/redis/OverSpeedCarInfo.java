package com.wikibook.bigdata.smartcar.redis;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class OverSpeedCarInfo {
	
	public static void main(String[] args) {
		
		String key = "";
		
		if(args != null) key = args[0];
		
		
		ExecutorService exc = Executors.newFixedThreadPool(1); 

		exc.submit(new RedisClient( key));
		

		
	}

}
