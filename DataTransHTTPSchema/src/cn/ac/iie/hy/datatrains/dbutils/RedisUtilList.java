/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.datatrains.dbutils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.datatrains.handler.DataDispatchHandler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtilList {

	static String confFilePath = "data-dispatcher.properties";
	static String key = "redisListServer";
	
	private static int MAX_ACTIVE = 2048;
	private static int MAX_IDLE = 4096;
	private static int MAX_WAIT = 1000;
	private static int TIMEOUT = 100000;
	private static boolean TEST_ON_BORROW = false;
	private static boolean TEST_ON_RETURN = false;
	private static JedisPool jedisPool = null;
	
	static Logger logger = null;
	
	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDispatchHandler.class.getName());
	}

	static {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(MAX_ACTIVE);
		config.setMaxIdle(MAX_IDLE);
		config.setMaxWaitMillis(MAX_WAIT);
		config.setTestOnBorrow(TEST_ON_BORROW);
		config.setTestOnReturn(TEST_ON_RETURN);
		Properties pps = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(confFilePath));
			pps.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
//		String ip = "10.224.78.5";
//		int port = 6379;
		String value = pps.getProperty(key);
		String ip = value.split(":")[0];
		int port = Integer.parseInt(value.split(":")[1]);

		jedisPool = new JedisPool(config, ip, port, TIMEOUT);
		
	}

	public synchronized static Jedis getJedis() {
		if (jedisPool != null) {
			Jedis resource = jedisPool.getResource();
			return resource;
		} else {
			return null;
		}
	}

	public static void returnBrokenResource(Jedis jedis) {
		if (jedis != null) {
			jedisPool.returnBrokenResource(jedis);

		}
	}

	public static void returnResource(Jedis jedis) {
		if (jedis != null) {
			jedisPool.returnResource(jedis);
		}
	}

}
