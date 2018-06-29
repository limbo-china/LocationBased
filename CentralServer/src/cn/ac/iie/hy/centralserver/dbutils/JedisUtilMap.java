/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.centralserver.dbutils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cn.ac.iie.hy.centralserver.config.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
public class JedisUtilMap {
        
    private static int PORT = 6379;
    private static int MAX_ACTIVE = 4096;
    private static int MAX_IDLE = 4096;
    private static int MAX_WAIT = 1000;
    private static int TIMEOUT = 100000;
    private static boolean TEST_ON_BORROW = true;
    private static ShardedJedisPool jedisPool = null;
    static String key = "redisMapIP";
    static{
        String configurationFileName = "data-pro.properties";
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            System.out.println("reading " + configurationFileName + " is failed.");
            System.exit(-1);
        }

//        String ADDR = conf.getString("redisMapIP", "");
//        if (ADDR.isEmpty()) {
//            System.out.println("definition redisIP is not found in " + configurationFileName);
//            System.exit(-1);
//        }

//        JedisPoolConfig config = new JedisPoolConfig();
//        config.setMaxIdle(MAX_IDLE);
//        config.setTestOnBorrow(TEST_ON_BORROW);
//        jedisPool = new JedisPool(config,ADDR,PORT,TIMEOUT);
        
        
        
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(2048);
		poolConfig.setMaxIdle(4096);
		poolConfig.setMaxWaitMillis(20000);
		poolConfig.setTestOnBorrow(false);
		poolConfig.setTestOnReturn(false);

		Properties pps = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(configurationFileName));
			pps.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String value = pps.getProperty(key);// ip:port
		List<JedisShardInfo> infoList = new ArrayList<JedisShardInfo>();

		String[] hosts = value.split(" ");
		for (String hostPair : hosts) {
			String ip = hostPair.split(":")[0];
			int port = Integer.parseInt(hostPair.split(":")[1]);
			infoList.add(new JedisShardInfo(ip, port));
		}
		jedisPool = new ShardedJedisPool(poolConfig, infoList);
		
		
    }
    
    
	public synchronized static ShardedJedis getSource() {
		if (jedisPool != null) {
			return jedisPool.getResource();
		} else {
			return null;
		}
	}

	public static void returnBrokenResource(ShardedJedis jedis) {
		if (jedis != null) {
			jedisPool.returnBrokenResource(jedis);

		}
	}

	public static void returnResource(ShardedJedis jedis) {
		if (jedis != null) {
			jedisPool.returnResource(jedis);
		}
	}
    
}
