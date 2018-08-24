package cn.ac.iie.hy.datadispatch.dbutils;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
public class PublishRedisUtil {
    
    //private static String ADDR = "10.244.78.18";
	static String key = "publicRedis";
   
    private static int MAX_ACTIVE = 4096;
    private static int MAX_IDLE = 4096;
    private static int MAX_WAIT = 1000;
    private static int TIMEOUT = 100000;
    private static boolean TEST_ON_BORROW = true;
    private static JedisPool jedisPool = null;
  
    static{
        String configurationFileName = "data-dispatcher.properties";

    	Properties pps = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(configurationFileName));
			pps.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		

        String value = pps.getProperty(key);
    	List<JedisShardInfo> infoList = new ArrayList<JedisShardInfo>();
        String ip ="";
        int port =0;
		String[] hosts = value.split(" ");
		for (String hostPair : hosts) {
			ip = hostPair.split(":")[0];
			port = Integer.parseInt(hostPair.split(":")[1]);
			infoList.add(new JedisShardInfo(ip, port));
		}
		

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(MAX_IDLE);
        config.setTestOnBorrow(TEST_ON_BORROW);
        jedisPool = new JedisPool(config,ip,port,TIMEOUT);
    }
    
    
   
    public synchronized static Jedis getJedis(){
       if(jedisPool!=null){
           Jedis resource = jedisPool.getResource();
           return resource;
       }else{
           return null;
       }
    }
   
    public static void returnBrokenResource(Jedis jedis){
        if(jedis!=null){
            jedisPool.returnBrokenResource(jedis);
            
        }
    }
    
    public static void returnResource(Jedis jedis){
    	if(jedis!=null){
    		jedisPool.returnResource(jedis);
    	}
    }
    
}
