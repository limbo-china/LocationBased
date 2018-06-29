/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.nodeserver.dbutils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtilPro {
    
    //private static String ADDR = "10.224.78.7";
	static String confFilePath = "data-node.properties";
	static String key = "redisProServer";
	
    private static int PORT = 6379;
    private static int MAX_ACTIVE = 1024;
    private static int MAX_IDLE = 4096;
    private static int MAX_WAIT = 1000;
    private static int TIMEOUT = 100000;
    private static boolean TEST_ON_BORROW = true;
    private static JedisPool jedisPool = null;
  
    static{
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(MAX_IDLE);
        config.setTestOnBorrow(TEST_ON_BORROW);
        
        Properties pps = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(confFilePath));
			pps.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String value = pps.getProperty(key);
		
        jedisPool = new JedisPool(config,value,PORT,TIMEOUT);
        
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
