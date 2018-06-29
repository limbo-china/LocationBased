/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.centralserver.dbutils;

import cn.ac.iie.hy.centralserver.config.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
public class RedisUtil {
    
    //private static String ADDR = "10.244.78.18";
    
    private static int PORT = 6379;
    private static int MAX_ACTIVE = 4096;
    private static int MAX_IDLE = 4096;
    private static int MAX_WAIT = 1000;
    private static int TIMEOUT = 100000;
    private static boolean TEST_ON_BORROW = true;
    private static JedisPool jedisPool = null;
  
    static{
        String configurationFileName = "data-pro.properties";
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            System.out.println("reading " + configurationFileName + " is failed.");
            System.exit(-1);
        }

        String ADDR = conf.getString("redisIP", "");
        if (ADDR.isEmpty()) {
            System.out.println("definition redisIP is not found in " + configurationFileName);
            System.exit(-1);
        }

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(MAX_IDLE);
        config.setTestOnBorrow(TEST_ON_BORROW);
        jedisPool = new JedisPool(config,ADDR,PORT,TIMEOUT);
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
