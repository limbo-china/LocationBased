package com.lbs.rtp;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
public class RedisUtil {
    
    private static String ADDR1 = "140.143.63.132";//token-msidn
    private static String ADDR2 = "10.224.78.7";
    private static int PORT = 6388;
    private static int MAX_ACTIVE = 4096;
    private static int MAX_IDLE = 4096;
    private static int MAX_WAIT = 1000;
    private static int TIMEOUT = 100000;
    private static boolean TEST_ON_BORROW = true;
    private static JedisPool jedisPool1 = null;
    private static JedisPool jedisPool2 = null;
  
    static{
        
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(MAX_IDLE);
        config.setTestOnBorrow(TEST_ON_BORROW);
        jedisPool1 = new JedisPool(config,ADDR1,PORT,TIMEOUT);
        jedisPool2 = new JedisPool(config,ADDR2,PORT,TIMEOUT);
    }
    
    
   
    public synchronized static Jedis getJedis1(){
       if(jedisPool1!=null){
           Jedis resource = jedisPool1.getResource();
           return resource;
       }else{
           return null;
       }
    }
    public synchronized static Jedis getJedis2(){
        if(jedisPool2!=null){
            Jedis resource = jedisPool2.getResource();
            return resource;
        }else{
            return null;
        }
     }
   
    public static void returnBrokenResource1(Jedis jedis){
        if(jedis!=null){
            jedisPool1.returnBrokenResource(jedis);
            
        }
    }
    
    public static void returnResource1(Jedis jedis){
    	if(jedis!=null){
    		jedisPool1.returnResource(jedis);
    	}
    }
    
}
