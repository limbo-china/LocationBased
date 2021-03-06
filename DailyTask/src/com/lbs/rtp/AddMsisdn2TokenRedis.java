package com.lbs.rtp;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import redis.clients.jedis.Jedis;

public class AddMsisdn2TokenRedis {

	static Logger logger = null;

 	static {
 		PropertyConfigurator.configure("log4j.properties");
 		logger = Logger.getLogger(AddMsisdn2TokenRedis.class.getName());
 	}
 
	
	public static void main(String[] args) throws IOException {

		Jedis jedis1 = RedisUtil.getJedis1();
		String fileName = "msisdn.txt";

		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
		    inputStream = new FileInputStream(fileName);
		    sc = new Scanner(inputStream, "UTF-8");
		
			int count =0 ;
			while(sc.hasNext()){
		    	String msisdn = sc.nextLine().split(";")[0];
		    	
		    	jedis1.hset("msisdn" ,msisdn,msisdn);		    	
    			count ++;
			}
			logger.info("count:" +count);
			logger.info("load finished.");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		    if (inputStream != null) {
		        inputStream.close();
		    }
		    if (sc != null) {
		        sc.close();
		    }
		}
		
	}

}
