package com.w.limbo.test;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import redis.clients.jedis.Jedis;

public class Add2Redis {

	static Logger logger = null;

 	static {
 		PropertyConfigurator.configure("log4j.properties");
 		logger = Logger.getLogger(Add2Redis.class.getName());
 	}
 
	
	public static void main(String[] args) throws IOException {

		Jedis jedis = new Jedis("140.143.63.132",6392);
		String fileName = "uliredis.txt";

		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
		    inputStream = new FileInputStream(fileName);
		    sc = new Scanner(inputStream, "UTF-8");
		
			int count =0 ;
			while(sc.hasNext()){
				String line = sc.nextLine();
		    	String uli = line.split(",")[0];
		    	
		    	jedis.set(uli ,line);	    	
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
