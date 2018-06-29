package com.lbs.rtp;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.mysql.jdbc.Connection;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class AddMsisdn2TokenRedis {

	static Logger logger = null;

 	static {
 		PropertyConfigurator.configure("log4j.properties");
 		logger = Logger.getLogger(AddMsisdn2TokenRedis.class.getName());
 	}
 
	
	public static void main(String[] args) throws IOException {

		
		
		//将要查询的手机号码添加到token-msisdn中获取查询权限
		Jedis jedis1 = RedisUtil.getJedis1();
		//Jedis jedis2 = RedisUtil.getJedis2();
		String fileName = "msisdn.txt";

		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
		    inputStream = new FileInputStream(fileName);
		    sc = new Scanner(inputStream, "UTF-8");


//			
			int count =0 ;
			while(sc.hasNext()){
		    	String msisdn = sc.nextLine();
		    	
		    	if(jedis1.hexists("83422b2817ed64dd6d4c8f3264eeaa77",msisdn))
		    	{
		    		System.out.println(msisdn);
		    	}
		    	jedis1.hset("83422b2817ed64dd6d4c8f3264eeaa77" , msisdn,msisdn);		    	
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
