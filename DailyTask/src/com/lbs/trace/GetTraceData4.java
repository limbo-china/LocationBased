package com.lbs.trace;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class GetTraceData4 {

	
	private static String configfile = "ST2.properties";
    
	String number = null;
	static Logger logger = null;
	static {
		
			PropertyConfigurator.configure("log4j.properties");
				logger = Logger.getLogger(GetTraceData4.class.getName());
		try {
			Class.forName("com.oscar.Driver");
		} catch (java.lang.ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static String userName;
	private static String passWord;
	private static String dbName;
	private static String port;
	private static String host;
	private static String phonefilename;
	private static String timefilename;
	private static String sql;
	private static long start;
	private static int day;

	static {
		try {
			Properties pps = new Properties();

			InputStream in;
			in = new FileInputStream(configfile);
			pps.load(in);
			userName = pps.getProperty("username");
			passWord = pps.getProperty("passwd");
			port = pps.getProperty("port");
			dbName = pps.getProperty("dbname");
			host = pps.getProperty("host");			
			phonefilename = pps.getProperty("phonefilename");
			timefilename = pps.getProperty("timelistname");
			start = Long.valueOf(pps.getProperty("start"));
			day = Integer.valueOf(pps.getProperty("day"));
			//sql = pps.getProperty("sql");

			logger.info("username:"+userName);
			logger.info("password:"+passWord);
			logger.info("port:"+port);
			logger.info("dbname:"+dbName);
			logger.info("host:"+host);
			logger.info("filename:"+phonefilename);
			logger.info("start:"+start);
			logger.info("day:"+day);
			

			in.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void query(){
		

		try{
			FileInputStream inputStreamPhone = null;
			Scanner scPhone = null;
			inputStreamPhone = new FileInputStream(phonefilename);
			scPhone = new Scanner(inputStreamPhone, "UTF-8");
//			inputStreamTime = new FileInputStream(timefilename);
//			scTime = new Scanner(inputStreamTime, "UTF-8");
			
		    File file =new File("tianjinresult4-20" +".txt");
			FileWriter fileWritter = new FileWriter(file.getName(),true);
			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			
			File err =new File("errortmp" +".txt");
			FileWriter errfileWritter2 = new FileWriter(err.getName(),true);
			BufferedWriter errbufferWritter2 = new BufferedWriter(errfileWritter2);
			
			bufferWritter.write("IMSI"+","+"IMEI"+","+"手机号"+","+"uli"+","+"省份代码"+","+"时间"+"\n");					
			bufferWritter.flush();
		
			
			
			int count =0;
			String date ="";
			String hour ="";
			String msisdntmp="";
			
			Connection conn = null;
			ResultSet rs = null;
			Statement stmt = null;
			
			String url = "jdbc:oscarcluster://" + host + ":" + port + "/" + dbName;
			conn = DriverManager.getConnection(url, userName, passWord);
			stmt = conn.createStatement();
			while(scPhone.hasNext()){
				
				count++;
				String s = scPhone.nextLine();
				
				date = s.split(" ")[1];
				hour = s.split(" ")[2].split(":")[0];
				msisdntmp = s.split(" ")[3];
				
			try{
							
				 sql= "select * from T_LBS_TRACE_HISTORY where C_TIMESTAMP >=unix_timestamp('"+date+" "+hour+":00:00') "
				 		+ "and  C_TIMESTAMP < unix_timestamp('"+date+" "+ (Integer.parseInt(hour)+1)+":59:59') and C_MSISDN ='"+msisdntmp+"' order by C_TIMESTAMP asc;";	
				 
					//bufferWritter.write(sql);
					//sql = "show index from T_LBS_TRACE_HISTORY;" ;
					//System.out.println(sql);
					//logger.info("1");
					logger.info(count+": "+sql);
					rs = stmt.executeQuery(sql);
					while (rs.next()) {
						bufferWritter.write(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3)
						+","+rs.getString(4)+","+rs.getString(5)+","+stampToDate(Long.valueOf(rs.getString(6))) +"\n");
						break;
					}					
					bufferWritter.flush();
								
				}catch (Exception e) {
					//PrintStream
					logger.error(e.getMessage());
					//e.printStackTrace();
					errbufferWritter2.write(sql+"\n");
					errbufferWritter2.flush();
			    }
			
			logger.info("query  finished");
			if(rs!=null)
			{
				rs.close();
			}		
			}
			stmt.close();
			conn.close();
			bufferWritter.close();
		
			
			
			
		}catch (Exception e) {
			e.printStackTrace();
	    }
	}
	
	public void queryerr(){
		

		try{
			FileInputStream inputStreamErr = null;
			Scanner scErr = null;
			inputStreamErr = new FileInputStream("errortmp2.txt");
			scErr = new Scanner(inputStreamErr, "UTF-8");
			
			File file =new File("tianjinresult3" +".txt");
			FileWriter fileWritter = new FileWriter(file.getName(),true);
			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			
			File err =new File("errortmp3" +".txt");
			FileWriter errfileWritter2 = new FileWriter(err.getName(),true);
			BufferedWriter errbufferWritter2 = new BufferedWriter(errfileWritter2);
			
			int count =0;
			Connection conn = null;
			ResultSet rs = null;
			Statement stmt = null;
			
			String url = "jdbc:oscarcluster://" + host + ":" + port + "/" + dbName;
			conn = DriverManager.getConnection(url, userName, passWord);
			stmt = conn.createStatement();
			while(scErr.hasNext()){
				
				count++;
				String sql = scErr.nextLine();				
				
				try{
							
					logger.info(count+": "+sql);
					rs = stmt.executeQuery(sql);
					while (rs.next()) {
						bufferWritter.write(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3)
						+","+rs.getString(4)+","+rs.getString(5)+","+stampToDate(Long.valueOf(rs.getString(6))) +"\n");
						break;
					}					
					bufferWritter.flush();
								
				}catch (Exception e) {
					//PrintStream
					logger.error(e.getMessage());
					errbufferWritter2.write(sql+"\n");
					errbufferWritter2.flush();
					//e.printStackTrace();
			    }
			
				logger.info("query  finished");
				if(rs!=null)
				{
					rs.close();
				}		
			}
			stmt.close();
			conn.close();
			bufferWritter.close();
		}catch (Exception e) {
			e.printStackTrace();
	    }
	}
	
	public String stampToDate(long stamp){

		SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date(stamp);
		return sim.format(date.getTime()*1000);
	}
	public long dateToStamp(String s) throws ParseException{
		SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sim.parse(s);
		return date.getTime()/1000;
	}
}

