package com.lbs.changeuli;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class ReadWriteFile2 {

	static Logger logger = null;
	
	private static String inputfileName = "jingneitest.txt";
	private static String inputfileName2 = "11uli.txt";//uli��  beijing uli split by '\t' others by ','!!!!
	private static HashMap<String, String> uliMap = null;
	
	
	
	private static HashMap<String, String> codeMap = new HashMap<String, String>(){

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			put("11", "����");
			put("12","���");
			put("13", "�ӱ�");
			put("14", "ɽ��");
			put("15", "���ɹ�");
			put("21", "����");
			put("22", "����");
			put("23", "������");
			put("31", "�Ϻ�");
			put("32", "����");
			put("33", "�㽭");
			put("34", "����");
			put("35", "����");
			put("36", "����");
			put("37", "ɽ��");
			put("41", "����");
			put("42", "����");
			put("43", "����");
			put("44", "�㶫");
			put("45", "����");
			put("46", "����");
			put("50", "����");
			put("51", "�Ĵ�");
			put("52", "����");
			put("53", "����");
			put("54", "����");
			put("61", "����");
			put("62", "����");
			put("63", "�ຣ");
			put("64", "����");
			put("65", "�½�");
		}
	};
	
	static {
		
		PropertyConfigurator.configure("log4j.properties");
				logger = Logger.getLogger(ReadWriteFile2.class.getName());
		try {
			Class.forName("com.oscar.Driver");
		} catch (java.lang.ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	static {
		uliMap = new HashMap<String, String>();

		FileInputStream inputStream2;
		try {
			inputStream2 = new FileInputStream(inputfileName2);
			Scanner sc2 = new Scanner(inputStream2, "UTF-8");
			
			String s2 = null;
			String uli2 =null;
			while(sc2.hasNext()){
		    	s2 =sc2.nextLine();
		    	String[] tmp = s2.split("\t");
		    	uli2 = tmp[0];
		    	if(tmp.length == 5 )
		    		uliMap.put(uli2, tmp[1]+","+tmp[2]+","+tmp[3]+","+tmp[4]);
		    	else if(tmp.length == 4)
		    		uliMap.put(uli2, tmp[1]+","+tmp[2]+","+tmp[3]);
		    }
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	     
	}
	
	
	public void readwrite(){
		
		try{
			FileInputStream inputStream = null;
			Scanner sc = null;
			
			
			inputStream = new FileInputStream(inputfileName);
		    sc = new Scanner(inputStream, "UTF-8");
		    	    
		    File file = new File("jingneires" +".txt"); ;
			FileWriter fileWritter = new FileWriter(file.getName(),true);;
			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			
			String s = null;
			String uli =null;
			//String prov_code = null;
			//String prov_name = null;
			int count =0;
			while(sc.hasNext()){
				
				s = sc.nextLine();
				//prov_code = s.split(",")[4].substring(0,2);
				//prov_name = codeMap.get(prov_code);
				uli = s.split(",")[3];
				try{
					bufferWritter.write(s.split(",")[0]+","+s.split(",")[1]+","+s.split(",")[2]
				    				+","+s.split(",")[3]+","+s.split(",")[5]);										
				    //if(prov_code.equals("11")){
				    String detail = uliMap.get(uli);
				    if(detail != null)
				    	bufferWritter.write(","+detail);
				    //}
				    bufferWritter.write("\n");
				    bufferWritter.flush();
				    count++;
					if(count % 1000 == 0)
						logger.info("count: "+count);	

				}catch (Exception e) {
					e.printStackTrace();
			    }						
			}
			logger.info("finished");
			bufferWritter.close();
		}catch (Exception e) {
			e.printStackTrace();
	    }
	}
	
	public void readCountLine(){
		
		try{
			FileInputStream inputStream = null;
			Scanner sc = null;
			
			inputStream = new FileInputStream("18-3-13\\2018-01-24 .txt");
		    sc = new Scanner(inputStream, "UTF-8");
		    
		    int count =0;
		    String s = null;
			while(sc.hasNext()){
				s = sc.nextLine();
				if(s.startsWith("861"))
					count ++;
				
				if(count % 10000000 == 0)
					System.out.println(count);
			}
			System.out.println("finished:"+count);
		}catch (Exception e) {
			e.printStackTrace();
	    }
	}
	public void readDelete(){
		
		try{
			FileInputStream inputStream = null;
			Scanner sc = null;
			
			File file = null;
			FileWriter fileWritter = null;
			BufferedWriter bufferWritter = null;
			
			for(long stamp = 1514736000; stamp <= 1520438400;stamp=stamp+86400)
			{
				String date = stampToDate(stamp).substring(0,10);
				inputStream = new FileInputStream("18-3-13\\"+ date + ".txt");
			    sc = new Scanner(inputStream, "UTF-8");
			    
			    file = new File(date.substring(0,10) +".txt");
			    fileWritter =new FileWriter(file.getName(),true);
			    bufferWritter = new BufferedWriter(fileWritter);
			    
			    logger.info("deal "+date);
			    String s = null;
				while(sc.hasNext()){
					s = sc.nextLine();
					if(isDigital(s))
						bufferWritter.write(s+"\n");
				}
				
				bufferWritter.flush();
				bufferWritter.close();
			}
			logger.info("finished");
		}catch (Exception e) {
			e.printStackTrace();
	    }
	}
	public static boolean isDigital(String str) throws PatternSyntaxException {  
			String regExp = "^[0-9]+$";  
			Pattern p = Pattern.compile(regExp);  
			Matcher m = p.matcher(str);  
			return m.matches();  
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
