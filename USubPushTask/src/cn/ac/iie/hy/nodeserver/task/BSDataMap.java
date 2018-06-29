package cn.ac.iie.hy.nodeserver.task;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class BSDataMap {

	private static HashMap<String, String> indexMap = new HashMap<String, String>();
	
	static public void loadIndex(String fileName){
		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
			inputStream = new FileInputStream(fileName);
			sc = new Scanner(inputStream, "UTF-8");
			int count = 0;
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				if(line.split(",").length == 13){
					String uli = line.split(",")[12];
					String value = line.split(",")[5] + "," + line.split(",")[6] + "," + line.split(",")[10] + 
							"," + line.split(",")[7] + "," + line.split(",")[8] + "," + line.split(",")[9]  + "," + line.split(",")[3]+",";
					indexMap.put(uli, value);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	static public String getLngi(String uli){
		String v = indexMap.get(uli);
		if(v == null){
			System.out.println("0.0");
			return "0.0";
		}
		else{
			System.out.println(uli+":"+v.split(",")[0]);
			return v.split(",")[0];
		}
	}
	
	static public String getLati(String uli){
		String v = indexMap.get(uli);
		if(v == null){
			return "0.0";
		}
		else{
			System.out.println(uli+":"+v.split(",")[1]);
			return v.split(",")[1];
		}
	}
	
	static public String getRegion(String uli){
		String v = indexMap.get(uli);
		if(v == null){
			return "";
		}
		else{
			return v.split(",")[2];
		}
	}
	
	static public String getProv(String uli){
		String v = indexMap.get(uli);
		if(v == null){
			return "";
		}
		else{
			return v.split(",")[3];
		}
	}
	
	static public String getCity(String uli){
		String v = indexMap.get(uli);
		if(v == null){
			return "";
		}
		else{
			return v.split(",")[4];
		}
	}
	
	static public String getDist(String uli){
		String v = indexMap.get(uli);
		if(v == null){
			return "";
		}
		else{
			return v.split(",")[5];
		}
	}
	
	static public String getAddr(String uli){
		String v = indexMap.get(uli);
		if(v == null || v.split(",").length <7){
			return "";
		}
		else{ 
//			if(v.split(",")[6] == "null"){
//				System.out.println("add null :"+v.split(",")[6]+" "+v.split(",")[5]);
//				return v.split(",")[5];
//
//			}
//			else{
				
				return v.split(",")[6];
			//}
			
		}
	}
	
	
	public static void main(String[] argv){
		loadIndex("index.csv");
		System.out.println(getRegion("460-00-70320-002")  );
	}
}
