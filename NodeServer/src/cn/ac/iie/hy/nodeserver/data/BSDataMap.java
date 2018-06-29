package cn.ac.iie.hy.nodeserver.data;

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
					String value = line.split(",")[5] + "," + line.split(",")[6] + "," + line.split(",")[10];
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
			return "0.0";
		}
		else{
			return v.split(",")[0];
		}
	}
	
	static public String getLati(String uli){
		String v = indexMap.get(uli);
		if(v == null){
			return "0.0";
		}
		else{
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
	public static void main(String[] argv){
		loadIndex("index.csv");
		System.out.println(getRegion("460-00-70320-002")  );
	}
}
