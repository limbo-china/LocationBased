package cn.ac.iie.hy.datadispatch.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class HomeCodeMap {

	private static HashMap<String, String> homecodeMap = new HashMap<String, String>();

	static public void loadIndex(String fileName){
		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
			inputStream = new FileInputStream(fileName);
			sc = new Scanner(inputStream, "UTF-8");
			
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				if(line.split(":").length > 1){
					String key = line.split(":")[0];
					String value = line.split(":")[1];
					homecodeMap.put(key, value);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	static public boolean homecodeExists(String homecode){
		return homecodeMap.containsKey(homecode);
	}
	
	static public boolean homecodePair(String regionCode, String homeCode){
		String province = homecodeMap.get(homeCode);
		if(province.equals(regionCode.substring(0, 2))){
			return false;
		}
		else{
			return true;
		}
	}
}
