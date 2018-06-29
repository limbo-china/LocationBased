package cn.ac.iie.hy.nodeserver.task;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.google.gson.Gson;

public class TMP {

	static Map<String, String> locationsMap = new HashMap<>();
	static void load(){
		FileInputStream inputStream = null;
		Scanner sc = null;
		
		try {
			inputStream = new FileInputStream("index.csv");
			sc = new Scanner(inputStream, "UTF-8");
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				locationsMap.put(line.split(",")[line.split(",").length - 1], line);
				//System.out.println(line.split(",")[line.split(",").length - 1]);
//				if(line.split(":").length > 1 && line.split(":")[1].length() == 6){
//					String imsi = line.split(":")[0];
//					String regionId = line.split(":")[1].substring(0, 4) + "00";
//					locationsMap.put(imsi, regionId);
//				}
			}
			inputStream.close();
		} catch (FileNotFoundException e) {
			//logger.error("no file found");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void main(String[] args) {
		load();
		FileInputStream inputStream = null;
		Scanner sc = null;
		Gson gson = new Gson();
		try {
			inputStream = new FileInputStream("XAC_65_LOCATION_MAP20171024.txt");
			sc = new Scanner(inputStream, "UTF-8");
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				Data data = gson.fromJson(line, Data.class);
				
				NData ndata= new NData();
				ndata.setC_msisdn(data.getC_msisdn());
				ndata.setC_areacode(data.getC_areacode());
				ndata.setC_province(data.getC_province());
				ndata.setC_uli(data.getC_uli());
				ndata.setC_date(data.getC_date());
				if(locationsMap.containsKey(ndata.getC_uli())){
					ndata.setLngi(locationsMap.get(ndata.getC_uli()).split(",")[5]);
					ndata.setLati(locationsMap.get(ndata.getC_uli()).split(",")[6]);
					ndata.setAddr(locationsMap.get(ndata.getC_uli()).split(",")[3]);
				}
				else{
					ndata.setLngi("null");
					ndata.setLati("null");
					ndata.setAddr("null");
				}
				System.out.println(gson.toJson(ndata));
				//locationsMap.put(line.split(",")[line.split(",").length - 1], line);
//				if(line.split(":").length > 1 && line.split(":")[1].length() == 6){
//					String imsi = line.split(":")[0];
//					String regionId = line.split(":")[1].substring(0, 4) + "00";
//					locationsMap.put(imsi, regionId);
//				}
			}
			inputStream.close();
		} catch (FileNotFoundException e) {
			//logger.error("no file found");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
