package cn.ac.iie.hy.centralserver.task;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

import cn.ac.iie.hy.centralserver.dbutils.GPSDataFromOscar;

public class TmpTask {

	private static String timeStamp2Date(String seconds, String format) {
		if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
			return "";
		}
		if (format == null || format.isEmpty())
			format = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date(Long.valueOf(seconds + "000")));
	}
	
	public static void main(String[] args) {
		FileInputStream inputStream = null;
		Scanner sc = null;
		List<String> result = new ArrayList();
		
		try {
			inputStream = new FileInputStream("Trace.txt");
			sc = new Scanner(inputStream, "UTF-8");
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				//System.out.println(line.split("\t")[3]);
				//String imsi = line.split("\t")[0];
				//String imei = line.split("\t")[1];
				//String msisdn = line.split("\t")[2];
				String uli = line;
				//String code = line.split("\t")[4];
				//String time= timeStamp2Date(line.split("\t")[5], null);
				
				result.add(uli + "," + new GPSDataFromOscar(uli).getGPS());
				//result.add(imsi + "," + imei + "," + msisdn + "," + uli + "," + code +"," + time + "," + new GPSDataFromOscar(uli).getGPS());
				
				//System.out.println( new GPSDataFromOscar(uli).getGPS());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			FileOutputStream fos = new FileOutputStream("result.txt");
			OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
			for (String r : result) {
				osw.write(r + "\n");
			}
			osw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
