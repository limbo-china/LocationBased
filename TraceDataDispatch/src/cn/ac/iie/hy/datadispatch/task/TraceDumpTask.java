package cn.ac.iie.hy.datadispatch.task;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;

import cn.ac.iie.hy.datadispatch.data.SMetaData;

public class TraceDumpTask implements Runnable {

	List<SMetaData> dataList = null;
	
	Gson gson = new Gson();
	public static Map<String, String> importPersion = new ConcurrentHashMap<>();
	
	static{
		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
			inputStream = new FileInputStream("importantPerson.txt");
			sc = new Scanner(inputStream, "UTF-8");
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				importPersion.put(line, "1");
			}
		} catch (FileNotFoundException e) {
			//logger.error("no file found");
		}
	}
	
	public TraceDumpTask(List<SMetaData> changeList) {
		super();
		this.dataList = changeList;
	}

	private String getDate() {
		String format = "yyyy-MM-dd";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date(System.currentTimeMillis()));
	}
	
	@Override
	public void run() {
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream("data/"+ System.currentTimeMillis()+".txt", true);
			OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
			
			for(SMetaData smd : dataList){
				if(importPersion.containsKey(smd.getC_msisdn())){
					String tmp = gson.toJson(smd);
					osw.write(tmp+"\n");
				}
			}
			
			osw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				fos.close();			
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
