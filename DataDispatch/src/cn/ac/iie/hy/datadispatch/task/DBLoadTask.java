package cn.ac.iie.hy.datadispatch.task;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class DBLoadTask implements Runnable {

	List<String> changeList = null;
	
	public static String[] urls = null; 
	static Logger logger = null;
	
	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DBLoadTask.class.getName());
		
	}
	
	
	private static HashMap<String, String> codeMap = new HashMap<String, String>(){

		private static final long serialVersionUID = -4696130080509842698L;

		{
			put("11", "北京");
			put("12","天津");
			put("13", "河北");
			put("14", "山西");
			put("15", "内蒙古");
			put("21", "辽宁");
			put("22", "吉林");
			put("23", "黑龙江");
			put("31", "上海");
			put("32", "江苏");
			put("33", "浙江");
			put("34", "安徽");
			put("35", "福建");
			put("36", "江西");
			put("37", "山东");
			put("41", "河南");
			put("42", "湖北");
			put("43", "湖南");
			put("44", "广东");
			put("45", "广西");
			put("46", "海南");
			put("50", "重庆");
			put("51", "四川");
			put("52", "贵州");
			put("53", "云南");
			put("54", "西藏");
			put("61", "陕西");
			put("62", "甘肃");
			put("63", "青海");
			put("64", "宁夏");
			put("65", "新疆");
		}
	};
	
	public DBLoadTask(List<String> changeList) {
		super();
		this.changeList = changeList;
		//logger.info("before send0011");
	}
	
	public  static void setUrlsList(String[] urls){
		DBLoadTask.urls = urls;
	}
	
	public byte[] getAvroData() {
		try {
			Protocol protocol = Protocol.parse(new File("t_ksls.json"));

			// 得到整个数据包doc_set的格式
			Schema docsSchema = protocol.getType("docs");
			GenericRecord docsRecord = new GenericData.Record(docsSchema);
			GenericArray docSet = new GenericData.Array<GenericRecord>(changeList.size(),
					docsSchema.getField("doc_set").schema());

			// 数据包内每个record的具体格式
			Schema dataSchema = protocol.getType("t_ksls");

			DatumWriter<GenericRecord> dataWriter = new GenericDatumWriter<GenericRecord>(dataSchema);
			ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
			BinaryEncoder bfdxbe = new EncoderFactory().binaryEncoder(dataBaos, null);

			for (Iterator<String> it = changeList.iterator(); it.hasNext();) {
				String smd = (String)it.next();
				if(smd == null||smd.isEmpty()){
					continue;
				}
				//System.out.println(smd);
				String[] items = smd.split(",");
				
//				if(items.length < 11){
//					continue;
//				}
				
				GenericRecord dataRecord = new GenericData.Record(dataSchema);
				dataRecord.put("c_imsi", items[0]);
				dataRecord.put("c_imei", items[1]);
				dataRecord.put("c_msisdn", items[2]);
				dataRecord.put("c_last_province", codeMap.get(items[items.length-1].substring(0, 2)));
				dataRecord.put("c_curr_province", codeMap.get(items[3].substring(0, 2)));
				
				dataRecord.put("c_time", System.currentTimeMillis()/1000);
				dataWriter.write(dataRecord, bfdxbe);
				bfdxbe.flush();
				docSet.add(ByteBuffer.wrap(dataBaos.toByteArray()));
				dataBaos.reset();
				//System.out.println(dataRecord);

	        }

			// 将二进制数据填充
			docsRecord.put("doc_set", docSet);
			docsRecord.put("doc_schema_name", "t_ksls");
			// 签名可根据业务系统需求来填写
			docsRecord.put("sign", "IIE");
			//System.out.println(docsRecord);
			DatumWriter<GenericRecord> docsWriter = new GenericDatumWriter<GenericRecord>(docsSchema);
			ByteArrayOutputStream docssos = new ByteArrayOutputStream();
			BinaryEncoder docsbe = new EncoderFactory().binaryEncoder(docssos, null);
			docsWriter.write(docsRecord, docsbe);
			docsbe.flush();
			return docssos.toByteArray();
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	private String getUrl(){
		int index = (int)(1+Math.random()*(urls.length-1));
		return "http://" + urls[index];
	}
	
	public Boolean sendData() {
		// long startTime = System.currentTimeMillis();
		// System.out.println(startTime);
		//logger.info("before send0000");
		HttpClient httpClient = new DefaultHttpClient();
		Boolean ret = true;
		//logger.info("before send");
		try {
			byte[] data = getAvroData();
			
			//logger.info("before send2");
			HttpPost httppost = new HttpPost(getUrl());
			InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), -1);
			reqEntity.setContentType("binary/octet-stream");
			reqEntity.setChunked(true);
			httppost.setEntity(reqEntity);
			HttpResponse response = httpClient.execute(httppost);
			logger.warn(response.getStatusLine());
			httppost.releaseConnection();
		} catch (Exception ex) {
			ret = false;
			ex.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}

		return ret;
		// long endTime = System.currentTimeMillis();

		// System.out.println("use " + (endTime - startTime) + " ms ");
	}
//	@Override
//	public void run() {
//		String fileName = getFileName();
//		
//		try {
//			File file =new File(fileName);
//			FileWriter fileWritter;
//			fileWritter = new FileWriter(file,true);
//			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
//			for(String data : changeList){
//				bufferWritter.write(data+"\n");
//			}
//			bufferWritter.close();
//			fileWritter.close();
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//	}
	@Override
	public void run() {
		sendData();
	}

}
