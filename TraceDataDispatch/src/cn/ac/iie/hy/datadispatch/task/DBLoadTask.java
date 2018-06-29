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

import com.google.gson.Gson;

import cn.ac.iie.hy.datadispatch.data.SMetaData;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class DBLoadTask implements Runnable {

	List<SMetaData> changeList = null;
	
	public static String[] urls = null; 
	static Logger logger = null;
	
	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DBLoadTask.class.getName());
		
	}
	private JsonAvroConverter converter = new JsonAvroConverter();
	Gson gson = new Gson();
	
//	private static HashMap<String, String> codeMap = new HashMap<String, String>(){
//
//		private static final long serialVersionUID = -4696130080509842698L;
//
//		{
//			put("11", "鍖椾含");
//			put("12","澶╂触");
//			put("13", "娌冲寳");
//			put("14", "灞辫タ");
//			put("15", "鍐呰挋鍙�");
//			put("21", "杈藉畞");
//			put("22", "鍚夋灄");
//			put("23", "榛戦緳姹�");
//			put("31", "涓婃捣");
//			put("32", "姹熻嫃");
//			put("33", "娴欐睙");
//			put("34", "瀹夊窘");
//			put("35", "绂忓缓");
//			put("36", "姹熻タ");
//			put("37", "灞变笢");
//			put("41", "娌冲崡");
//			put("42", "婀栧寳");
//			put("43", "婀栧崡");
//			put("44", "骞夸笢");
//			put("45", "骞胯タ");
//			put("46", "娴峰崡");
//			put("50", "閲嶅簡");
//			put("51", "鍥涘窛");
//			put("52", "璐靛窞");
//			put("53", "浜戝崡");
//			put("54", "瑗胯棌");
//			put("61", "闄曡タ");
//			put("62", "鐢樿們");
//			put("63", "闈掓捣");
//			put("64", "瀹佸");
//			put("65", "鏂扮枂");
//		}
//	};
	
	public DBLoadTask(List<SMetaData> changeList) {
		super();
		this.changeList = changeList;
	}
	
	public  static void setUrlsList(String[] urls){
		DBLoadTask.urls = urls;
	}
	
	public byte[] getAvroData() {
		try {
			Protocol protocol = Protocol.parse(new File("t_lbs_trace_history.json"));

			// 寰楀埌鏁翠釜鏁版嵁鍖卍oc_set鐨勬牸寮�
			Schema docsSchema = protocol.getType("docs");
			GenericRecord docsRecord = new GenericData.Record(docsSchema);
			GenericArray docSet = new GenericData.Array<GenericRecord>(changeList.size(),
					docsSchema.getField("doc_set").schema());

			// 鏁版嵁鍖呭唴姣忎釜record鐨勫叿浣撴牸寮�
			Schema dataSchema = protocol.getType("t_lbs_trace_history");

			DatumWriter<GenericRecord> dataWriter = new GenericDatumWriter<GenericRecord>(dataSchema);
			ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
			BinaryEncoder bfdxbe = new EncoderFactory().binaryEncoder(dataBaos, null);

			for (Iterator<SMetaData> it = changeList.iterator(); it.hasNext();) {
				SMetaData smd = (SMetaData)it.next();
				
				if(smd.getC_imei().length() > 20 
						|| smd.getC_imsi().length() > 20
						|| smd.getC_msisdn().length() > 20
						|| smd.getC_areacode().length() > 8){
					continue;
				}
				
				
				String ssmd = gson.toJson(smd);
				//System.out.println(smd);
				
				
//				if(items.length < 11){
//					continue;
//				}
				
				//GenericRecord dataRecord = new GenericData.Record(dataSchema);
				GenericRecord dataRecord = converter.convertToGenericDataRecord(ssmd.getBytes(), dataSchema);
//				dataRecord.put("c_imsi", items[0]);
//				dataRecord.put("c_imei", items[1]);
//				dataRecord.put("c_msisdn", items[2]);
//				dataRecord.put("c_last_province", codeMap.get(items[items.length-1].substring(0, 2)));
//				dataRecord.put("c_curr_province", codeMap.get(items[3].substring(0, 2)));
//				
//				dataRecord.put("c_time", System.currentTimeMillis()/1000);
				dataWriter.write(dataRecord, bfdxbe);
				bfdxbe.flush();
				docSet.add(ByteBuffer.wrap(dataBaos.toByteArray()));
				dataBaos.reset();
				//System.out.println(dataRecord);

	        }

			// 灏嗕簩杩涘埗鏁版嵁濉厖
			docsRecord.put("doc_set", docSet);
			docsRecord.put("doc_schema_name", "t_lbs_trace_history");
			// 绛惧悕鍙牴鎹笟鍔＄郴缁熼渶姹傛潵濉啓
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
		int index = (int)(Math.random()*urls.length);
		return "http://" + urls[index];
	}
	
	public Boolean sendData() {
		// long startTime = System.currentTimeMillis();
		// System.out.println(startTime);
		HttpClient httpClient = new DefaultHttpClient();
		Boolean ret = true;
		try {
			byte[] data = getAvroData();

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
