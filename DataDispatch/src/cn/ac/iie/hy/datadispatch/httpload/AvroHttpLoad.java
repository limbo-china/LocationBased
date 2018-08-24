package cn.ac.iie.hy.datadispatch.httpload;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import cn.ac.iie.hy.datadispatch.task.DBGJTask;

import com.fasterxml.jackson.core.type.TypeReference;
/*
 * AvroHttpLoad class
 * public basic util class
 * 
 */
public class AvroHttpLoad implements Serializable {
	private static final long serialVersionUID = -2965639532011484594L;
//	private static Config config = null;
	private static Logger logger = Logger.getLogger(AvroHttpLoad.class.getName());
	private static String loadSchemaUrl = null;
	private static String url = null;
	private String schemaName = null;
	private String redisQueueName = null;
	private static String topicName = null;
	private String user = "iie";
	private String passwd = "123456";
	private List<Object> al = new ArrayList<Object>();
	static Schema avroSchema = null;
	private JsonAvroConverter converter = new JsonAvroConverter();
  
	public static String loadSchemaUrlSTK,urlSTK, avroMonitorWarningTableName;
	

	static {
		   PropertyConfigurator.configure("log4j.properties");
		
		   String configurationFileName = "data-dispatcher.properties";

	    	Properties pps = new Properties();
			try {
				InputStream in = new BufferedInputStream(new FileInputStream(configurationFileName));
				pps.load(in);
				loadSchemaUrl = pps.getProperty("loadSchemaUrlSTK");
				url=pps.getProperty("load.data.url");
				topicName=pps.getProperty("avroMonitorWarningTableName");
				//topicName=avroMonitorWarningTableName;
				logger.info("monitorwarning publishToRedis!!loadSchemaUrlSTK " + loadSchemaUrl);
			    logger.info("monitorwarning publishToRedis!!lurlSTK " + url);
			    logger.info("monitorwarning publishToRedis!!avroMonitorWarningTableName " + topicName);
			    
			    
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				avroSchema = getAvroSchema(topicName);
			} catch (RestClientException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
	
			
	}
	
	
	
	public AvroHttpLoad() {

	}
	public AvroHttpLoad(String topicName) {
		this.topicName = topicName;

		try {
			avroSchema = getAvroSchema(topicName);
		} catch (RestClientException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

//	public AvroHttpLoad(String loadSchemaUrl, String url, String topicName) {
//		this.topicName = topicName;
//		this.loadSchemaUrl = loadSchemaUrl;
//		this.url = url;
//		try {
//			avroSchema = getAvroSchema(topicName);
//		} catch (RestClientException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

	public AvroHttpLoad(String schemaName, String redisQueueName, String topicName, String user, String passwd,
			String url) {
		this.schemaName = schemaName;
		this.redisQueueName = redisQueueName;
		this.topicName = topicName;
		this.user = user;
		this.passwd = passwd;
		this.url = url;
		Schema.Parser parser = new Schema.Parser();
		String schemaStr = getSchemaStr("schema/" + schemaName + ".json");
		logger.info("Schema is " + schemaStr.replaceAll("\\s*", ""));
		avroSchema = parser.parse(schemaStr);
	}

	public static Schema getAvroSchema(String topicName) throws Exception, RestClientException {
		Schema avroSchema = null;
		try{
			logger.info("loadSchemaUrl: "+loadSchemaUrl);
			CachedSchemaRegistryClient client;
	        client = new CachedSchemaRegistryClient(loadSchemaUrl, 100);
		    SchemaMetadata sm = client.getLatestSchemaMetadata(topicName);
			 avroSchema = new Schema.Parser().parse(sm.getSchema());
			logger.info("the " + topicName + " schema :" + avroSchema.toString());
			
		}catch(IOException e)
		{e.printStackTrace();}
		return avroSchema;
		
	}

	public List<Object> getAl() {
		return al;
	}

	public void setAl(List<Object> al) {
		this.al = al;
	}

	public static String getSchemaStr(String filePath) {
		try {
			String encoding = "UTF-8";
			File file = new File(filePath);
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				StringBuilder result = new StringBuilder();
				while ((lineTxt = bufferedReader.readLine()) != null) {
					result.append(lineTxt);
				}
				read.close();
				return result.toString();
			} else {
				System.out.println("找不到指定的文件");
				return null;
			}
		} catch (Exception e) {
			System.out.println("读取文件内容出错");
			e.printStackTrace();
			return null;
		}

	}

	public byte[] getAvroData() throws IOException {
		try {
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroSchema);
			// ~=10MB
			ByteArrayOutputStream out = new ByteArrayOutputStream(10000000);
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
			out.reset();
			for (Iterator<Object> it = al.iterator(); it.hasNext();) {
				String smd = (String) it.next();
				if (smd == null || smd.isEmpty()) {
					continue;
				}
				// System.out.println(smd);
				GenericRecord dataRecord = converter.convertToGenericDataRecord(smd.getBytes(), avroSchema);
				System.out.println(dataRecord);
				writer.write(dataRecord, encoder);
				System.out.println(out.size());
			}
			encoder.flush();

			// return out.toByteArray();
			// byte[] value = out.toByteArray();
			// DatumReader<GenericRecord> datumReader = new
			// GenericDatumReader<GenericRecord>(avroSchema);
			// BinaryDecoder defaultDecoder = null;
			// defaultDecoder = DecoderFactory.get().binaryDecoder(value,
			// defaultDecoder);
			// GenericRecord gRecord = new GenericData.Record(avroSchema);
			// while (!defaultDecoder.isEnd()) {
			// try {
			// datumReader.read(gRecord, defaultDecoder);
			// System.out.println(gRecord);
			// //log.info("to find
			// key====================================================="+key);
			// } catch (IOException e1) {
			// e1.printStackTrace();
			// }
			// }

			return out.toByteArray();
		} catch (IOException ex) {
			logger.error(ex.getMessage());
			return null;
		}
	}

	public byte[] getAvroData(List<Object> loadDataList) throws IOException {
		try {
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroSchema);
			// ~=10MB
			ByteArrayOutputStream out = new ByteArrayOutputStream(10000000);
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
			out.reset();
			for (Iterator<Object> it = loadDataList.iterator(); it.hasNext();) {
				String smd = (String) it.next();
				
				if (smd == null || smd.isEmpty()) {
					logger.info("monitorwarning avro: null");
					continue;
				}
				logger.info("monitorwarning avro:"+smd);
				//System.out.println(smd);
				if(converter == null)
				{
					logger.info("monitorwarning converter null :");
				}
				else
				{
					logger.info("monitorwarning converter not  null :");
				}
				if(avroSchema == null)
				{
					logger.info("monitorwarning avroSchema null :");
				}
				else
				{
				
						logger.info("monitorwarning avroSchema not null :"+avroSchema.toString());
					
				}
				GenericRecord dataRecord = converter.convertToGenericDataRecord(smd.getBytes(), avroSchema);
				// System.out.println(dataRecord);
				// System.out.println(dataRecord.toString());
				// System.out.println("Anti-avro START");
				// byte str1[] = converter.convertToJson(dataRecord);
				// System.out.println(str1.toString());
				writer.write(dataRecord, encoder);
				// System.out.println(out.size());
			}
			encoder.flush();
			return out.toByteArray();
		} catch (IOException ex) {
			logger.error(ex.getMessage());
			return null;
		}
	}

	public Boolean sendData(List<Object> loadDataList) {

		HttpClient httpClient = new DefaultHttpClient();
		Boolean ret = true;
		try {
			byte[] data = getAvroData(loadDataList);
			logger.info("monitorwarning avro: data.len "+ data.length);
		//	System.out.println("data.length:" + data.length);
			if (data != null) {
				HttpPost httppost = new HttpPost(url);
				httppost.addHeader("content-type", "utf-8");
				httppost.addHeader("User", user);
				httppost.addHeader("Password", passwd);
				httppost.addHeader("Topic", topicName);
				httppost.addHeader("Format", "avro");

				InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), data.length);
				reqEntity.setContentType("binary/octet-stream");
				// reqEntity.setChunked(true);

				httppost.setEntity(reqEntity);
				HttpResponse response = httpClient.execute(httppost);
				logger.info("Transfer status: " + response.getStatusLine());
				logger.info("monitorwarning getStatusLine "+ response.getStatusLine());
				httppost.releaseConnection();
			}

		} catch (IOException ex) {
			ret = false;
			logger.error(ex.getMessage());
		} finally {
			httpClient.getConnectionManager().shutdown();
		}
		return ret;
	}

	public Boolean sendData() {

		HttpClient httpClient = new DefaultHttpClient();
		Boolean ret = true;
		try {
			byte[] data = getAvroData();
			System.out.println(data.length);
			if (data != null) {
				HttpPost httppost = new HttpPost(url);
				httppost.addHeader("content-type", "utf-8");
				httppost.addHeader("User", user);
				httppost.addHeader("Password", passwd);
				httppost.addHeader("Topic", topicName);
				httppost.addHeader("Format", "avro");

				InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), data.length);
				reqEntity.setContentType("binary/octet-stream");
				// reqEntity.setChunked(true);

				httppost.setEntity(reqEntity);
				HttpResponse response = httpClient.execute(httppost);
				logger.info("Transfer status: " + response.getStatusLine());
				httppost.releaseConnection();
			}

		} catch (IOException ex) {
			ret = false;
			logger.error(ex.getMessage());
		} finally {
			httpClient.getConnectionManager().shutdown();
		}

		return ret;
	}

	/**
	 * 单元测试
	 */
	public static void main(String[] argv) {
		// new DataLoadTask(1, 1L, "http://127.0.0.1:8080/dataload/", "xl",
		// "qqqq").run();
		List<Object> al = new ArrayList<Object>();
		al.add("{\"imsi\":\"4601233123123\", \"imei\":\"123123123123\", \"msisdn\":\"8612313123222\"}");
		al.add("{\"imsi\":\"4601233123123\", \"imei\":\"123123123123\", \"msisdn\":\"8612313123222\"}");
		al.add("{\"imsi\":\"4601233123123\", \"imei\":\"123123123123\", \"msisdn\":\"8612313123222\"}");
		AvroHttpLoad task = new AvroHttpLoad("demo", "qqqq", "ttt", "fsaff", "dddd", "http://127.0.0.1:8080/dataload/");
		task.setAl(al);
		task.sendData();
	}
}