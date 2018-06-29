package cn.ac.iie.hy.dataload.task;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.dataload.tokenbucket.TokenBucket;
import cn.ac.iie.hy.dataload.tokenbucket.TokenBuckets;
import cn.ac.iie.hy.dataload.crypt.CryptData;
import cn.ac.iie.hy.dataload.crypt.DataCrypt;
import cn.ac.iie.hy.dataload.dbutils.RedisUtil;
import cn.ac.iie.hy.dataload.server.DataLoadServer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　　　　　┃
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　　　　　┃
 * 　　┃　　　┻　　　┃
 * 　　┃　　　　　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃神兽保佑, 永无BUG!
 * 　　　　┃　　　┃Code is far away from bug with the animal protecting
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * @author zhangyu
 *
 */
public class DataLoadTask implements Runnable{

	private int batchCount  = 0;
	private long timeInternal = 0L;
	private String areaID = null;
	private List<Object> al = new ArrayList<Object>();
	private String url = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataLoadTask.class.getName());
    }

	public DataLoadTask(int batchCount, long timeInternal, String url, String areaID) {
		this.batchCount = batchCount;
		this.timeInternal = timeInternal;
		this.url = url;
		this.areaID = areaID;
	}
	

	public byte[] getAvroData() {
		try {
			Protocol protocol = Protocol.parse(new File("xl-protocol.json"));

			// 得到整个数据包doc_set的格式
			Schema docsSchema = protocol.getType("docs");
			GenericRecord docsRecord = new GenericData.Record(docsSchema);
			GenericArray docSet = new GenericData.Array<GenericRecord>(al.size(),
					docsSchema.getField("doc_set").schema());

			// 数据包内每个record的具体格式
			Schema dataSchema = protocol.getType("xl");

			DatumWriter<GenericRecord> dataWriter = new GenericDatumWriter<GenericRecord>(dataSchema);
			ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
			BinaryEncoder bfdxbe = new EncoderFactory().binaryEncoder(dataBaos, null);

			//for (int i = 0; i < batchSize; i++) {
			for (Iterator<Object> it = al.iterator(); it.hasNext();) {
				String smd = (String)it.next();
				if(smd == null||smd.isEmpty()){
					continue;
				}
				String[] items = smd.split(";");
				if(items.length < 11){
					continue;
				}
				
				CryptData cd = new CryptData();
				cd.setImsi(items[0]);
				cd.setImei(items[1]);
				cd.setMsisdn(items[2]);
				
				int dedataout_result = 0;
				
				if(!isNotJM(cd.getImsi())){
					dedataout_result = cd.decryptData();
					while(-1 == dedataout_result){
						logger.info("Decrypt ticket time out!");
						try {
							DataCrypt.auth("jm.conf");
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						dedataout_result = cd.decryptData();
					}
				}
				//cd.print();
				String homecode = areaID;
				
				GenericRecord dataRecord = new GenericData.Record(dataSchema);
				dataRecord.put("imsi", cd.getImsi());
				dataRecord.put("imei", cd.getImei());
				dataRecord.put("msisdn", cd.getMsisdn());
				dataRecord.put("regioncode", homecode);
				dataRecord.put("lac", items[4]);
				dataRecord.put("ci", items[5]);
				dataRecord.put("uli", items[6]);
				dataRecord.put("homecode", items[7]);
				dataRecord.put("lngi", 0.0f);
				dataRecord.put("lati", 0.0f);
				dataRecord.put("timestamp", System.currentTimeMillis()/1000);
				dataWriter.write(dataRecord, bfdxbe);
				bfdxbe.flush();
				docSet.add(ByteBuffer.wrap(dataBaos.toByteArray()));
				dataBaos.reset();
				//System.out.println(dataRecord);

	        }

			// 将二进制数据填充
			docsRecord.put("doc_set", docSet);
			docsRecord.put("doc_schema_name", "xl");
			// 签名可根据业务系统需求来填写
			docsRecord.put("sign", "your sign");
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
	private  boolean isNotJM(String str) throws PatternSyntaxException {  
		String regExp = "^[0-9]+$";  
		Pattern p = Pattern.compile(regExp);  
		Matcher m = p.matcher(str);  
		return m.matches();  
	}

	public Boolean sendData() {
		// long startTime = System.currentTimeMillis();
		// System.out.println(startTime);
		HttpClient httpClient = new DefaultHttpClient();
		Boolean ret = true;
		try {
			byte[] data = getAvroData();
			//System.out.println(data.length);

			HttpPost httppost = new HttpPost(url);
			InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), -1);
			reqEntity.setContentType("binary/octet-stream");
			reqEntity.setChunked(true);

			httppost.setEntity(reqEntity);
			//System.out.println("before");
			HttpResponse response = httpClient.execute(httppost);
			//System.out.println("after");
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
	
	public static void main(String[] argv){
		DataLoadTask task = new DataLoadTask(1, 1, "http://10.213.73.4:8010/dataload/?region=ppp", "330000");
		//task.al.add("460030761320091;;8618008306626;500000;52;6677;460-03-52-6677;023;0.0;0.0;1484618904;1484618904");
		//task.al.add("460031204310629;;8618008306636;500000;52;6677;460-03-52-6677;023;0.0;0.0;1484618904;1484618904");
		task.al.add("460110193955942;86827602023428;8618072772675;330000;19216;162668721;460-03-635424-177;0571;0.0;0.0;1488353573;1488353573");
		//task.al.add("460110129825682;35929206365118;8618918305613;310000;52;6677;460-03-52-6677;021;0.0;0.0;1487661345;1487661345");
		task.sendData();
	}
	
	@Override
	public void run() {

		TokenBucket bucket = TokenBuckets.builder().withCapacity(batchCount * 2)
				.withFixedIntervalRefillStrategy(batchCount, timeInternal, TimeUnit.SECONDS).build();

		while (true) {
			// startTime = System.currentTimeMillis();
			Jedis myJedis = null;
			try {
				myJedis = RedisUtil.getJedis();
				Long length = myJedis.llen("loadqueue");
				//System.out.println(length);
				if (length < batchCount) {
					Thread.sleep(100);
					RedisUtil.returnResource(myJedis);
					continue;
				} else {
					Pipeline pipe = myJedis.pipelined();
					for (int i = 0; i < batchCount; i++) {
						pipe.lpop("loadqueue");
					}
					al = pipe.syncAndReturnAll();
					if (!sendData()) {
						logger.error("send error");
					}
					bucket.consume(al.size());
					logger.warn("send " + al.size() + " records");
					al.clear();
					RedisUtil.returnResource(myJedis);
				}

			} catch (Exception e) {
				if (myJedis != null) {
					RedisUtil.returnBrokenResource(myJedis);
				}
			}

		}
	}

}
