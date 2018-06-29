package cn.ac.iie.hy.dataload.task;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

import cn.ac.iie.hy.dataload.dbutils.RedisUtil;
import cn.ac.iie.hy.dataload.tokenbucket.TokenBucket;
import cn.ac.iie.hy.dataload.tokenbucket.TokenBuckets;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class TraceCenterLoadTask implements Runnable{
	private int batchCount  = 0;
	private long timeInternal = 0L;
	private String areaID = null;
	private List<Object> al = new ArrayList<Object>();
	public static String[] urls = null;
    static Logger logger = null;
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(TraceCenterLoadTask.class.getName());
    }
	public TraceCenterLoadTask(int batchCount, long timeInternal, String[] urls, String areaID) {
		super();
		this.batchCount = batchCount;
		this.timeInternal = timeInternal;
		this.areaID = areaID;
		this.urls = urls;
	}

	public byte[] getAvroData() {
		try {
			Protocol protocol = Protocol.parse(new File("t_lbs_trace_history.json"));

			// 瀵版鍩岄弫缈犻嚋閺佺増宓侀崠鍗峯c_set閻ㄥ嫭鐗稿锟�
			Schema docsSchema = protocol.getType("docs");
			GenericRecord docsRecord = new GenericData.Record(docsSchema);
			GenericArray docSet = new GenericData.Array<GenericRecord>(al.size(),
					docsSchema.getField("doc_set").schema());

			// 閺佺増宓侀崠鍛敶濮ｅ繋閲渞ecord閻ㄥ嫬鍙挎担鎾寸壐瀵拷
			Schema dataSchema = protocol.getType("t_lbs_trace_history");

			DatumWriter<GenericRecord> dataWriter = new GenericDatumWriter<GenericRecord>(dataSchema);
			ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
			BinaryEncoder bfdxbe = new EncoderFactory().binaryEncoder(dataBaos, null);

			//for (int i = 0; i < batchSize; i++) {
			for (Iterator<Object> it = al.iterator(); it.hasNext();) {
				String smd = (String)it.next();
				if(smd == null||smd.isEmpty()){
					continue;
				}
				//System.out.println(smd);
				String[] items = smd.split(";");
				
				if(items.length < 11){
					continue;
				}
				String homecode = items[3];
				if(items[3].length() < 6 ||items[3].startsWith("0")){
					homecode = areaID;
				}
				GenericRecord dataRecord = new GenericData.Record(dataSchema);
                dataRecord.put("c_imsi", items[0]);
                dataRecord.put("c_imei", items[1]);
				dataRecord.put("c_msisdn", items[2]);
				dataRecord.put("c_uli", items[6]);
				dataRecord.put("c_areacode", homecode);
				dataRecord.put("c_timestamp", System.currentTimeMillis()/1000);
				dataWriter.write(dataRecord, bfdxbe);
				bfdxbe.flush();
				docSet.add(ByteBuffer.wrap(dataBaos.toByteArray()));
				dataBaos.reset();
				//System.out.println(dataRecord);

	        }

			// 鐏忓棔绨╂潻娑樺煑閺佺増宓佹繅顐㈠帠
			docsRecord.put("doc_set", docSet);
			docsRecord.put("doc_schema_name", "t_lbs_trace_history");
			// 缁涙儳鎮曢崣顖涚壌閹诡喕绗熼崝锛勯兇缂佺喖娓跺Ч鍌涙降婵夘偄鍟�
			docsRecord.put("sign", "LBS");
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
			System.out.println(response.getStatusLine());
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
	String[] urls = {"127.0.0.1:8012/dataload/?region=nc"};
		
		TraceCenterLoadTask task = new TraceCenterLoadTask(1, 1,urls , "330000");
//		//task.al.add("460030761320091;;8618008306626;500000;52;6677;460-03-52-6677;023;0.0;0.0;1484618904;1484618904");
//		//task.al.add("460031204310629;;8618008306636;500000;52;6677;460-03-52-6677;023;0.0;0.0;1484618904;1484618904");
		task.al.add("460110193955942;86827602023428;8618072772675;330000;19216;162668721;460-03-635424-177;0571;0.0;0.0;1488353573;1488353573");
//		//task.al.add("460110129825682;35929206365118;8618918305613;310000;52;6677;460-03-52-6677;021;0.0;0.0;1487661345;1487661345");
		task.sendData();
	}
	
	@Override
	public void run() {
		TokenBucket bucket = TokenBuckets.builder().withCapacity(batchCount * 1)
				.withFixedIntervalRefillStrategy(batchCount, timeInternal, TimeUnit.SECONDS).build();
		while (true) {
			// startTime = System.currentTimeMillis();
			Jedis myJedis = null;
			try {
				myJedis = RedisUtil.getJedis();
				Long length = myJedis.llen("CenterPushQueue");
				if (length < batchCount) {
					Thread.sleep(100);
					RedisUtil.returnResource(myJedis);
					continue;
				} else {
					Pipeline pipe = myJedis.pipelined();
					for (int i = 0; i < batchCount; i++) {
						pipe.lpop("CenterPushQueue");
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
