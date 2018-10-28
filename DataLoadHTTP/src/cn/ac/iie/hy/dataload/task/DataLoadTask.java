package cn.ac.iie.hy.dataload.task;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.xml.validation.Schema;

import cn.ac.iie.hy.dataload.dbutils.RedisUtil;
import cn.ac.iie.hy.dataload.tokenbucket.TokenBucket;
import cn.ac.iie.hy.dataload.tokenbucket.TokenBuckets;

/**
 * 閳逛讲鏀ｉ埞浣叉敚閳逛讲鏀ｇ粊鐐插悢閸戠儤鐥呴埞浣叉敚閳逛讲鏀ｉ埞浣叉敚 閵嗭拷閵嗭拷閵嗭拷閳瑰繆鏁侀妴锟介妴锟介妴锟介埞蹇婃晛
 * 閵嗭拷閵嗭拷閳瑰繆鏁嶉埞鐑╂敚閳逛讲鏀ｉ埞娑掓暰閳癸拷 閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳癸拷
 * 閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閳逛降锟斤拷閵嗭拷閵嗭拷閳癸拷 閵嗭拷閵嗭拷閳瑰啨锟斤拷閳规枼鏁嶉妴锟介埞妞绘暤閵嗭拷閳癸拷
 * 閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳癸拷 閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閳规眹锟斤拷閵嗭拷閵嗭拷閳癸拷
 * 閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳癸拷 閵嗭拷閵嗭拷閳规せ鏀ｉ埞鎾憋拷锟介妴锟介妴锟介埞蹇婃敚閳癸拷
 * 閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閳瑰啰顨ｉ崗鎴掔箽娴ｏ拷, 濮樺憡妫UG! 閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閳瑰儉ode
 * is far away from bug with the animal protecting
 * 閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閳规せ鏀ｉ埞浣叉敚閳癸拷
 * 閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳癸絺鏁�
 * 閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳瑰啨锟斤拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳瑰繆鏁� 閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳规せ鏁侀埞鎾敽閳逛讲鏁甸埞鎾敽閳癸拷
 * 閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳瑰啠鏁ㄩ埞顐狅拷锟介埞鍐ｆ暔閳癸拷 閵嗭拷閵嗭拷閵嗭拷閵嗭拷閵嗭拷閳规せ鏁鹃埞娑栵拷锟介埞妞绘暰閳癸拷
 * 閳逛讲鏀ｉ埞浣叉敚閳逛讲鏀ｉ幇鐔活潕閽�宀冩倱閸濇巻鏀ｉ埞浣叉敚閳逛讲鏀ｉ埞锟�
 * 
 * @author zhangyu
 *
 */
public class DataLoadTask implements Runnable {

	private int batchCount = 0;
	private long timeInternal = 0L;
	private String areaID = null;
	private List<Object> al = new ArrayList<Object>();
	private String url = null;
	static Logger logger = null;

	// for test
	private Random random = new Random();

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataLoadTask.class.getName());
	}

	public DataLoadTask(int batchCount, long timeInternal, String url,
			String areaID) {
		this.batchCount = batchCount;
		this.timeInternal = timeInternal;
		this.url = url;
		this.areaID = areaID;
	}

	public byte[] getAvroData() {
		try {
			Protocol protocol = Protocol.parse(new File("t_cdr_in.json"));

			// 瀵版鍩岄弫缈犻嚋閺佺増宓侀崠鍗峯c_set閻ㄥ嫭鐗稿锟�
			Schema docsSchema = protocol.getType("docs");
			GenericRecord docsRecord = new GenericData.Record(docsSchema);
			GenericArray docSet = new GenericData.Array<GenericRecord>(
					al.size(), docsSchema.getField("doc_set").schema());

			// 閺佺増宓侀崠鍛敶濮ｅ繋閲渞ecord閻ㄥ嫬鍙挎担鎾寸壐瀵拷
			Schema dataSchema = protocol.getType("t_cdr_in");

			DatumWriter<GenericRecord> dataWriter = new GenericDatumWriter<GenericRecord>(
					dataSchema);
			ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
			BinaryEncoder bfdxbe = new EncoderFactory().binaryEncoder(dataBaos,
					null);

			// for (int i = 0; i < batchSize; i++) {
			for (Iterator<Object> it = al.iterator(); it.hasNext();) {
				String smd = (String) it.next();
				if (smd == null || smd.isEmpty()) {
					continue;
				}
				// System.out.println(smd);
				String[] items = smd.split(";");

				if (items.length < 11) {
					continue;
				}

				String homecode = areaID;

				// for test
				// String[] areaIDs = areaID.split(",");
				// String homecode = areaIDs[random.nextInt(areaIDs.length)];

				GenericRecord dataRecord = new GenericData.Record(dataSchema);
				dataRecord.put("imsi", items[0]);
				dataRecord.put("imei", items[1]);
				dataRecord.put("msisdn", items[2]);
				dataRecord.put("regioncode", homecode);
				dataRecord.put("lac", items[4]);
				dataRecord.put("ci", items[5]);
				dataRecord.put("uli", items[6]);
				dataRecord.put("homecode", items[7]);
				dataRecord.put("lngi", 0.0f);
				dataRecord.put("lati", 0.0f);
				dataRecord.put("timestamp", System.currentTimeMillis() / 1000);
				dataWriter.write(dataRecord, bfdxbe);
				bfdxbe.flush();
				docSet.add(ByteBuffer.wrap(dataBaos.toByteArray()));
				dataBaos.reset();
				// System.out.println(dataRecord);

			}

			// 鐏忓棔绨╂潻娑樺煑閺佺増宓佹繅顐㈠帠
			docsRecord.put("doc_set", docSet);
			docsRecord.put("doc_schema_name", "xl");
			// 缁涙儳鎮曢崣顖涚壌閹诡喕绗熼崝锛勯兇缂佺喖娓跺Ч鍌涙降婵夘偄鍟�
			docsRecord.put("sign", "your sign");
			// System.out.println(docsRecord);
			DatumWriter<GenericRecord> docsWriter = new GenericDatumWriter<GenericRecord>(
					docsSchema);
			ByteArrayOutputStream docssos = new ByteArrayOutputStream();
			BinaryEncoder docsbe = new EncoderFactory().binaryEncoder(docssos,
					null);
			docsWriter.write(docsRecord, docsbe);
			docsbe.flush();
			return docssos.toByteArray();
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	public Boolean sendData() {
		// long startTime = System.currentTimeMillis();
		// System.out.println(startTime);
		HttpClient httpClient = new DefaultHttpClient();
		Boolean ret = true;
		try {
			byte[] data = getAvroData();
			// System.out.println(data.length);

			HttpPost httppost = new HttpPost(url);
			InputStreamEntity reqEntity = new InputStreamEntity(
					new ByteArrayInputStream(data), -1);
			reqEntity.setContentType("binary/octet-stream");
			reqEntity.setChunked(true);

			httppost.setEntity(reqEntity);
			// System.out.println("before");
			HttpResponse response = httpClient.execute(httppost);
			// System.out.println("after");
			logger.info(response.getStatusLine());
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

	public static void main(String[] argv) {
		DataLoadTask task = new DataLoadTask(1, 1,
				"http://10.213.73.4:8010/dataload/?region=ppp", "330000");
		// task.al.add("460030761320091;;8618008306626;500000;52;6677;460-03-52-6677;023;0.0;0.0;1484618904;1484618904");
		// task.al.add("460031204310629;;8618008306636;500000;52;6677;460-03-52-6677;023;0.0;0.0;1484618904;1484618904");
		task.al.add("460110193955942;86827602023428;8618072772675;330000;19216;162668721;460-03-635424-177;0571;0.0;0.0;1488353573;1488353573");
		// task.al.add("460110129825682;35929206365118;8618918305613;310000;52;6677;460-03-52-6677;021;0.0;0.0;1487661345;1487661345");
		task.sendData();
	}

	@Override
	public void run() {

		TokenBucket bucket = TokenBuckets
				.builder()
				.withCapacity(batchCount * 2)
				.withFixedIntervalRefillStrategy(batchCount, timeInternal,
						TimeUnit.SECONDS).build();

		while (true) {
			Jedis myJedis = null;
			try {
				myJedis = RedisUtil.getJedis();
				Long length = myJedis.llen("loadqueue");
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
