package cn.ac.iie.hy.dataload.task;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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

import cn.ac.iie.hy.dataload.dbutils.RedisUtil;
import cn.ac.iie.hy.dataload.tokenbucket.TokenBucket;
import cn.ac.iie.hy.dataload.tokenbucket.TokenBuckets;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class TraceProvinceLoadTaskV2 implements Runnable{

	private int batchCount  = 0;
	private long timeInternal = 0L;
	private List<Object> al = new ArrayList<Object>();
	private String url = null;
	private String schemaName = null;
	private Schema docsSchema = null;
    static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(TraceProvinceLoadTaskV2.class.getName());
    }
    
    
    
    public TraceProvinceLoadTaskV2(int batchCount, long timeInternal, String url, String schemaName) {
		super();
		this.batchCount = batchCount;
		this.timeInternal = timeInternal;
		this.url = url;
		this.schemaName = schemaName;
		
		Schema.Parser parser = new Schema.Parser();
        String schemaStr = getSchemaStr("schema/" + schemaName + ".json");
        System.out.println("Schema is " + schemaStr.replaceAll("\\s*", ""));
        docsSchema = parser.parse(schemaStr);
        
	}
    
    public static String getSchemaStr(String filePath){
        try {
            String encoding="UTF-8";
            File file=new File(filePath);
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                StringBuilder result = new StringBuilder();
                while((lineTxt = bufferedReader.readLine()) != null){
                    result.append(lineTxt);
                }
                read.close();
                return result.toString();
            }else{
                System.out.println("找不到指定的文件");
                return null;
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
            return null;
        }

    }

	public byte[] getAvroData() {
		try {
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(docsSchema);
            // ~=10MB
            ByteArrayOutputStream out = new ByteArrayOutputStream(10000000);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            out.reset();
            for (Iterator<Object> it = al.iterator(); it.hasNext(); ) {
                String smd = (String)it.next();
				if(smd == null||smd.isEmpty()){
					continue;
				}
				//System.out.println(smd);
				String[] items = smd.split(";");
				
				if(items.length < 11){
					continue;
				}
				//String homecode = areaID;
                GenericRecord dataRecord = new GenericData.Record(docsSchema);
                dataRecord.put("c_imsi", items[0]);
                dataRecord.put("c_imei", items[1]);
				dataRecord.put("c_msisdn", items[2]);
				dataRecord.put("c_uli", items[6]);
				dataRecord.put("c_areacode", items[3]);
				dataRecord.put("c_timestamp", System.currentTimeMillis()/1000);
                //System.out.println(smd);
                //GenericRecord dataRecord = converter.convertToGenericDataRecord(smd.getBytes(), docsSchema);
                //System.out.println(dataRecord);
                writer.write(dataRecord, encoder);
                //System.out.println(out.size());
            }
            encoder.flush();

            //return out.toByteArray();
//            byte[] value = out.toByteArray();
//            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(docsSchema);
//            BinaryDecoder defaultDecoder = null;
//            defaultDecoder = DecoderFactory.get().binaryDecoder(value, defaultDecoder);
//            GenericRecord gRecord = new GenericData.Record(docsSchema);
//            while (!defaultDecoder.isEnd()) {
//                try {
//                    datumReader.read(gRecord, defaultDecoder);
//                    System.out.println(gRecord);
//                    //log.info("to find key====================================================="+key);
//                } catch (IOException e1) {
//                    e1.printStackTrace();
//                }
//            }

            return out.toByteArray();
        } catch (IOException ex) {
            logger.error(ex.getMessage());
            return null;
        }
	}

	public Boolean sendData() {
		HttpClient httpClient = new DefaultHttpClient();
        Boolean ret = true;
        try {
            byte[] data = getAvroData();
            if (data != null) {
                HttpPost httppost = new HttpPost(url);
                httppost.addHeader("content-type", "utf-8");
                httppost.addHeader("Topic", schemaName + "_dt");
                httppost.addHeader("Format", "avro");

                InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), data.length);
                reqEntity.setContentType("binary/octet-stream");


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
	
	public static void main(String[] argv){
		TraceProvinceLoadTaskV2 task = new TraceProvinceLoadTaskV2 (1, 1, "http://127.0.0.1:8010/dataload/?region=ppp", "t_lbs_trace_history");
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
				Long length = myJedis.llen("PronvincePushQueue");
				if (length < batchCount) {
					Thread.sleep(100);
					RedisUtil.returnResource(myJedis);
					continue;
				} else {
					Pipeline pipe = myJedis.pipelined();
					for (int i = 0; i < batchCount; i++) {
						pipe.lpop("PronvincePushQueue");
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
