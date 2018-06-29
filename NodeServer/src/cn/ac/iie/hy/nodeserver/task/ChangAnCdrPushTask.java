package cn.ac.iie.hy.nodeserver.task;

import java.nio.charset.Charset;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.nodeserver.dbutils.RedisUtilPro;
import cn.ac.iie.hy.nodeserver.server.DataNodeServer;
import cn.ac.iie.hy.nodeserver.tokenbucket.TokenBucket;
import cn.ac.iie.hy.nodeserver.tokenbucket.TokenBuckets;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class ChangAnCdrPushTask  implements Runnable{

	static Logger logger = null;

	static Random ra =new Random();
	
	private StringBuffer buffer  = new StringBuffer(2000000);
	
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataNodeServer.class.getName());
    }
	/**
	 * 时间戳转换成日期格式字符串
	 * 
	 * @param seconds
	 *            精确到秒的字符串
	 * @param formatStr
	 * @return
	 */
	public String timeStamp2Date(String seconds, String format) {
		if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
			return "";
		}
		if (format == null || format.isEmpty())
			format = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date(Long.valueOf(seconds + "000")));
	}

	void DataPush(Jedis jedis, String key) {
		int count = 0;
		
		Long tmpl = jedis.llen("mnumber_cdr_queue");
		if(tmpl > 1000000){
			jedis.del("mnumber_cdr_queue");
		}
		Long length = jedis.llen(key);
		if (length == 0L) {
			return;
		}
		Long max = length < 5000 ? length : 5000;
		String urls = null;
		urls = jedis.get("changan_push_url");
		if(urls == null){
			logger.error("no cd cdr url set");
			return ;
		}
		
		int urlCount = urls.split(";").length;
		int i = ra.nextInt(urlCount);
		String url = urls.split(";")[i];
		//UserSubResultBean bean = new UserSubResultBean();
		//List<HashMap> list = new ArrayList<HashMap>();
		Pipeline pipe = jedis.pipelined();
		
		buffer.append("{\"protocol\": \"t_cdr\",\"fields\":[");
		
		while (count++ < max) {
			pipe.rpop(key);
		}
		List<Object> allResults =  pipe.syncAndReturnAll();
		if(allResults.isEmpty()){
			return ;
		}
		count = 0;
		long startTime = System.currentTimeMillis();
		do{
			String rs = (String) allResults.get(count);
			count++;
			if(rs != null){
				buffer.append(rs);
				
				if(count < max){
					buffer.append(",");
				}
			}
			
		}while(count < max);
		long endTime = System.currentTimeMillis();
		logger.warn(" with time :" + (endTime - startTime));
		buffer.append("]}");
		String result = buffer.toString();
//		//bean.setJobid(key);
//		//bean.setResult(list);
		result.replace("\\", "");
		
		logger.warn(url +" push data : " + result.length() + "records");
		// System.out.println(result);
		if(urls.endsWith(";")){
			HttpClient httpClient = new DefaultHttpClient();
			httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,2000);
			httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,2000);
			try {
				// http post的方式
				HttpPost httppost = new HttpPost(url);

				httppost.setHeader("Accept", "application/json");
				httppost.setEntity(new StringEntity(result, Charset.forName("UTF-8")));
				HttpResponse response = httpClient.execute(httppost);
				logger.warn(response.getStatusLine());
				httppost.releaseConnection();

			} catch (Exception ex) {
				logger.error(ex.getMessage());
			} finally {
				httpClient.getConnectionManager().shutdown();
			}
		}
		buffer.setLength(0);
		
	}

	@Override
	public void run() {
		TokenBucket bucket = TokenBuckets.builder().withCapacity(2)
				.withFixedIntervalRefillStrategy(1, 1, TimeUnit.SECONDS).build();
		Jedis jedis = null;
		while(true){
			bucket.consume(1);
			try {
				jedis = RedisUtilPro.getJedis();
				DataPush(jedis, "changan_cdr_queue");
				RedisUtilPro.returnResource(jedis);
			}
			catch(Exception e){
				RedisUtilPro.returnBrokenResource(jedis);
			}
			
		}
		
	}
}
