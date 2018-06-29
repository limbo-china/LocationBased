package cn.ac.iie.hy.nodeserver.task;

import java.nio.charset.Charset;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.gson.Gson;

import cn.ac.iie.hy.nodeserver.data.BSDataMap;
import cn.ac.iie.hy.nodeserver.data.UserSubResultBean;
import cn.ac.iie.hy.nodeserver.dbutils.RedisUtilPro;
import cn.ac.iie.hy.nodeserver.server.DataNodeServer;
import cn.ac.iie.hy.nodeserver.tokenbucket.TokenBucket;
import cn.ac.iie.hy.nodeserver.tokenbucket.TokenBuckets;
import redis.clients.jedis.Jedis;

public class CSCSRPushTask implements Runnable{

	static Logger logger = null;

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
		
		Long count = 0L;
		Long length = jedis.llen(key);
		if (length == 0L) {
			return;
		}
		Long max = length < 1000 ? length : 1000;
		String url = null;
		url = jedis.get("d_cs_url");
		if(url == null){
			logger.error("no cs cdr url set");
			return ;
		}
		//UserSubResultBean bean = new UserSubResultBean();
		//List<HashMap> list = new ArrayList<HashMap>();
		String result = "{\"protocol\": \"t_cs_cdr\",\"fields\":[";
		while (count++ < max) {
			String source = jedis.rpop(key);
			result += source;
			if(count < max){
				result += ",";
			}
		}
		result += "]}";
		//bean.setJobid(key);
		//bean.setResult(list);
		result = result.replace("\\", "");
		logger.info("pushdata:" + result);
		//logger.info("push data : " + url + "  " + result);
		// System.out.println(result);
		HttpClient httpClient = new DefaultHttpClient();
		try {
			// http post的方式
			HttpPost httppost = new HttpPost(url);

			httppost.setHeader("Accept", "application/json");
			httppost.setEntity(new StringEntity(result, Charset.forName("UTF-8")));
			HttpResponse response = httpClient.execute(httppost);
			System.out.println(response.getStatusLine());
			httppost.releaseConnection();

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}

	}

	@Override
	public void run() {
		TokenBucket bucket = TokenBuckets.builder().withCapacity(1)
				.withFixedIntervalRefillStrategy(1, 1, TimeUnit.SECONDS).build();
		Jedis jedis = null;
		while(true){
			bucket.consume(1);
			try {
				jedis = RedisUtilPro.getJedis();
				DataPush(jedis, "cs_cdr_queue");

				RedisUtilPro.returnResource(jedis);
			}
			catch (Exception e) {
				RedisUtilPro.returnBrokenResource(jedis);
			}
		}
		
	}
	
}
