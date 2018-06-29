package cn.ac.iie.hy.nodeserver.task;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.gson.Gson;

import cn.ac.iie.hy.nodeserver.tokenbucket.TokenBucket;
import cn.ac.iie.hy.nodeserver.tokenbucket.TokenBuckets;
import cn.ac.iie.hy.nodeserver.dbutils.RedisUtilPro;
import cn.ac.iie.hy.nodeserver.dbutils.UliRedisUlit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class UserSubPushTask implements Runnable {

	private String key = null;
	
	public UserSubPushTask(String key) {
		super();
		this.key = key;
	}

	static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(UserSubPushTask.class.getName());
    }
	/**
	 * 鏃堕棿鎴宠浆鎹㈡垚鏃ユ湡鏍煎紡瀛楃涓�
	 * 
	 * @param seconds
	 *            绮剧‘鍒扮鐨勫瓧绗︿覆
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
		if(seconds.contains("-")){
			sdf.format(new Date(System.currentTimeMillis()));
		}
		
		return sdf.format(new Date(Long.valueOf(seconds + "000")));
	}
	
	public boolean isNumeric(String str){
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);
		if(!isNum.matches()){
			return false;
		}
		return true;
	}

	Gson gson = new Gson();
	
	void DataPush(Jedis jedis, String key) {
		Long count = 0L;
		Long length = jedis.llen(key);
		if (length == 0L) {
			return;
		}
		Long max = length < 3000 ? length : 3000;
		String url = null;

		List<HashMap<String, String>> list = new ArrayList<HashMap<String, String>>();

		HttpClient httpClient = new DefaultHttpClient();
		
		httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,5000);
		httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,5000);
		try {
			Pipeline pipe = jedis.pipelined();
			while (count++ < max) {
				pipe.lpop(key);
			}
			List<Object> resp = pipe.syncAndReturnAll();
			for(Object obj : resp){
			//while (count++ < max) {
				//if(obj!=null){
				String source = obj.toString();
				HashMap<String, String> record = new HashMap<String, String>();
				record.put("imsi", source.split(";")[0]);
				record.put("imei", source.split(";")[1]);
				record.put("msisdn", source.split(";")[2]);
				record.put("lac", source.split(";")[3]);
				record.put("ci", source.split(";")[4]);
				record.put("uli", source.split(";")[5]);
				if(source.split(";").length == 10){
					record.put("homecode", source.split(";")[9]);
				}
				else{
					record.put("homecode", "");
				}
				//record.put("homecode", source.split(";")[9]);
				String v = UliRedisUlit.getUliRedisResource().get(source.split(";")[5]);
				if(v != null){
					record.put("regioncode", v.split(",")[8]);
					record.put("lngi", v.split(",")[1]);
					record.put("lati", v.split(",")[2]);
					record.put("province", v.split(",")[3]);
					record.put("city", v.split(",")[4]);
					record.put("district", v.split(",")[5]);
					record.put("baseinfo", v.split(",")[7]);
					record.put("updatetime", timeStamp2Date(source.split(";")[6], null));
				}else{
					record.put("regioncode", "");
					record.put("lngi", "0.0");
					record.put("lati", "0.0");
					record.put("province", "");
					record.put("city", "");
					record.put("district", "");
					record.put("baseinfo", "");
					record.put("updatetime", timeStamp2Date(source.split(";")[6], null));
				}
				
				list.add(record);
				
				url = source.split(";")[7];
				
				//logger.warn(record.get("baseinfo"));
				
			}
			//url = "http://10.2.8.77:8080/getLbsData/";
			String result = gson.toJson(list);
			logger.warn("push data : " + url + "  " + list.size());
			//System.out.println(result);
			
			
			// http post鐨勬柟寮�
			HttpPost httppost = new HttpPost(url);
			
			httppost.setHeader("Accept", "application/json");
			httppost.setEntity(new StringEntity(result, Charset.forName("UTF-8")));
			HttpResponse response;
		
			response = httpClient.execute(httppost);
			logger.warn(response.getStatusLine());
			
			httppost.releaseConnection();

		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
		} finally {
			httpClient.getConnectionManager().shutdown();
		}

	}

	@Override
	public void run() {
		TokenBucket bucket = TokenBuckets.builder().withCapacity(1)
				.withFixedIntervalRefillStrategy(1, 1, TimeUnit.SECONDS).build();
		
		while(true){
			bucket.consume(1);
			Jedis jedis = RedisUtilPro.getJedis();
			DataPush(jedis, key);

			RedisUtilPro.returnResource(jedis);
			
		}
		
	}

}
