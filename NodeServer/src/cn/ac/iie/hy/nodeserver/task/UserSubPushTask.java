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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
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
import cn.ac.iie.hy.nodeserver.data.BSDataMap;
import cn.ac.iie.hy.nodeserver.data.UserSubResultBean;
import cn.ac.iie.hy.nodeserver.dbutils.RedisUtilPro;
import cn.ac.iie.hy.nodeserver.server.DataNodeServer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class UserSubPushTask implements Runnable {

	public UserSubPushTask() {
		super();
	}

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

	void DataPush(Jedis jedis, String key) {
		Long count = 0L;
		Long length = jedis.llen(key);
		if (length == 0L) {
			return;
		}
		Long max = length < 3000 ? length : 3000;
		String url = null;

		UserSubResultBean bean = new UserSubResultBean();
		List<HashMap> list = new ArrayList<HashMap>();

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
				
				String source = obj.toString();
				HashMap record = new HashMap();
				record.put("imsi", source.split(";")[0]);
				record.put("imei", source.split(";")[1]);
				record.put("msisdn", source.split(";")[2]);
				record.put("lac", source.split(";")[3]);
				record.put("ci", source.split(";")[4]);
				record.put("uli", source.split(";")[5]);
				record.put("regioncode", BSDataMap.getRegion(source.split(";")[5]));
				record.put("lngi", BSDataMap.getLngi(source.split(";")[5]));
				record.put("lati", BSDataMap.getLati(source.split(";")[5]));
				//record.put("homecode", source.split(";")[9]);
				record.put("updatetime", timeStamp2Date(source.split(";")[6] + "", null));
				list.add(record);
				url = source.split(";")[7];
			}

			bean.setJobid(key);
			bean.setResult(list);
			Gson gson = new Gson();
			String result = gson.toJson(bean);
			logger.warn("push data : " + url + "  " + list.size());
			// System.out.println(result);
			
			
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

	@Override
	public void run() {
		TokenBucket bucket = TokenBuckets.builder().withCapacity(1)
				.withFixedIntervalRefillStrategy(1, 1, TimeUnit.SECONDS).build();
		
		while(true){
			bucket.consume(1);
			Jedis jedis = RedisUtilPro.getJedis();
			Set<String> keys = jedis.keys("JOBID_*");
			for (Iterator<String> it = keys.iterator(); it.hasNext();) {
				String key = it.next();
				if(key.startsWith("JOBID_1")||key.startsWith("JOBID_5")){
					DataPush(jedis, key);
				}
			}

			RedisUtilPro.returnResource(jedis);
			
		}
		
	}

}
