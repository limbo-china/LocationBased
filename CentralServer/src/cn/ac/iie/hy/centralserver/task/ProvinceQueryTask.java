package cn.ac.iie.hy.centralserver.task;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.google.gson.Gson;

import cn.ac.iie.hy.centralserver.config.Configuration;
import cn.ac.iie.hy.centralserver.data.ProvinceQueryResult;
import cn.ac.iie.hy.centralserver.dbutils.ShardedJedisUtil;
import cn.ac.iie.hy.centralserver.dbutils.JedisUtilMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;


public class ProvinceQueryTask {

	private static Configuration conf = null;
	static Logger logger = null;
	// private static Cache<String, String> basicCache = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(ProvinceQueryTask.class.getName());
		String configurationFileName = "data-pro.properties";
		conf = Configuration.getConfiguration(configurationFileName);
		if (conf == null) {
			logger.error("reading " + configurationFileName + " is failed.");
		}
//		FileInputStream inputStream = null;
//		Scanner sc = null;
//		try {
//			inputStream = new FileInputStream(mapFile);
//			sc = new Scanner(inputStream, "UTF-8");
//			 while (sc.hasNextLine()) {
//			    String line = sc.nextLine();
//			    if(line == null || line.isEmpty()){
//			    	continue;
//			    }
//			    if(line.split(",").length == 2){
//			    	codeMap.put(line.split(",")[0].substring(0, 2), line.split(",")[1]);
//			    }
//			 }
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//		finally {
//		    if (inputStream != null) {
//		        try {
//					inputStream.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//		    }
//		    if (sc != null) {
//		        sc.close();
//		    }
//		}
	}
	
	private String timeStamp2Date(String seconds, String format) {
		if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
			return "";
		}
		if (format == null || format.isEmpty())
			format = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date(Long.valueOf(seconds + "000")));
	}

	public ProvinceQueryTask() {
	}
/*
 * 瀹佸 鍜屽浗瀹朵腑蹇冧竴鏍峰叆鍙傚拰鍑哄弬涓�鏍�
 */
	public String queryProvinceResult(String type, String index) {
		ShardedJedis jedis = JedisUtilMap.getSource();
		ShardedJedis jedisCluster = ShardedJedisUtil.getSource();
		String value = null;

		if (type.equals("imsi")) { 
			value = jedisCluster.get(index);
		} else if (type.equals("msisdn")) {
			String imsi = jedis.get(index);
			if (imsi != null) {
				value = jedisCluster.get(imsi);
			}
		} else {
			value = null;
		}

		JedisUtilMap.returnResource(jedis);
		ShardedJedisUtil.returnResource(jedisCluster);

		if (value != null) {

			ProvinceQueryResult pdr = new ProvinceQueryResult();
			pdr.setStatus(0);
			pdr.setImsi(value.split(",")[0]);
			pdr.setImei(value.split(",")[1]);
			pdr.setMsisdn(value.split(",")[2]);
			pdr.setProvince(value.split(",")[3].substring(0, 2) + "0000");
			pdr.setLastUpdateTime(timeStamp2Date(value.split(",")[10], null));
			Gson gson = new Gson();
			String jsonResult = gson.toJson(pdr);
			return jsonResult;

		} else {
			return null;
		}

	}
	
}