package cn.ac.iie.hy.nodeserver.task;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;

import com.google.gson.Gson;

import cn.ac.iie.hy.nodeserver.config.Configuration;
import cn.ac.iie.hy.nodeserver.config.ConfigurationException;
import cn.ac.iie.hy.nodeserver.data.BSDataMap;
import cn.ac.iie.hy.nodeserver.data.UserSubQueryBean;
import cn.ac.iie.hy.nodeserver.dbutils.RedisUtilList;
import cn.ac.iie.hy.nodeserver.dbutils.RedisUtilPro;
import redis.clients.jedis.Jedis;

/**
 * 
 * @zhangyu
 */
public class UserSubQueryTask {

	private static Configuration conf = null;

	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(UserSubQueryTask.class.getName());
		String configurationFileName = "data-pro.properties";
		conf = Configuration.getConfiguration(configurationFileName);
		if (conf == null) {
			logger.error("reading " + configurationFileName + " is failed.");
		}
	}

	public UserSubQueryTask() {
	}

	public String querySubUserResult(String type, String index, String token) {
		Jedis jedis = RedisUtilList.getJedis();
		String suffix = "SUB_";
		if (type.equals("imsi")) {
			suffix += "IMSI_";
		} else if (type.equals("imei")) {
			suffix += "IMEI_";
		} else if (type.equals("msisdn")) {
			suffix += "MSISDN_";
		} else {
			suffix = null;
		}
		if (suffix != null) {
			String proKey = suffix + index;
			String pro = jedis.get(proKey);
			if(pro == null){
				RedisUtilList.returnResource(jedis);
				return null;
			}
			if(!pro.split(";")[4].contains(token)){
				RedisUtilList.returnResource(jedis);
				return null;
			}
			String v = jedis.get("POSITION_" + index);
			RedisUtilList.returnResource(jedis);
			if (v == null) {
				return null;
			} else {
				UserSubQueryBean usqb = new UserSubQueryBean();
				usqb.setStatus(0);
				usqb.setImsi(v.split(";")[0]);
				usqb.setImei(v.split(";")[1]);
				usqb.setMsisdn(v.split(";")[2]);
				usqb.setLac(v.split(";")[3]);
				usqb.setCi(v.split(";")[4]);
				usqb.setUli(v.split(";")[5]);
				usqb.setLngi(Double.parseDouble(BSDataMap.getLngi(v.split(";")[5])));
				usqb.setLati(Double.parseDouble(BSDataMap.getLati(v.split(";")[5])));
				
				usqb.setTime(v.split(";")[6]);
				if(v.split(";").length == 10){
					usqb.setRegionCode(v.split(";")[8]);
					usqb.setHomeCode(v.split(";")[9]);
				}
				else{
					usqb.setRegionCode("");
					usqb.setHomeCode("");
				}
				
				Gson gson = new Gson();
				String jsonResult = gson.toJson(usqb);
				return jsonResult;
			}
		} else {
			return null;
		}

	}

}
