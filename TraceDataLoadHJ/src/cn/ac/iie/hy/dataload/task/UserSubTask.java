package cn.ac.iie.hy.dataload.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.dataload.config.ConfigurationException;
import cn.ac.iie.hy.dataload.config.Configuration;
import cn.ac.iie.hy.dataload.dbutils.RedisUtilList;
import cn.ac.iie.hy.dataload.dbutils.RedisUtilPro;
import cn.ac.iie.hy.dataload.dbutils.RedisUtilList_t;
import cn.ac.iie.hy.dataload.metadata.SMetaData;
import redis.clients.jedis.Jedis;
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

public class UserSubTask {

	private List<Object> al = new ArrayList<Object>();
	private static Configuration conf = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(UserSubTask.class.getName());
		String configurationFileName = "data-load.properties";
		conf = Configuration.getConfiguration(configurationFileName);
		if (conf == null) {
			logger.error("reading " + configurationFileName + " is failed.");
		}
	}

	public UserSubTask(List<Object> al) {

		this.al = al;
	}	
	
	private void dataPushUpdate(SMetaData smd, String profile,Jedis jedis) {

		String url = profile.split(";")[3];
		String jobID = profile.split(";")[2];

		String result = smd.getImsi() + ";" + smd.getImei() + ";" + smd.getMsisdn() + ";" + smd.getLac() + ";"
				+ smd.getCi() + ";" + smd.getUli() + ";" + smd.getTimestamp() + ";" + url + ";" + smd.getRegionCode() + ";" + smd.getHomeCode();
		jedis.lpush("JOBID_" + jobID, result);
	}

	public void send() {
		
		//Jedis jedis = RedisUtilPro.getJedis();
		Jedis jedis = RedisUtilList.getJedis();
		Jedis ljedis_t = RedisUtilList_t.getJedis();

		int count =0;
		for (Iterator<Object> it = al.iterator(); it.hasNext();) {
			
			
			String data = (String) it.next();
			
			if(data == null||data.isEmpty()){
				continue;
			}
			
			String[] items = data.split(";");
			
			if(items.length < 11){
				continue;
			}
			
			SMetaData smd = new SMetaData();
			smd.setImsi(data.split(";")[0]);
			smd.setImei(data.split(";")[1]);
			smd.setMsisdn(data.split(";")[2]);
			smd.setRegionCode(data.split(";")[3]);
			smd.setLac(data.split(";")[4]);
			smd.setCi(data.split(";")[5]);
			smd.setUli(data.split(";")[6]);
			smd.setHomeCode(data.split(";")[7]);
			smd.setTimestamp(Long.parseLong(data.split(";")[10]));
			
			String value = ljedis_t.get("SUB_MSISDN_" + smd.getMsisdn());
			
			if (value != null){
				System.out.println("JOBID_"+value.split(";")[2]);
				//dataPushUpdate(smd, value, jedis);
				count++;
			}
						
		}
		//logger.info("send " + count + "Sub to queue successfully!");
		
		RedisUtilList_t.returnResource(ljedis_t);
		RedisUtilList_t.returnResource(jedis);
		
	}

}
