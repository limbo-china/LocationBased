package cn.ac.iie.hy.datastrains.task;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;

import cn.ac.iie.hy.datatrains.config.ConfigurationException;
import cn.ac.iie.hy.datatrains.config.Configuration;
import cn.ac.iie.hy.datatrains.dbutils.RedisUtilList;
import cn.ac.iie.hy.datatrains.dbutils.RedisUtilPro;
import cn.ac.iie.hy.datatrains.handler.DataDispatchReceiver;
import cn.ac.iie.hy.datatrains.metadata.SMetaData;
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

public class UserSubTask implements Runnable {

	private List<SMetaData> al = null;
	private static Configuration conf = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDispatchReceiver.class.getName());
		String configurationFileName = "data-dispatcher.properties";
		conf = Configuration.getConfiguration(configurationFileName);
		if (conf == null) {
			logger.error("reading " + configurationFileName + " is failed.");
		}
	}

	public UserSubTask(List<SMetaData> al) {

		this.al = al;
//		if (mongoManagerDetail == null) {
//
//			try {
//				String mongoServerDetail = conf.getString("mongoServerDetail", "");
//				if (mongoServerDetail.isEmpty()) {
//					logger.error("definition mongoServerDetail is not found in ");
//				}
//				int mongoPortDetail = conf.getInt("mongoPortDetail", -1);
//				if (mongoPortDetail == -1) {
//					logger.error("definition mongoPortDetail is not found in ");
//				}
//				String mongoDBNameDetail = conf.getString("mongoDBNameDetail", "");
//				if (mongoDBNameDetail.isEmpty()) {
//					logger.error("definition mongoDBNameDetail is not found in ");
//				}
//				String mongoTableNameDetail = conf.getString("mongoTableNameDetail", "");
//				if (mongoTableNameDetail.isEmpty()) {
//					logger.error("definition mongoTableNameDetail is not found in ");
//				}
//				mongoManagerDetail = new MongoSMetaDataUtils(mongoServerDetail, mongoPortDetail, mongoDBNameDetail,
//						mongoTableNameDetail);
//
//			} catch (ConfigurationException e) {
//				e.printStackTrace();
//			}
//		}
	}

//	private Boolean positionUpdate(SMetaData smd) {
//		Document dresult = mongoManagerDetail.findOneObj(smd);
//		if (dresult != null) {
//			SMetaData data = mongoManagerDetail.pauseDocObj(dresult);
//			if (data.getLac().equals(smd.getLac()) && data.getCi().equals(smd.getCi())) {
//				return false;
//			} else {
//				mongoManagerDetail.UpdateWithDoc(dresult, smd);
//				return true;
//			}
//		} else {
//			mongoManagerDetail.save(smd);
//			return true;
//		}
//	}
	
	
	private void dataPushUpdate(SMetaData smd, String profile, Jedis jedis, Jedis ljedis) {

		String url = profile.split(";")[3];
		String jobID = profile.split(";")[2];

		String result = smd.getImsi() + ";" + smd.getImei() + ";" + smd.getMsisdn() + ";" + smd.getLac() + ";"
				+ smd.getCi() + ";" + smd.getUli() + ";" + smd.getTimestamp() + ";" + url + ";" + smd.getRegionCode() + ";" + smd.getHomeCode();
		jedis.lpush("JOBID_" + jobID, result);
		ljedis.set("POSITION_" + smd.getMsisdn(), result);

	}

	@Override
	public void run() {
		
		Jedis jedis = RedisUtilPro.getJedis();
		Jedis ljedis = RedisUtilList.getJedis();

//		Long l = jedis.llen("user_pro_list");
//		if (l == 0L) {
//			RedisUtilPro.returnResource(jedis);
//			return;
//		}
//
//		HashMap<String, String> pro = new HashMap<>();
//		List<String> detailUserProfileList = jedis.lrange("user_pro_list", 0, -1);
//		for (Iterator<String> it = detailUserProfileList.iterator(); it.hasNext();) {
//			String profile = it.next();
//			String key = profile.split(";")[1];
//			String value = profile;
//			pro.put(key, value);
//		}
		
		for (Iterator<SMetaData> it = al.iterator(); it.hasNext();) {
			SMetaData smd = it.next();
			String profile = ljedis.get("SUB_MSISDN_" + smd.getMsisdn());
//			if (pro.containsKey(smd.getImsi())) {
//				profile = pro.get(smd.getImsi());
//			}
//			if (pro.containsKey(smd.getMsisdn())) {
//				profile = pro.get(smd.getMsisdn());
//			}
//			if (pro.containsKey(smd.getImei())) {
//				profile = pro.get(smd.getImei());
//			}
			
			if (profile != null) {
				String info = ljedis.get("POSITION_" + smd.getMsisdn());
				if(info != null && info.split(";").length > 5){
					String olduli = info.split(";")[5];
					if(!olduli.equals(smd.getUli())){
						//update
						//push
						dataPushUpdate(smd, profile, jedis, ljedis);
					}
					
				}
				else{
					//update
					//push
					dataPushUpdate(smd, profile, jedis, ljedis);
				}
//				if (positionUpdate(smd)) {
//					dataPush(smd, profile, jedis);
//				}
			}
		}
		
		RedisUtilPro.returnResource(jedis);
		RedisUtilList.returnResource(ljedis);
		
	}

}
