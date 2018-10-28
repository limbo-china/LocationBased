package cn.ac.iie.hy.datastrains.task;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.hy.datatrains.dbutils.RedisUtilList;
import cn.ac.iie.hy.datatrains.dbutils.RedisUtilPro;
import cn.ac.iie.hy.datatrains.dbutils.ShardedJedisUtil;
import cn.ac.iie.hy.datatrains.handler.DataDispatchHandler;
import cn.ac.iie.hy.datatrains.metadata.SMetaData;

public class DBUpdateTask implements Runnable {

	private List<SMetaData> al = null;
	private static Properties prop = new Properties();

	Long provincePushInternal = 0L;
	static Logger logger = null;
	List<String> loadList = new ArrayList<>();
	List<String> provinceLoadList = new ArrayList<>();
	List<SMetaData> changedList = new ArrayList<>();

	static {
		try {
			prop.load(new FileInputStream("data-dispatcher.properties"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDispatchHandler.class.getName());
	}

	public DBUpdateTask(List<SMetaData> al) {
		this.al = al;
	}

	private String SData2StrCTable(SMetaData smd, long newLoadTime,
			long newCenterPushTime, long newProvincePushTime,
			long newValidTypeLoadTime) {
		return smd.toRedisString() + newLoadTime + ";" + newCenterPushTime
				+ ";" + newProvincePushTime + ";" + newValidTypeLoadTime + ";";

	}

	private String SData2StrLoadQueue(SMetaData smd) {
		String type = isValidType(smd) ? "1" : "0";
		return smd.toRedisString() + type + ";";

	}

	private String SData2Str2PushQueue(SMetaData smd) {
		return smd.toRedisString();
	}

	private boolean isValidType(SMetaData smd) {
		if (smd.getCdrType() == 18 || smd.getCdrType() == 19
				|| smd.getCdrType() == 20)
			return false;
		return true;
	}

	private void dataPushUpdate(SMetaData smd, String profile, Jedis jedis) {

		String urls = profile.split(";")[3];
		String jobIDs = profile.split(";")[2];
		if (urls.split(",").length != jobIDs.split(",").length) {
			logger.error("pro error : " + profile);
		}
		for (int i = 0; i < jobIDs.split(",").length; i++) {
			String jobID = jobIDs.split(",")[i];
			String url = urls.split(",")[i];
			String result = smd.getImsi() + ";" + smd.getImei() + ";"
					+ smd.getMsisdn() + ";" + smd.getLac() + ";" + smd.getCi()
					+ ";" + smd.getUli() + ";" + smd.getTimestamp() + ";" + url
					+ ";" + smd.getRegionCode() + ";" + smd.getHomeCode();
			jedis.lpush("JOBID_" + jobID, result);
		}

	}

	private boolean dbUpdateTaskRedis(SMetaData smd, String value,
			Jedis configJedis, ShardedJedisPipeline cachePipe, Jedis listJedis) {
		String key = smd.getImsi();

		if (key.length() != 15) {
			return false;
		}
		long lastLoadTime = 0L;
		long lastProvincePushTime = 0L;
		long lastValidTypeLoadTime = 0L;

		if (value != null) {
			try {
				int length = value.split(";").length;
				lastLoadTime = Long.parseLong(value.split(";")[11]);
				lastProvincePushTime = Long.parseLong(value.split(";")[13]);
				lastValidTypeLoadTime = (length > 14) ? Long.parseLong(value
						.split(";")[14]) : 0L;
			} catch (Exception e) {
				logger.error(e);
			}

			long newLoadTime = lastLoadTime;
			long newProvincePushTime = lastProvincePushTime;
			long newValidTypeLoadTime = lastValidTypeLoadTime;
			long localTime = System.currentTimeMillis() / 1000;
			boolean ifChange = false;

			if (provincePushInternal != 0) {
				if (lastProvincePushTime == 0L
						|| localTime - lastProvincePushTime > provincePushInternal) {
					provinceLoadList.add(SData2Str2PushQueue(smd));

					newProvincePushTime = localTime;
					ifChange = true;
				}
			}

			String uli = value.split(";")[6];
			if (!smd.getUli().equals(uli) || ifChange) {
				cachePipe.set(
						key,
						SData2StrCTable(smd, newLoadTime, 0L,
								newProvincePushTime, newValidTypeLoadTime));

				changedList.add(smd);

			}
			int loadInterval = 60 * Integer.valueOf(prop
					.getProperty("loadInterval"));
			if (smd.getTimestamp() - lastLoadTime > loadInterval) {
				if (isValidType(smd))
					newValidTypeLoadTime = smd.getTimestamp();
				newLoadTime = smd.getTimestamp();
				cachePipe.set(
						key,
						SData2StrCTable(smd, newLoadTime, 0L,
								newProvincePushTime, newValidTypeLoadTime));
				loadList.add(SData2StrLoadQueue(smd));

			} else {
				if (isValidType(smd)
						&& (smd.getTimestamp() - lastValidTypeLoadTime > loadInterval)) {
					newValidTypeLoadTime = smd.getTimestamp();
					newLoadTime = smd.getTimestamp();
					cachePipe.set(
							key,
							SData2StrCTable(smd, newLoadTime, 0L,
									newProvincePushTime, newValidTypeLoadTime));
					loadList.add(SData2StrLoadQueue(smd));
				}
			}

		}
		return true;
	}

	@Override
	public void run() {

		Jedis configJedis = null;
		ShardedJedis cacheJedis = null;
		ShardedJedis wCacheJedis = null;
		Jedis listJedis = null;

		long startTime = System.currentTimeMillis();

		try {

			cacheJedis = ShardedJedisUtil.getSource();
			wCacheJedis = ShardedJedisUtil.getSource();
			configJedis = RedisUtilPro.getJedis();
			listJedis = RedisUtilList.getJedis();

			ShardedJedisPipeline cachePipe = cacheJedis.pipelined();

			HashMap<String, SMetaData> imsiMap = new HashMap<>();

			if (configJedis.get("ProvincePushInternal") != null) {
				provincePushInternal = Long.parseLong(configJedis
						.get("ProvincePushInternal"));
			}

			for (Iterator<SMetaData> it = al.iterator(); it.hasNext();) {
				SMetaData smd = it.next();
				if (imsiMap.containsKey(smd.getImsi())) {
					continue;
				} else {
					cachePipe.get(smd.getImsi());
					imsiMap.put(smd.getImsi(), smd);
				}

			}
			List<Object> resp = cachePipe.syncAndReturnAll();
			ShardedJedisPipeline wCachePipe = wCacheJedis.pipelined();

			for (Iterator<Object> it = resp.iterator(); it.hasNext();) {
				String rs = (String) it.next();
				if (rs != null && rs.split(";").length > 2) {
					SMetaData smd = imsiMap.get(rs.split(";")[0]);
					dbUpdateTaskRedis(smd, rs, configJedis, wCachePipe,
							listJedis);
					imsiMap.remove(rs.split(";")[0]);
				}
			}

			if (configJedis.llen("loadqueue") > 1000000) {
				configJedis.del("loadqueue");
				logger.error("loadqueue full");
			}

			if (configJedis.llen("PronvincePushQueue") > 1000000) {
				configJedis.del("PronvincePushQueue");
			}

			Pipeline loadPipe = configJedis.pipelined();

			for (String data : provinceLoadList) {
				loadPipe.rpush("PronvincePushQueue", data);
			}

			for (String data : loadList) {
				loadPipe.rpush("loadqueue", data);
			}

			for (SMetaData smd : imsiMap.values()) {
				long newValidTypeLoadTime = isValidType(smd) ? smd
						.getTimestamp() : 0L;
				wCachePipe.set(
						smd.getImsi(),
						SData2StrCTable(smd, smd.getTimestamp(), 0L, 0L,
								newValidTypeLoadTime));
				loadPipe.rpush("loadqueue", SData2StrLoadQueue(smd));
			}

			wCachePipe.sync();
			loadPipe.sync();

			Pipeline configPipe = listJedis.pipelined();

			Map<String, SMetaData> changedMap = new HashMap<>();
			for (SMetaData smd : changedList) {
				configPipe.get("SUB_MSISDN_" + smd.getMsisdn());
				if (smd.getMsisdn().length() >= 11) {
					changedMap.put(smd.getMsisdn(), smd);
				}

			}

			List<Object> sublist = configPipe.syncAndReturnAll();
			for (Object obj : sublist) {
				if (obj != null) {
					String profile = obj.toString();
					dataPushUpdate(changedMap.get(profile.split(";")[1]),
							profile, configJedis);
				}
			}

			RedisUtilPro.returnResource(configJedis);
			ShardedJedisUtil.returnResource(cacheJedis);
			ShardedJedisUtil.returnResource(wCacheJedis);
			RedisUtilList.returnResource(listJedis);
			long endTime = System.currentTimeMillis();
			logger.info(imsiMap.size() + " record with time :"
					+ (endTime - startTime));
		} catch (Exception e) {
			RedisUtilPro.returnBrokenResource(configJedis);
			ShardedJedisUtil.returnBrokenResource(cacheJedis);
			ShardedJedisUtil.returnBrokenResource(wCacheJedis);
			RedisUtilList.returnBrokenResource(listJedis);
			e.printStackTrace();
			logger.error(e);
		}

	}
}
