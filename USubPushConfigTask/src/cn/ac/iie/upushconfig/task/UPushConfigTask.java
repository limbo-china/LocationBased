package cn.ac.iie.upushconfig.task;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.upushconfig.config.ConfigUtil;
import cn.ac.iie.upushconfig.db.RedisUtil;
import cn.ac.iie.upushconfig.log.LogUtil;
import cn.ac.iie.upushcongfig.bean.ResultResponse;

import com.google.gson.Gson;

public class UPushConfigTask {

	private List<String> msisdns;
	private static String redisIp = ConfigUtil.getString("configRedisIp");
	private static String jobId = ConfigUtil.getString("jobId");
	private static String pushUrl = ConfigUtil.getString("pushUrl");

	public UPushConfigTask(List<String> msisdns) {
		this.msisdns = msisdns;
	}

	public String exec() {

		LogUtil.info("start configing " + msisdns.size() + " to Redis "
				+ redisIp);

		Gson gson = new Gson();
		ResultResponse response = new ResultResponse();
		try {
			addMsisdn2Redis(msisdns);
			response.setStatus(0, msisdns.size());
			LogUtil.info("config " + msisdns.size() + " to Redis finished");
		} catch (Exception e) {
			response.setStatus(1, 0);
			LogUtil.info("config " + msisdns.size() + " to Redis failed for "
					+ e.getMessage());
		}

		return gson.toJson(response);
	}

	private void addMsisdn2Redis(List<String> msisdns) {

		String dateTime = getDateTime();
		ShardedJedis jedis = RedisUtil.getJedis("configRedisIp");
		ShardedJedisPipeline pipelined = jedis.pipelined();

		for (String msisdn : msisdns) {
			pipelined.set("SUB_MSISDN_" + msisdn, "msisdn;" + msisdn + ";"
					+ jobId + ";" + pushUrl + ";xxx;" + dateTime);
		}

		pipelined.sync();
		RedisUtil.returnJedis(jedis, "configRedisIp");

	}

	private String getDateTime() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
		return format.format(new Date());
	}
}
