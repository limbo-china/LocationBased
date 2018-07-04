package cn.ac.iie.jc.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import cn.ac.iie.jc.config.ConfigUtil;

public class RedisUtil {

	private String parameter;
	private static JedisPoolConfig poolConfig = new JedisPoolConfig();
	private static HashMap<String, ShardedJedisPool> jedisPoolMap = null;
	static {
		jedisPoolMap = new HashMap<String, ShardedJedisPool>();
		configPool();
	}

	public RedisUtil(String para) {
		this.parameter = para;
	}

	private static void configPool() {
		poolConfig.setMaxTotal(2048);
		poolConfig.setMaxIdle(4096);
		poolConfig.setMaxWaitMillis(20000);
		poolConfig.setTestOnBorrow(false);
		poolConfig.setTestOnReturn(false);
	}

	public ShardedJedis getJedis() {

		if (jedisPoolMap.get(parameter) == null) {
			List<JedisShardInfo> infoList = getInfoList();
			ShardedJedisPool jedisPool = new ShardedJedisPool(poolConfig,
					infoList);
			jedisPoolMap.put(parameter, jedisPool);
		}

		return getResource();
	}

	private List<JedisShardInfo> getInfoList() {
		List<JedisShardInfo> infoList = new ArrayList<JedisShardInfo>();

		String[] hosts = ConfigUtil.getString(parameter).split(" ");
		for (String hostPair : hosts) {
			String ip = hostPair.split(":")[0];
			int port = Integer.parseInt(hostPair.split(":")[1]);
			infoList.add(new JedisShardInfo(ip, port));
		}
		return infoList;
	}

	private synchronized ShardedJedis getResource() {
		ShardedJedisPool jedisPool = jedisPoolMap.get(parameter);
		if (jedisPool != null)
			return jedisPool.getResource();
		else
			return null;
	}

	public void returnBrokenJedis(ShardedJedis jedis) {
		ShardedJedisPool jedisPool = jedisPoolMap.get(parameter);
		if (jedis != null && jedisPool != null)
			jedisPool.returnBrokenResource(jedis);
	}

	public void returnJedis(ShardedJedis jedis) {
		ShardedJedisPool jedisPool = jedisPoolMap.get(parameter);
		if (jedis != null && jedisPool != null)
			jedisPool.returnResource(jedis);
	}

}
