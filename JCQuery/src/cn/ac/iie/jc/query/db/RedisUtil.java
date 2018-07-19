package cn.ac.iie.jc.query.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import cn.ac.iie.jc.query.config.ConfigUtil;

public class RedisUtil {

	private static JedisPoolConfig poolConfig = new JedisPoolConfig();
	private static HashMap<String, ShardedJedisPool> jedisPoolMap = null;
	static {
		jedisPoolMap = new HashMap<String, ShardedJedisPool>();
		configPool();
	}

	private RedisUtil() {
	}

	private static void configPool() {
		poolConfig.setMaxTotal(2048);
		poolConfig.setMaxIdle(4096);
		poolConfig.setMaxWaitMillis(20000);
		poolConfig.setTestOnBorrow(false);
		poolConfig.setTestOnReturn(false);
	}

	public synchronized static ShardedJedis getJedis(String para) {

		if (jedisPoolMap.get(para) == null) {
			List<JedisShardInfo> infoList = getInfoList(para);
			ShardedJedisPool jedisPool = new ShardedJedisPool(poolConfig,
					infoList);
			jedisPoolMap.put(para, jedisPool);
		}
		return getResource(para);
	}

	public static ShardedJedis getJedisByIpList(String ipList) {
		List<JedisShardInfo> infoList = new ArrayList<JedisShardInfo>();

		String[] hosts = ipList.split(" ");
		if (jedisPoolMap.get(ipList) == null) {
			for (String hostPair : hosts) {
				String ip = hostPair.split(":")[0];
				int port = Integer.parseInt(hostPair.split(":")[1]);
				infoList.add(new JedisShardInfo(ip, port));
			}
			ShardedJedisPool jedisPool = new ShardedJedisPool(poolConfig,
					infoList);
			jedisPoolMap.put(ipList, jedisPool);
		}
		if (jedisPoolMap.get(ipList) != null)
			return jedisPoolMap.get(ipList).getResource();

		return null;
	}

	private static List<JedisShardInfo> getInfoList(String para) {
		List<JedisShardInfo> infoList = new ArrayList<JedisShardInfo>();

		String[] hosts = ConfigUtil.getString(para).split(" ");
		for (String hostPair : hosts) {
			String ip = hostPair.split(":")[0];
			int port = Integer.parseInt(hostPair.split(":")[1]);
			infoList.add(new JedisShardInfo(ip, port));
		}
		return infoList;
	}

	private static ShardedJedis getResource(String para) {
		ShardedJedisPool jedisPool = jedisPoolMap.get(para);
		if (jedisPool != null)
			return jedisPool.getResource();
		else
			return null;
	}

	public static void returnBrokenJedis(ShardedJedis jedis, String para) {
		ShardedJedisPool jedisPool = jedisPoolMap.get(para);
		if (jedis != null && jedisPool != null)
			jedisPool.returnBrokenResource(jedis);
	}

	public static void returnJedis(ShardedJedis jedis, String para) {
		ShardedJedisPool jedisPool = jedisPoolMap.get(para);
		if (jedis != null && jedisPool != null)
			jedisPool.returnResource(jedis);
	}

}
