package cn.ac.iie.hy.datatrains.dbutils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class ShardedJedisUtil {

	static String confFilePath = "data-dispatcher.properties";
	static String key = "redisList";
	static ShardedJedisPool jedisPool = null;

	static {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(5000);
		poolConfig.setMaxIdle(4096);
		poolConfig.setMaxWaitMillis(20000);
		
		poolConfig.setTestOnBorrow(false);
		poolConfig.setTestOnReturn(false);

		Properties pps = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(confFilePath));
			pps.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String value = pps.getProperty(key);// ip:port
		List<JedisShardInfo> infoList = new ArrayList<JedisShardInfo>();

		String[] hosts = value.split(" ");
		for (String hostPair : hosts) {
			String ip = hostPair.split(":")[0];
			int port = Integer.parseInt(hostPair.split(":")[1]);
			infoList.add(new JedisShardInfo(ip, port));
		}
		jedisPool = new ShardedJedisPool(poolConfig, infoList);
		
	}

	public ShardedJedisUtil() {
		super();
	}

	public synchronized static ShardedJedis getSource() {
		if (jedisPool != null) {
			return jedisPool.getResource();
		} else {
			return null;
		}
	}

	public static void returnBrokenResource(ShardedJedis jedis) {
		if (jedis != null) {
			jedisPool.returnBrokenResource(jedis);

		}
	}

	public static void returnResource(ShardedJedis jedis) {
		if (jedis != null) {
			jedisPool.returnResource(jedis);
		}
	}

}
