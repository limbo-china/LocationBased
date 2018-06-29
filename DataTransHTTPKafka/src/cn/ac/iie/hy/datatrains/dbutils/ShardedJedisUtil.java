package cn.ac.iie.hy.datatrains.dbutils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class ShardedJedisUtil {

	static String confFilePath = "data-dispatcher.properties";
	static String key = "redisList";
	static ShardedJedisPool jedisPool = null;

	static {
		try{
		//System.out.println("-------------------1");
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(2048);
		poolConfig.setMaxIdle(4096);
		poolConfig.setMaxWaitMillis(20000);
		
		poolConfig.setTestOnBorrow(false);
		poolConfig.setTestOnReturn(false);
		//System.out.println("-------------------2");
		Properties pps = new Properties();
		try {
			//System.out.println("-------------------3");
			InputStream in = new BufferedInputStream(new FileInputStream(confFilePath));
			pps.load(in);
			//System.out.println("-------------------4");
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println("-------------------5");
		String value = pps.getProperty(key);// ip:port
		List<JedisShardInfo> infoList = new ArrayList<JedisShardInfo>();
		//System.out.println("-------------------6");
		String[] hosts = value.split(" ");
		//System.out.println("-------------------6---"+hosts.length);
		for (String hostPair : hosts) {
			
			String ip = hostPair.split(":")[0];
			int port = Integer.parseInt(hostPair.split(":")[1]);
			infoList.add(new JedisShardInfo(ip, port));
			//System.out.println("-------------------7"+ip+" "+port);
		}
		//System.out.println("-------------------8");
		jedisPool = new ShardedJedisPool(poolConfig, infoList);
		//System.out.println("-------------------9");
		}catch(Exception e)
		{e.printStackTrace();}
		
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
