/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.centralserver.dbutils;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class ShardedJedisUtilWithPara {

	String ipList = null; // ip:port
	JedisPoolConfig poolConfig = null;
	private ShardedJedisPool jedisPool = null;

	public ShardedJedisUtilWithPara(String ipList) {
		this.ipList = ipList;
		poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(2048);
		poolConfig.setMaxIdle(4096);
		poolConfig.setMaxWaitMillis(20000);
		poolConfig.setTestOnBorrow(false);
		poolConfig.setTestOnReturn(false);
	}

	public synchronized ShardedJedis getSource() {

		List<JedisShardInfo> infoList = new ArrayList<JedisShardInfo>();

		String[] hosts = ipList.split(" ");
		for (String hostPair : hosts) {
			String ip = hostPair.split(":")[0];
			int port = Integer.parseInt(hostPair.split(":")[1]);
			infoList.add(new JedisShardInfo(ip, port));
		}
		jedisPool = new ShardedJedisPool(poolConfig, infoList);

		if (jedisPool != null) {
			return jedisPool.getResource();
		} else {
			return null;
		}
	}

	public void returnBrokenResource(ShardedJedis jedis) {
		if (jedis != null) {
			jedisPool.returnBrokenResource(jedis);

		}
	}

	public void returnResource(ShardedJedis jedis) {
		if (jedis != null) {
			jedisPool.returnResource(jedis);
		}
	}

}
