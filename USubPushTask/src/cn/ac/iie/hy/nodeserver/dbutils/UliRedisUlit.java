package cn.ac.iie.hy.nodeserver.dbutils;

import java.io.FileInputStream;
import java.util.Properties;

import redis.clients.jedis.Jedis;

public class UliRedisUlit {

	private static Properties prop = new Properties();
	private static Jedis jedis = null;
	static{
		try {
			prop.load(new FileInputStream("data-node.properties"));
			String ip = prop.getProperty("uliRedisIp").split(":")[0];
			int port = Integer.valueOf(prop.getProperty("uliRedisIp").split(":")[1]);
			jedis = new Jedis(ip, port);
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public static Jedis getUliRedisResource(){
		return jedis;
	}
}
