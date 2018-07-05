package cn.ac.iie.test.distribution;

import redis.clients.jedis.Jedis;

public class RedisPublish {

		public static void main(String[] args) throws InterruptedException{
			Jedis jedis = new Jedis("172.16.18.34",6398);
			
			int count =0;
			while(true){
				jedis.publish("TestChannel", "TestPublibsh: message sent!"+count++);
				Thread.sleep(2000);
			}
		}
}
