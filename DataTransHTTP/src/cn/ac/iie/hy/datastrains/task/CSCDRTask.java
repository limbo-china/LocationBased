package cn.ac.iie.hy.datastrains.task;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.datatrains.dbutils.RedisUtilPro;
import cn.ac.iie.hy.datatrains.handler.DataDispatchHandler;
import cn.ac.iie.hy.datatrains.metadata.SMetaData;
import redis.clients.jedis.Jedis;

public class CSCDRTask implements Runnable{

	private List<String> al = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDispatchHandler.class.getName());
	}
	public CSCDRTask(List<String> al) {
		this.al = al;
	}

	@Override
	public void run() {
		Jedis jedis = RedisUtilPro.getJedis();
		try{
			for (Iterator<String> it = al.iterator(); it.hasNext();){
				String data = it.next();
				jedis.lpush("cs_cdr_queue", data);
			}
			RedisUtilPro.returnResource(jedis);
		}
		catch (Exception e) {
			RedisUtilPro.returnBrokenResource(jedis);
		}
		
	}

}
