package cn.ac.iie.hy.centralserver.task;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import com.google.gson.Gson;

import cn.ac.iie.hy.centralserver.data.UserSubConfigBean;
import cn.ac.iie.hy.centralserver.dbutils.RedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class SubConfigTask implements Runnable {

	private String configId;
	private static String key = "ImportantPersonRule";
	private static String configIpFile = "";
	public SubConfigTask(String configId) {
		super();
		this.configId = configId;
	}

	private void loadConfig(UserSubConfigBean uscb){
		String indexType = uscb.getIndexType();
		String time = uscb.getTime();
		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
		    inputStream = new FileInputStream(configIpFile);
		    sc = new Scanner(inputStream, "UTF-8");
		    
		    //List<String> cache = new ArrayList<String>();
		    //Pipeline p = jedis.pipelined();
		    while (sc.hasNextLine()) {
		    	String line = sc.nextLine();
		    	Jedis jedis = new Jedis(line, 6379, 5000);
		    	Pipeline p = jedis.pipelined();
		    	for(String index : uscb.getPersonInfos()){
		    		String v = indexType.toLowerCase() + ";" + index + ";" + "24782317481724411;http://10.213.73.2:8888/datarecv/;6e2ba35c78eaa7655c46b46fd02ae9c0;"+time;
		    		//String v = "msisdn;" + line + ";20803769127521655;http://10.213.73.2:8888/datarecv/;6e2ba35c78eaa7655c46b46fd02ae9c0;2016-12-13 17:09:15";
			    	p.set("SUB_"+indexType+"_" + index, v);
		    	}
		    	p.sync();
		    }
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
		    if (inputStream != null) {
		        try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		    }
		    if (sc != null) {
		        sc.close();
		    }
		}		
	}
	
	@Override
	public void run() {
		Jedis jedis = null;
		try{
			jedis = RedisUtil.getJedis();
			String rawData = jedis.hget(key, configId);
			System.out.println(rawData);
			UserSubConfigBean uscb = new Gson().fromJson(rawData, UserSubConfigBean.class);
			System.out.println(uscb.getPersonInfos().size());
			RedisUtil.returnResource(jedis);
		} catch (Exception e){
			RedisUtil.returnBrokenResource(jedis);
		}
	}
	
	public static void main(String[] argv){
		new SubConfigTask("100000001").run();
	}

}
