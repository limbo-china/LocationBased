package cn.ac.iie.hy.nodeserver.task;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.List;

import cn.ac.iie.hy.nodeserver.dbutils.RedisUtilPro;
import redis.clients.jedis.Jedis;

public class UserSubTask {

	public  String timeStamp2Date(String seconds,String format) {  
        if(seconds == null || seconds.isEmpty() || seconds.equals("null")){  
            return "";  
        }  
        if(format == null || format.isEmpty()) format = "yyyy-MM-dd HH:mm:ss";  
        SimpleDateFormat sdf = new SimpleDateFormat(format);  
        return sdf.format(new Date(Long.valueOf(seconds+"000")));  
    }  

	
	public int subUserTaskRedis(String token, String indextype, String indexList, String jobID, String targetHost) {
		String[] index = indexList.split(";");
		Jedis jedis = RedisUtilPro.getJedis();

		for (int i = 0; i < index.length; i++) {
			String num = index[i];
			jedis.lpush("user_pro_list", indextype + ";" + num + ";" + jobID + ";" + targetHost + ";" + token + ";"
					+ timeStamp2Date(System.currentTimeMillis()/1000 + "", null));
		}
		RedisUtilPro.returnResource(jedis);
		return 0;

	}
	
	public int cancelUserTaskRedis(String token, String indextype, String indexList, String jobID, String targetHost) {
		
		Jedis jedis = RedisUtilPro.getJedis();

		List<String> allPro = jedis.lrange("user_pro_list", 0, -1);
		
		jedis.del("user_pro_list");
		
		for(int i = 0; i < allPro.size(); i++){
			String line = allPro.get(i);
			String proToken = line.split(";")[4];
			String index = line.split(";")[1];
			if(proToken.equals(token)&&indexList.contains(index)){
				
			}
			else{
				jedis.lpush("user_pro_list", line);
			}
			
		}
		
		RedisUtilPro.returnResource(jedis);
		return 0;

	}
}
