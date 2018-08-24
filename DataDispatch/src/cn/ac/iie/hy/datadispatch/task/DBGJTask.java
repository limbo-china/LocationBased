package cn.ac.iie.hy.datadispatch.task;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;




import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import cn.ac.iie.hy.datadispatch.dbutils.PublishRedisUtil;
import cn.ac.iie.hy.datadispatch.httpload.AvroHttpLoad;
import cn.ac.iie.hy.datadispatch.metadata.RTPGJData;


public class DBGJTask implements Runnable {

	List<Object> gjList = null;

	public static String[] urls = null;
	static Logger logger = null;
	static {
		   PropertyConfigurator.configure("log4j.properties");
	   logger = Logger.getLogger(DBGJTask.class.getName());
	}
//	private static HashMap<String, String> codeMap = new HashMap<String, String>() {
//
//		private static final long serialVersionUID = -4696130080509842698L;
//
//		{
//			put("11", "北京");
//			put("12", "天津");
//			put("13", "河北");
//			put("14", "山西");
//			put("15", "内蒙古");
//			put("21", "辽宁");
//			put("22", "吉林");
//			put("23", "黑龙江");
//			put("31", "上海");
//			put("32", "江苏");
//			put("33", "浙江");
//			put("34", "安徽");
//			put("35", "福建");
//			put("36", "江西");
//			put("37", "山东");
//			put("41", "河南");
//			put("42", "湖北");
//			put("43", "湖南");
//			put("44", "广东");
//			put("45", "广西");
//			put("46", "海南");
//			put("50", "重庆");
//			put("51", "四川");
//			put("52", "贵州");
//			put("53", "云南");
//			put("54", "西藏");
//			put("61", "陕西");
//			put("62", "甘肃");
//			put("63", "青海");
//			put("64", "宁夏");
//			put("65", "新疆");
//		}
//	};

	public DBGJTask(List<Object> glList) {
		super();
		this.gjList = glList;
		// logger.info("before send0011");
	}

	public static void setUrlsList(String[] urls) {
		DBGJTask.urls = urls;
	}

	
	private String getUrl() {
		int index = (int) (1 + Math.random() * (urls.length - 1));
		return "http://" + urls[index];
	}

	public void sendData() {
		
	logger.info("monitorwarning publishToSTS!!");
	
		
	if (this.gjList.size() > 0 && this.gjList != null) {
		
		   try{
		    
			AvroHttpLoad avroHttpLoadSTKActivePlace = new AvroHttpLoad();
			avroHttpLoadSTKActivePlace.sendData(this.gjList);
//			if (this.gjList.size() < 2) {
//				avroHttpLoadSTKActivePlace.sendData(this.gjList);
//			} else {
//				for (int i = 0;; i = i + 2) {
//					if (i + 2 < this.gjList.size()) {
//
//						avroHttpLoadSTKActivePlace
//								.sendData(this.gjList.subList(i,
//										i + 2));
//					} else {
//						avroHttpLoadSTKActivePlace
//								.sendData(this.gjList.subList(i,
//										(this.gjList.size())));
//						break;
//					}
//				}
//
//			}
		   }catch(Exception e)

		   {e.printStackTrace();}
		}
	}


	public String  timestampToDate(String time) {
		long timeStamp =  Long.parseLong(time)*1000l;//直接是时间戳
		//long timeStamp = System.currentTimeMillis();  //获取当前时间戳,也可以是你自已给的一个随机的或是别人给你的时间戳(一定是long型的数据)
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//这个是你要转成后的时间的格式
		String sd = sdf.format(new Date(timeStamp));   // 时间戳转换成时间
		        System.out.println(sd);//打印出你要的时间
		return sd;
	}
	public void publishToRedis() {
		System.out.println("publishToRedis!!");
		logger.info("monitorwarning publishToRedis!!");
		Jedis jedis = PublishRedisUtil.getJedis();
		List<Object> fdata = new ArrayList(gjList.size());
        fdata = gjList;
		for (Object rd : fdata) {
			try {
				String smd = (String)rd;
//				JSONObject jsonObject = new JSONObject();
//				jsonObject.put("groupId", ((RTPGJData) rd).getGroupId());
//				jsonObject.put("groupName",((RTPGJData) rd).getGroupName());
//				jsonObject.put("source", ((RTPGJData) rd).getSource());
//				jsonObject.put("provinceId", ((RTPGJData) rd).getProvinceId());
//				jsonObject.put("provinceName", ((RTPGJData) rd).getProvinceName());
//				jsonObject.put("phone", ((RTPGJData) rd).getPhone());
//				jsonObject.put("updatetime", timestampToDate(((RTPGJData) rd).getUpdatetime()));
//				jsonObject.put("action",((RTPGJData) rd).getAction());
				
			
				logger.info("monitorwarning publishToRedis!!"+smd);
				jedis.publish("YDJC-RT-WARNING",
						smd);

			} catch (JedisConnectionException e) {
				PublishRedisUtil.returnBrokenResource(jedis);
			}
		}
		PublishRedisUtil.returnResource(jedis);

	}

	@Override
	public void run() {		
		publishToRedis();
		sendData();
	}

}
