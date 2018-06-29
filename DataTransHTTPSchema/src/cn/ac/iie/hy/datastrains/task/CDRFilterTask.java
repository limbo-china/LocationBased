package cn.ac.iie.hy.datastrains.task;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.datatrains.dbutils.RedisUtilPro;
import cn.ac.iie.hy.datatrains.handler.DataDispatchHandler;
import cn.ac.iie.hy.datatrains.metadata.SMetaData;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class CDRFilterTask implements Runnable{

	private List<SMetaData> al = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDispatchHandler.class.getName());
	}
	
	public CDRFilterTask(List<SMetaData> al) {
		this.al = al;
	}

	private String SData2Str(SMetaData smd) {
		return smd.getCdrType() + ";" + smd.getMsisdn()  + ";" + smd.getRelateNum()  + ";"+ smd.getImsi() + ";" 
				+ smd.getLac() + ";" + smd.getCi() + ";" + smd.getUli() + ";"  + smd.getCdrContent().replaceAll(";", ",").replaceAll("\n", " ").replaceAll("\r",  "") + ";" +smd.getTimestamp() + ";" + smd.getImei() + ";";
	}
	
	private String getData(){
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");//设置日期格式
		return df.format(new Date());
	}
	
	@Override
	public void run() {
		
		Jedis jedis = RedisUtilPro.getJedis();
		Pipeline pipe = jedis.pipelined(); 
		try{
			for (Iterator<SMetaData> it = al.iterator(); it.hasNext();){
				SMetaData data = it.next();
//				if(data.getCdrType() >=3 && data.getCdrType()<=8){
//					pipe.lpush("mnumber_cdr_queue", SData2Str(data));
//				}
//				if(data.getCdrType() == 15 || data.getCdrType() == 16){
//					pipe.lpush("changan_cdr_queue", data.getSourceData());
//				}
//				if(data.getUli().equals("460-03-17-1266")||data.getUli().equals("460-00-94891-1")||data.getUli().equals("460-01-41026-43421")){
//					pipe.lpush("area_monitor_queue", data.getSourceData());
//				}
//				if(data.getImsi() == null || data.getImsi().length()<14){
//					pipe.incr("IMSI_LOST_" + getData());
//				}
//				if(data.getImei() == null || data.getImei().length()<14){
//					pipe.incr("IMEI_LOST_" + getData());
//				}
//				if(data.getMsisdn() == null || data.getMsisdn().length()<9){
//					pipe.incr("MSISDN_LOST_" + getData());
//				}
				
			}
			//
			pipe.sync();
			RedisUtilPro.returnResource(jedis);
		}
		catch (Exception e) {
			RedisUtilPro.returnBrokenResource(jedis);
		}
		
	}

}
