package cn.ac.iie.hy.datadispatch.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import cn.ac.iie.hy.datadispatch.dbutils.MySQLUtils;
import cn.ac.iie.hy.datadispatch.dbutils.MsisndMapRedisUtil;
import cn.ac.iie.hy.datadispatch.dbutils.ShardedJedisUtil;
import cn.ac.iie.hy.datadispatch.handler.DataDispatcher;
import cn.ac.iie.hy.datadispatch.metadata.RoamData;
import cn.ac.iie.hy.datadispatch.metadata.SMetaData;
import cn.ac.iie.hy.datadispatch.utils.HomeCodeMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;


/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　　　　　┃
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　　　　　┃
 * 　　┃　　　┻　　　┃
 * 　　┃　　　　　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃神兽保佑, 永无BUG!
 * 　　　　┃　　　┃Code is far away from bug with the animal protecting
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * @author zhangyu
 *
 */
public class DBUpdateTask implements Runnable {

	List<SMetaData> al = null;
	private static MySQLUtils mysqlutil = null; 
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DBUpdateTask.class.getName());
	}
	
	public  DBUpdateTask(List<SMetaData> al) {
		this.al = al;
		if(mysqlutil == null){
			mysqlutil = new MySQLUtils("jdbc:mysql://10.213.73.7:3306/diie?characterEncoding=utf8&useSSL=true");
		}
	}

	private void saveOneSmd(SMetaData smd, String last) {
		String sql = "INSERT INTO `diie`.`t_xl_data`"
				+ "(`c_imsi`,`c_imei`,`c_msisdn`,`c_lastregion`,`c_regioncode`,`c_lac`,`c_ci`,`c_uli`,`c_homecode`,`c_lngi`,`c_lati`,`c_timestamp`)"
				+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?);";
		Object[] param = {smd.getImsi(), smd.getImei(), smd.getMsisdn(), last, smd.getRegionCode(), smd.getLac(), smd.getCi(), smd.getUli()
				,smd.getHomeCode(), smd.getLngi(), smd.getLati(), smd.getTimestamp()};
		mysqlutil.executeUpdate(sql, param);
		
	}

	private String SData2Str(SMetaData smd) {
		return smd.getImsi() + "," + smd.getImei() + "," + smd.getMsisdn() + "," + smd.getRegionCode() + ","
				+ smd.getLac() + "," + smd.getCi() + "," + smd.getUli() + "," + smd.getHomeCode() + "," + smd.getLngi()
				+ "," + smd.getLati() + "," + smd.getTimestamp();
	}

	private boolean fieldsCheck(String regionCode, String homeCode) {
		if (homeCode.isEmpty() || homeCode.length() < 3 || regionCode.length() != 6)
			return false;
		return HomeCodeMap.homecodeExists(homeCode);
	}

	private boolean isRoaming(String regionCode, String homeCode) {
		return HomeCodeMap.homecodePair(regionCode, homeCode);
	}

	private void dbUpdateTask(SMetaData smd, String value, ShardedJedisPipeline jedisCluster, ShardedJedis jedisMsisdn, List<RoamData> rdList, List<String> changeList) {
	
		String imsi = smd.getImsi();	
		if (value != null) {
			if (smd.getRegionCode().length() != 6) {
				return;
			}
			
			if(smd.getMsisdn()=="" || smd.getMsisdn()=="0"){
				String msisdn = value.split(",")[2];
				smd.setMsisdn(msisdn);	
			}
			
			jedisCluster.set(imsi, SData2Str(smd));//modify by zhangyu 2017-02-23
			String oldRegionCode = value.split(",")[3];
			String sub = smd.getRegionCode().substring(0, 2);
			
			if (!oldRegionCode.startsWith(sub)) {
				
				changeList.add(SData2Str(smd)+","+oldRegionCode);
				
				// if(smd.getRegionCode().substring(0, 2).equals("33") &&
				// !oldRegionCode.isEmpty() && !oldRegionCode.startsWith("00")){
				// //saveOneSmd(smd, oldRegionCode);
				// }
//				if (smd.getMsisdn().length() == 13) {
//					jedis.set(smd.getMsisdn(), imsi);
//				}
				// 所在省份有变化
				// 如果原来没有漫游，发送一条漫游记录
				// 如果原来是漫游，判断进入地时候还是漫游
				
				if(fieldsCheck(value.split(",")[3], value.split(",")[7])){
					if (!isRoaming(value.split(",")[3], value.split(",")[7])) {
						// add one
						rdList.add(new RoamData(sub, smd.getRegionCode(), smd.getHomeCode(), smd.getMsisdn(), (int)smd.getTimestamp(), 1));
						//System.out.println("111111");
					} else {
						//System.out.println("111111");
						if (isRoaming(smd.getRegionCode(), value.split(",")[7])) {
							// delete and add
							rdList.add(new RoamData(value.split(",")[3].substring(0, 2), smd.getRegionCode(), smd.getHomeCode(), smd.getMsisdn(), (int)smd.getTimestamp(), 0));
							rdList.add(new RoamData(sub, smd.getRegionCode(), smd.getHomeCode(), smd.getMsisdn(), (int)smd.getTimestamp(), 1));

						} else {
							// delete
							rdList.add(new RoamData(value.split(",")[3].substring(0, 2), smd.getRegionCode(), smd.getHomeCode(), smd.getMsisdn(), (int)smd.getTimestamp(), 0));
						}
					}
				}
				
			} else {
//				if (smd.getMsisdn().length() == 13) {
//					jedis.set(smd.getMsisdn(), imsi);
//				}
				// 所在省份无变化，但是还在漫游，发送增量
				
				if (fieldsCheck(smd.getRegionCode(), smd.getHomeCode())){
					if (isRoaming(smd.getRegionCode(), smd.getHomeCode())) {
						// add
						rdList.add(new RoamData(sub, smd.getRegionCode(), smd.getHomeCode(), smd.getMsisdn(), (int)smd.getTimestamp(), 1));

					}
				}
			}
		} else {

		}
		
	}

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        ShardedJedis jedisMsisdn = null; 
        ShardedJedis rCacheJedis = null;
		ShardedJedis wCacheJedis = null;
		//logger.info("-------------1");
		 
		try {
			
			//logger.info("-------------2");
			List<RoamData> rdList = new ArrayList<RoamData>();
			List<String> changeList = new ArrayList<String>();//modify 2017-02-16
			jedisMsisdn = MsisndMapRedisUtil.getJedis();
			ShardedJedisPipeline msisdnPipe = jedisMsisdn.pipelined();
			rCacheJedis = ShardedJedisUtil.getSource();
			wCacheJedis = ShardedJedisUtil.getSource();
			ShardedJedisPipeline rCachePipe = rCacheJedis.pipelined();
			//logger.info("-------------3");
			HashMap<String, SMetaData> imsiMap = new HashMap<>();

			for (Iterator<SMetaData> it = al.iterator(); it.hasNext();) {
				SMetaData smd = it.next();
				rCachePipe.get(smd.getImsi());
				imsiMap.put(smd.getImsi(), smd);
	        }
			List<Object> resp = rCachePipe.syncAndReturnAll();
			ShardedJedisPipeline wCachePipe = wCacheJedis.pipelined();
			for (Iterator<Object> it = resp.iterator(); it.hasNext();) {
				String rs = (String) it.next();
				if (rs != null && rs.split(",").length > 2) {
					SMetaData smd = imsiMap.get(rs.split(",")[0]);
					if(smd != null){
						dbUpdateTask(smd, rs, wCachePipe, jedisMsisdn, rdList, changeList);
						imsiMap.remove(rs.split(",")[0]);
					}
					
				}
			}

			int all =imsiMap.size();
			int count =0 ;
			for (SMetaData smd : imsiMap.values()) {
				wCachePipe.set(smd.getImsi(), SData2Str(smd));
				if (fieldsCheck(smd.getRegionCode(), smd.getHomeCode())) {
					if (isRoaming(smd.getRegionCode(), smd.getHomeCode())) {
						
						rdList.add(new RoamData(smd.getRegionCode().substring(0, 2), smd.getRegionCode(),
								smd.getHomeCode(), smd.getMsisdn(), (int) smd.getTimestamp(), 1));

					}
				}
				if (smd.getMsisdn().length() == 13) {
					msisdnPipe.set(smd.getMsisdn(), smd.getImsi());
				}
				count ++;
				//logger.info("all: "+all+" count: "+count);
			}
			wCachePipe.sync();
			msisdnPipe.sync();
	        //needSomeTime();
			
//	        MsisndMapRedisUtil.returnResource(jedisMsisdn);
//	        ShardedJedisUtil.returnResource(rCacheJedis);
//	        ShardedJedisUtil.returnResource(wCacheJedis);
			jedisMsisdn.close();
			rCacheJedis.close();
			wCacheJedis.close();
//			rdList.add(new RoamData("11","310000","0000","8618019167492",1502787592,0));
//			rdList.add(new RoamData("31","310000","0000","8618019167492",1502787853,1));
//			rdList.add(new RoamData("13","310000","0000","8618019167479",1502787592,0));
//			rdList.add(new RoamData("31","310000","0000","8618019167479",1502787853,1));
			
	        if(!rdList.isEmpty()){
				DataDispatcher.runRabbitTask(rdList);
			}
	        //logger.info("changeList size:"+ changeList.size());
	        if(!changeList.isEmpty()){
	        	//logger.info("changeList sending");
	        	DataDispatcher.runLoadTask(changeList);
	        }
	        
	        long endTime = System.currentTimeMillis();
	        logger.info("ChangeList send ending: "+changeList.size()+"time: "+(endTime-startTime));
		}
		catch (Exception e) {
			logger.info(e.getMessage());
			e.printStackTrace();
//			MsisndMapRedisUtil.returnBrokenResource(jedisMsisdn);
//			ShardedJedisUtil.returnBrokenResource(rCacheJedis);
//			ShardedJedisUtil.returnBrokenResource(wCacheJedis);
		}
       
    }
}
