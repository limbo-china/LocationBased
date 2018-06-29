package cn.ac.iie.hy.centralserver.handler;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.scistor.softcrypto.SoftCrypto;

import cn.ac.iie.hy.centralserver.data.ProvinceDBMap;
import cn.ac.iie.hy.centralserver.data.UserSubQueryBean;
import cn.ac.iie.hy.centralserver.data.UserSubQueryEncryptBean;
import cn.ac.iie.hy.centralserver.dbutils.GPSDataFromOscar;
import cn.ac.iie.hy.centralserver.dbutils.JedisUtilMap;
import cn.ac.iie.hy.centralserver.dbutils.RedisUtil;
import cn.ac.iie.hy.centralserver.dbutils.ShardedJedisUtil;
import cn.ac.iie.hy.centralserver.dbutils.ShardedJedisUtilWithPara;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

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
 */

public class DataDetailQueryHandler extends AbstractHandler {

	private static DataDetailQueryHandler dataHandler = null;
	private static Properties prop = new Properties();
	private static Jedis jedisuli = null;
	static Logger logger = null;

	static {
		try {
			prop.load(new FileInputStream("data-pro.properties"));
			jedisuli = new Jedis(prop.getProperty("uliRedisIp").split(":")[0],Integer.valueOf(prop.getProperty("uliRedisIp").split(":")[1]));
		} catch (Exception e){
			e.printStackTrace();
		}
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDetailQueryHandler.class.getName());
	}

	private class Turple2 {
		private final int ret;
		private final String out;

		public Turple2(int ret, String out) {
			super();
			this.ret = ret;
			this.out = out;
		}

		public int getRet() {
			return ret;
		}

		public String getOut() {
			return out;
		}

	}

	public DataDetailQueryHandler() {
		super();
	}

	public static DataDetailQueryHandler getHandler() {
		if (dataHandler != null) {
			return dataHandler;
		}
		dataHandler = new DataDetailQueryHandler();
		return dataHandler;
	}

	@Override
	public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException, ServletException {
		String token = httpServletRequest.getParameter("token");
		String queryType = httpServletRequest.getParameter("querytype");
		String index = httpServletRequest.getParameter("index");
		List<String> indexList = Arrays.asList(index.split(","));
		
		String remoteHost = baseRequest.getRemoteAddr();
		int ret = 0;
		int count =0;
		String result = null;
		String imsi = null;
		String responese = null;
		logger.info(remoteHost + "request pro query token:" + token + " querytype:" + queryType + " index:" + index);

		for(String aIndex : indexList){
			count++;
			logger.info("times: "+count);
			do {
				
				Turple2 r = checkToken(token, queryType, aIndex);
				ret = r.getRet();
				boolean isEncrypt = false;
				if(token.equals("123456")){
					ret = 0;
					r = new Turple2(0, aIndex);
				}
				if (ret == 1) {
					isEncrypt = true;
					ret = 0;
				}
				if (ret != 0) {
					break;
				}
				String out = r.getOut();
				//logger.info(out + "len:"+out.length());
				if (queryType.equals("msisdn")) {
					imsi = queryImsi(out);
					logger.info(imsi);
				} else {
					imsi = out;
				}
				if (imsi == null) {
					ret = 6;
					break;
				}
	
				String provinceCode = queryProvinceCode(imsi);
				//logger.info("prov: "+provinceCode);
				if (provinceCode == null) {
					ret = 7;
					break;
				}
	
				String ipList = ProvinceDBMap.getProDBIP(provinceCode);
				//logger.info("ipList: "+ipList);
				if (isEncrypt) {
					result = queryEncryptDetail(ipList, imsi, aIndex);
				} else {
					result = queryDetail(ipList, imsi, provinceCode);
				}
	
			} while (false);
	
			if (ret != 0) {
				JsonObject element = new JsonObject();
				element.addProperty("status", ret);
				element.addProperty("reason", getReason(ret));
				result = element.toString();
			}
			responese += result;
		}

		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		httpServletResponse.getWriter().println(responese);
	}
//现在用
	private Turple2 checkToken(String token, String queryType, String index) {
		if (token == null || token.isEmpty()) {
			return new Turple2(2, null);
		}
		if (queryType == null || queryType.isEmpty()) {
			return new Turple2(2, null);
		}
		if (index == null || index.isEmpty()) {
			return new Turple2(2, null);
		}
		Jedis confJedis = RedisUtil.getJedis();
		if (!confJedis.exists(token)) {
			RedisUtil.returnResource(confJedis);
			return new Turple2(5, null);
		}
		String out = confJedis.hget(token, index);
		if (out == null) {
			RedisUtil.returnResource(confJedis);
			return new Turple2(3, null);
		}
		if (!out.equals(index)) {
			RedisUtil.returnResource(confJedis);
			return new Turple2(1, out);
		}
		RedisUtil.returnResource(confJedis);
		return new Turple2(0, out);
	}

	private String queryGPSFromDB(String uli) {
		return new GPSDataFromOscar(uli).getGPS();
	}
	
	private String queryGPSFromRedis(String uli){	
		 return jedisuli.get(uli);
	}

	private String queryEncryptDetail(String ip, String imsi, String index) {
		if(ip.startsWith("10.224")){
			for (int port = 6379; port < 6382; port++) {
				Jedis jedis = new Jedis("10.224.82.1", port);
				String v = jedis.get(imsi);
				jedis.close();
				if (v != null) {
					UserSubQueryEncryptBean usqb = new UserSubQueryEncryptBean();
					usqb.setStatus(0);
					usqb.setIndex(index);
					usqb.setRegionCode(v.split(";")[3]);
					usqb.setLac(v.split(";")[4]);
					usqb.setCi(v.split(";")[5]);
					usqb.setUli(v.split(";")[6]);
					usqb.setHomeCode(v.split(";")[7]);
					usqb.setTime(timeStamp2Date(v.split(";")[10], null));

					String rawGPS = queryGPSFromDB(v.split(";")[6]);
					usqb.setLngi(Double.parseDouble(rawGPS.split(",")[0]));
					usqb.setLati(Double.parseDouble(rawGPS.split(",")[1]));
					usqb.setProvince(rawGPS.split(",")[2]);
					usqb.setCity(rawGPS.split(",")[3]);
					usqb.setDistrict(rawGPS.split(",")[4]);
					usqb.setBaseinfo(rawGPS.split(",")[5]);
					Gson gson = new Gson();
					String jsonResult = gson.toJson(usqb);
					return jsonResult;
				}
			}
			for (int port = 6379; port < 6382; port++) {
				Jedis jedis = new Jedis("10.224.82.2", port);
				String v = jedis.get(imsi);
				jedis.close();
				if (v != null) {
					UserSubQueryEncryptBean usqb = new UserSubQueryEncryptBean();
					usqb.setStatus(0);
					usqb.setIndex(index);
					usqb.setRegionCode(v.split(";")[3]);
					usqb.setLac(v.split(";")[4]);
					usqb.setCi(v.split(";")[5]);
					usqb.setUli(v.split(";")[6]);
					usqb.setHomeCode(v.split(";")[7]);
					usqb.setTime(timeStamp2Date(v.split(";")[10], null));

					String rawGPS = queryGPSFromDB(v.split(";")[6]);
					usqb.setLngi(Double.parseDouble(rawGPS.split(",")[0]));
					usqb.setLati(Double.parseDouble(rawGPS.split(",")[1]));
					usqb.setProvince(rawGPS.split(",")[2]);
					usqb.setCity(rawGPS.split(",")[3]);
					usqb.setDistrict(rawGPS.split(",")[4]);
					usqb.setBaseinfo(rawGPS.split(",")[5]);
					Gson gson = new Gson();
					String jsonResult = gson.toJson(usqb);
					return jsonResult;
				}
			}
		}
		int portUpper = 6383;
		if(ip.startsWith("10.231")){
			portUpper = 6384;
		}
		for (int port = 6379; port < portUpper; port++) {
			Jedis jedis = new Jedis(ip, port);
			String v = jedis.get(imsi);
			jedis.close();
			if (v != null) {
				UserSubQueryEncryptBean usqb = new UserSubQueryEncryptBean();
				usqb.setStatus(0);
				usqb.setIndex(index);
				usqb.setRegionCode(v.split(";")[3]);
				usqb.setLac(v.split(";")[4]);
				usqb.setCi(v.split(";")[5]);
				usqb.setUli(v.split(";")[6]);
				usqb.setHomeCode(v.split(";")[7]);
				usqb.setTime(timeStamp2Date(v.split(";")[10], null));

				String rawGPS = queryGPSFromDB(v.split(";")[6]);
				usqb.setLngi(Double.parseDouble(rawGPS.split(",")[0]));
				usqb.setLati(Double.parseDouble(rawGPS.split(",")[1]));
				usqb.setProvince(rawGPS.split(",")[2]);
				usqb.setCity(rawGPS.split(",")[3]);
				usqb.setDistrict(rawGPS.split(",")[4]);
				usqb.setBaseinfo(rawGPS.split(",")[5]);
				Gson gson = new Gson();
				String jsonResult = gson.toJson(usqb);
				return jsonResult;
			}
		}
		return null;
	}

	private String queryDetail(String ipList, String imsi, String defaultRegionCode) {
		
		if(ipList.startsWith("10.241"))
			return "241";
		if(ipList.startsWith("10.245")){
			String imsitmp = imsi;
			if(imsi!=null && imsi.length()==15)
			{
				SoftCrypto crypt = new SoftCrypto();
				crypt.Initialize("abc");
				byte[] datain = imsi.substring(3, 15).getBytes();
				byte[] dataout = new byte[datain.length];			
				int res = crypt.crypto_encrypt(datain, dataout, datain.length, 1, 0);
				String t = new String(dataout);
				//logger.info("res: "+res);
				imsi = imsi.substring(0,3)+t;
			}
			//logger.info("imsi: "+imsi);		
		}
		ShardedJedisUtilWithPara redisList = new ShardedJedisUtilWithPara(ipList);
		ShardedJedis jedisCluster = redisList.getSource();
		String v = jedisCluster.get(imsi);
		redisList.returnResource(jedisCluster);
		//logger.info("v: "+v);	
		
		if (v != null) {
			UserSubQueryBean usqb = new UserSubQueryBean();
			usqb.setStatus(0);
			usqb.setImsi(v.split(";")[0]);
			usqb.setImei(v.split(";")[1]);
			usqb.setMsisdn(v.split(";")[2]);
			if(v.split(";")[3].length()!=6){
				usqb.setRegionCode(defaultRegionCode+"0000");

			}else{
					usqb.setRegionCode(v.split(";")[3]);

			}
			usqb.setLac(v.split(";")[4]);
			usqb.setCi(v.split(";")[5]);
			usqb.setUli(v.split(";")[6]);
			usqb.setHomeCode(v.split(";")[7]);
			usqb.setTime(timeStamp2Date(v.split(";")[10], null));

			String rawGPS = queryGPSFromRedis(v.split(";")[6]);
			if(rawGPS !=null){
				usqb.setLngi(Double.parseDouble(rawGPS.split(",")[1]));
				usqb.setLati(Double.parseDouble(rawGPS.split(",")[2]));
				usqb.setProvince(rawGPS.split(",")[3]);
				usqb.setCity(rawGPS.split(",")[4]);
				usqb.setDistrict(rawGPS.split(",")[5]);
				usqb.setBaseinfo(rawGPS.split(",")[7]);
			}else{
				usqb.setLngi(0.0);
				usqb.setLati(0.0);
				usqb.setProvince("null");
				usqb.setCity("null");
				usqb.setDistrict("null");
				usqb.setBaseinfo("null");
			}
			Gson gson = new Gson();
			String jsonResult = gson.toJson(usqb);
			return jsonResult;
		}
		return null;
	}

	private String queryImsi(String msisdn) {
		ShardedJedis jedis = JedisUtilMap.getSource();
		String imsi = jedis.get(msisdn);
		JedisUtilMap.returnResource(jedis);
		return imsi;
	}

	private String timeStamp2Date(String seconds, String format) {
		if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
			return "";
		}
		if (format == null || format.isEmpty())
			format = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date(Long.valueOf(seconds + "000")));
	}

	private String queryProvinceCode(String index) {

		ShardedJedis jedisCluster = ShardedJedisUtil.getSource();
		String value = null;

		value = jedisCluster.get(index);

		ShardedJedisUtil.returnResource(jedisCluster);

		if (value != null) {

			return value.split(",")[3].substring(0, 2);

		} else {
			return null;
		}
	}

	private String getReason(int ret) {
		switch (ret) {
		case 0:
			return "Right";
		case 1:
			return "服务器错误";
		case 2:
			return "请求参数非法";
		case 3:
			return "权限校验失败";
		case 4:
			return "配额不足";
		case 5:
			return "token 不存在或非法";
		case 6:
			return "手机号映射缺失";
		case 7:
			return "查询结果为空";
		default:
			return "未知错误";
		}
	}
}
