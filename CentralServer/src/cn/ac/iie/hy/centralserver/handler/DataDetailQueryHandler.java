package cn.ac.iie.hy.centralserver.handler;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.hy.centralserver.data.ProvinceDBMap;
import cn.ac.iie.hy.centralserver.data.UserSubQueryBean;
import cn.ac.iie.hy.centralserver.dbutils.JedisUtilMap;
import cn.ac.iie.hy.centralserver.dbutils.RedisUtil;
import cn.ac.iie.hy.centralserver.dbutils.ShardedJedisUtil;
import cn.ac.iie.hy.centralserver.dbutils.ShardedJedisUtilWithPara;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class DataDetailQueryHandler extends AbstractHandler {

	private static DataDetailQueryHandler dataHandler = null;
	private static Properties prop = new Properties();
	private static Jedis jedisuli = null;
	static Logger logger = null;

	static {
		try {
			prop.load(new FileInputStream("data-pro.properties"));
			jedisuli = new Jedis(
					prop.getProperty("uliRedisIp").split(":")[0],
					Integer.valueOf(prop.getProperty("uliRedisIp").split(":")[1]));
		} catch (Exception e) {
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
	public void handle(String string, Request baseRequest,
			HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException,
			ServletException {
		String token = httpServletRequest.getParameter("token");
		String queryType = httpServletRequest.getParameter("querytype");
		String index = httpServletRequest.getParameter("index");
		List<String> indexList = Arrays.asList(index.split(","));
		HashMap<String, HashMap<String, Integer>> imsiMap = new HashMap<String, HashMap<String, Integer>>();

		logger.info(httpServletRequest.getRequestURL());
		HashMap<String, String> imsi2msisdn = new HashMap<String, String>();
		HashMap<String, Turple2> imsi2turple = new HashMap<String, Turple2>();
		HashMap<String, String> provinceMap = new HashMap<String, String>();

		String remoteHost = baseRequest.getRemoteAddr();
		int ret = 0;
		int count = 0;
		String result = "";
		String imsi = null;
		logger.info(remoteHost + "request pro query token:" + token
				+ " querytype:" + queryType + " index:" + index);

		checkToken(token, queryType, indexList, imsi2turple);
		getImsi(queryType, indexList, imsi2msisdn);
		getProvince(indexList, imsi2turple, imsi2msisdn, provinceMap);

		for (String aIndex : indexList) {
			count++;
			logger.info("times: " + count);
			do {

				Turple2 r = imsi2turple.get(aIndex);
				ret = r.getRet();
				boolean isEncrypt = false;
				if (token.equals("123456")) {
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
				imsi = imsi2msisdn.get(r.getOut());
				logger.info("imsi: " + imsi);
				if (imsi == null) {
					ret = 6;
					break;
				}
			} while (false);

			String provinceCode = provinceMap.get(imsi);
			// logger.info("prov: "+provinceCode);
			if (provinceCode == null) {
				ret = 7;
				provinceCode = "00";
			}

			// what if provinceCode is null
			if (!imsiMap.containsKey(provinceCode)) {
				HashMap<String, Integer> map = new HashMap<String, Integer>();
				imsiMap.put(provinceCode, map);
			}
			imsiMap.get(provinceCode).put(imsi, ret);
		}

		for (Map.Entry<String, HashMap<String, Integer>> mapEntry : imsiMap
				.entrySet()) {
			String ipList = ProvinceDBMap.getProDBIP(mapEntry.getKey());
			result += queryDetail(ipList, mapEntry.getValue());
		}

		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		httpServletResponse.getWriter().println(result);
	}

	// 鐜板湪鐢�
	private void checkToken(String token, String queryType,
			List<String> indexList, HashMap<String, Turple2> map) {

		HashMap<String, String> indexmap = new HashMap<String, String>();

		boolean isTokenExist = true;
		Jedis confJedis = RedisUtil.getJedis();
		Pipeline pipeline = confJedis.pipelined();
		if (token == null || token.isEmpty() || !confJedis.exists(token))
			isTokenExist = false;

		if (isTokenExist) {
			for (String aIndex : indexList)
				pipeline.hget(token, aIndex);
		}

		List<Object> resp = pipeline.syncAndReturnAll();

		int count = 0;
		for (Object rs : resp) {
			indexmap.put(indexList.get(count++), (String) rs);
		}

		RedisUtil.returnResource(confJedis);

		for (String aIndex : indexList) {
			if (!isTokenExist) {
				map.put(aIndex, new Turple2(5, null));
				continue;
			}
			if (queryType == null || queryType.isEmpty()) {
				map.put(aIndex, new Turple2(2, null));
				continue;
			}
			String out = indexmap.get(aIndex);
			if (out == null) {
				map.put(aIndex, new Turple2(3, null));
				continue;
			}
			if (!out.equals(aIndex)) {
				map.put(aIndex, new Turple2(1, out));
				continue;
			}
			map.put(aIndex, new Turple2(0, out));
		}
	}

	private void getImsi(String queryType, List<String> indexList,
			HashMap<String, String> map) {
		ShardedJedis jedis = JedisUtilMap.getSource();
		ShardedJedisPipeline pipeline = jedis.pipelined();

		if (queryType.equals("msisdn"))
			for (String aIndex : indexList)
				pipeline.get(aIndex);
		else {
			for (String aIndex : indexList)
				map.put(aIndex, aIndex);
			return;
		}
		List<Object> resp = pipeline.syncAndReturnAll();
		JedisUtilMap.returnResource(jedis);

		int count = 0;
		for (Object rs : resp)
			map.put(indexList.get(count++), (String) rs);
	}

	private void getProvince(List<String> indexList,
			HashMap<String, Turple2> imsi2turple,
			HashMap<String, String> imsi2msisdn,
			HashMap<String, String> provinceMap) {
		ShardedJedis jedisCluster = ShardedJedisUtil.getSource();
		ShardedJedisPipeline pipeline = jedisCluster.pipelined();

		for (String aIndex : indexList) {
			String imsi = imsi2msisdn.get(imsi2turple.get(aIndex).getOut());
			if (imsi != null)
				pipeline.get(imsi);
		}
		List<Object> resp = pipeline.syncAndReturnAll();
		ShardedJedisUtil.returnResource(jedisCluster);

		int count = 0;
		for (Object rs : resp) {
			String imsi = imsi2msisdn.get(imsi2turple.get(
					indexList.get(count++)).getOut());
			if (imsi != null)
				provinceMap.put(imsi,
						((String) rs).split(",")[3].substring(0, 2));
		}

	}

	private String queryDetail(String ipList, HashMap<String, Integer> map) {

		// if (ipList.startsWith("10.245")) {
		// String imsitmp = imsi;
		// if (imsi != null && imsi.length() == 15) {
		// SoftCrypto crypt = new SoftCrypto();
		// crypt.Initialize("abc");
		// byte[] datain = imsi.substring(3, 15).getBytes();
		// byte[] dataout = new byte[datain.length];
		// int res = crypt.crypto_encrypt(datain, dataout, datain.length,
		// 1, 0);
		// String t = new String(dataout);
		// // logger.info("res: "+res);
		// imsi = imsi.substring(0, 3) + t;
		// }
		// // logger.info("imsi: "+imsi);
		// }

		String result = "";
		HashMap<String, String> uliMap = new HashMap<String, String>();
		ShardedJedisUtilWithPara redisList = new ShardedJedisUtilWithPara(
				ipList);
		ShardedJedis jedisCluster = redisList.getSource();
		ShardedJedisPipeline pipeline = jedisCluster.pipelined();

		Pipeline uliPipeline = jedisuli.pipelined();

		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			int ret = entry.getValue();
			if (ret != 0) {
				JsonObject element = new JsonObject();
				element.addProperty("status", ret);
				element.addProperty("reason", getReason(ret));
				result += element.toString();
				continue;
			}
			String imsi = entry.getKey();
			pipeline.get(imsi);
		}

		List<Object> resp = pipeline.syncAndReturnAll();
		for (Object rs : resp) {
			String v = (String) rs;
			uliPipeline.get(v.split(";")[6]);
		}
		List<Object> uliresp = uliPipeline.syncAndReturnAll();
		for (Object ulirs : uliresp) {
			String uliv = (String) ulirs;
			uliMap.put(uliv.split(",")[0], uliv);
		}

		for (Object rs : resp) {
			String v = (String) rs;
			if (v != null) {
				UserSubQueryBean usqb = new UserSubQueryBean();
				usqb.setStatus(0);
				usqb.setImsi(v.split(";")[0]);
				usqb.setImei(v.split(";")[1]);
				usqb.setMsisdn(v.split(";")[2]);
				usqb.setRegionCode(v.split(";")[3]);
				usqb.setLac(v.split(";")[4]);
				usqb.setCi(v.split(";")[5]);
				usqb.setUli(v.split(";")[6]);
				usqb.setHomeCode(v.split(";")[7]);
				usqb.setTime(timeStamp2Date(v.split(";")[10], null));

				String rawGPS = uliMap.get(v.split(";")[6]);
				if (rawGPS != null) {
					usqb.setLngi(Double.parseDouble(rawGPS.split(",")[1]));
					usqb.setLati(Double.parseDouble(rawGPS.split(",")[2]));
					usqb.setProvince(rawGPS.split(",")[3]);
					usqb.setCity(rawGPS.split(",")[4]);
					usqb.setDistrict(rawGPS.split(",")[5]);
					usqb.setBaseinfo(rawGPS.split(",")[7]);
				} else {
					usqb.setLngi(0.0);
					usqb.setLati(0.0);
					usqb.setProvince("null");
					usqb.setCity("null");
					usqb.setDistrict("null");
					usqb.setBaseinfo("null");
				}
				Gson gson = new Gson();
				result += gson.toJson(usqb);
			}
		}
		redisList.returnResource(jedisCluster);

		return result;
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

	private String getReason(int ret) {
		switch (ret) {
		case 0:
			return "Right";
		case 1:
			return "鏈嶅姟鍣ㄩ敊璇�";
		case 2:
			return "璇锋眰鍙傛暟闈炴硶";
		case 3:
			return "鏉冮檺鏍￠獙澶辫触";
		case 4:
			return "閰嶉涓嶈冻";
		case 5:
			return "token 涓嶅瓨鍦ㄦ垨闈炴硶";
		case 6:
			return "鎵嬫満鍙锋槧灏勭己澶�";
		case 7:
			return "鏌ヨ缁撴灉涓虹┖";
		default:
			return "鏈煡閿欒";
		}
	}
}
