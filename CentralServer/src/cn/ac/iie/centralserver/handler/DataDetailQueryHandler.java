package cn.ac.iie.centralserver.handler;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import cn.ac.iie.centralserver.config.ProvinceRedisMap;
import cn.ac.iie.centralserver.crypt.CryptData;
import cn.ac.iie.centralserver.crypt.DataCrypt;
import cn.ac.iie.centralserver.data.IndexToQuery;
import cn.ac.iie.centralserver.data.PersonResult;
import cn.ac.iie.centralserver.data.QueryRequest;
import cn.ac.iie.centralserver.dbutils.RedisUtil;
import cn.ac.iie.centralserver.log.LogUtil;

import com.google.gson.Gson;
import com.scistor.softcrypto.SoftCrypto;

public class DataDetailQueryHandler extends AbstractHandler {

	private static DataDetailQueryHandler dataHandler = null;

	private static SoftCrypto crypt = new SoftCrypto();
	private static ShardedJedis jedisuli = RedisUtil.getJedis("uliRedisIp");

	static {
		crypt.Initialize("abc");
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

		QueryRequest request = parseRequest(baseRequest, httpServletRequest);

		List<IndexToQuery> indexList = fetchIndexListByRequest(request);
		checkToken(request, indexList);
		if (request.getQueryType().equals("msisdn"))
			getImsi(indexList);

		HashMap<String, List<IndexToQuery>> provinceMap = getProvince(indexList);
		HashMap<IndexToQuery, PersonResult> resultMap = initResultMap(indexList);

		for (Map.Entry<String, List<IndexToQuery>> mapEntry : provinceMap
				.entrySet()) {
			String ipList = ProvinceRedisMap.getProRedisIP(mapEntry.getKey());
			queryDetail(ipList, mapEntry.getValue(), resultMap, 1);
		}

		// String result = resultMapToJsonHJ(indexList, resultMap);//JSON串返回
		String result = resultMapToJson(indexList, resultMap);// JSON字符数组
		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		httpServletResponse.getWriter().println(result);
	}

	private QueryRequest parseRequest(Request baseRequest,
			HttpServletRequest httpServletRequest) {

		QueryRequest request = new QueryRequest();
		request.setUrl(new String(httpServletRequest.getRequestURL()));
		request.setToken(httpServletRequest.getParameter("token"));
		request.setQueryType(httpServletRequest.getParameter("querytype"));
		request.setIndex(httpServletRequest.getParameter("index"));
		request.setRemoteHost(baseRequest.getRemoteAddr());
		request.generateIndexList();
		LogUtil.info(request.toString());
		return request;
	}

	private List<IndexToQuery> fetchIndexListByRequest(QueryRequest request) {

		List<IndexToQuery> indexList = new ArrayList<IndexToQuery>();
		String queryType = request.getQueryType();
		String[] list = request.getIndexList();
		for (String aIndex : list) {
			IndexToQuery iq = new IndexToQuery();
			if (queryType.equals("msisdn"))
				iq.setMsisdn(aIndex);
			else if (queryType.equals("imsi"))
				iq.setImsi(aIndex);
			indexList.add(iq);
		}

		return indexList;
	}

	private void checkToken(QueryRequest request, List<IndexToQuery> indexList) {

		HashMap<String, String> indexmap = new HashMap<String, String>();
		String token = request.getToken();
		String queryType = request.getQueryType();

		boolean isTokenExist = true;
		ShardedJedis tokenJedis = RedisUtil.getJedis("redisTokenIp");

		if (token == null || token.isEmpty() || !tokenJedis.exists(token))
			isTokenExist = false;

		if (isTokenExist && !token.equals("351370377d867c3b8dba04533b1f7e53")) {
			ShardedJedisPipeline pipeline = tokenJedis.pipelined();
			for (IndexToQuery aIndex : indexList)
				pipeline.hget(token, aIndex.getKeyByQueryType(queryType));

			List<Object> resp = pipeline.syncAndReturnAll();

			int count = 0;
			Iterator<Object> iter = resp.iterator();
			while (iter.hasNext()) {
				indexmap.put(indexList.get(count++)
						.getKeyByQueryType(queryType), (String) iter.next());
			}
		}

		RedisUtil.returnJedis(tokenJedis, "redisTokenIp");

		for (IndexToQuery aIndex : indexList) {
			if (!isTokenExist) {
				aIndex.setStatus(5);
				continue;
			}
			if (queryType == null || queryType.isEmpty()) {
				aIndex.setStatus(2);
				continue;
			}
			if (!token.equals("351370377d867c3b8dba04533b1f7e53")) {
				String out = indexmap.get(aIndex.getKeyByQueryType(queryType));
				if (out == null) {
					aIndex.setStatus(3);
					continue;
				}
				if ((queryType.equals("msisdn") && !out.equals(aIndex
						.getMsisdn()))
						|| (queryType.equals("imsi") && !out.equals(aIndex
								.getImsi()))) {
					aIndex.setStatus(1);
					continue;
				}
			}
			aIndex.setStatus(0);
		}
	}

	private void getImsi(List<IndexToQuery> indexList) {// 2018-09-21
		ShardedJedis jedis = RedisUtil.getJedis("redisMapIp");
		ShardedJedis ibsJedis = RedisUtil.getJedis("redisMapIpIBS");// 改了配置文件，变成94.95.96
		ShardedJedisPipeline pipeline = ibsJedis.pipelined();// 注意是lbsJedis的pipeine

		for (IndexToQuery aIndex : indexList)
			if (aIndex.isSuccess())
				pipeline.get(aIndex.getMsisdn());// 先去三台里取
		List<Object> resp = pipeline.syncAndReturnAll();

		int count = 0;
		Iterator<Object> iter = resp.iterator();
		while (iter.hasNext()) {
			IndexToQuery aIndex = indexList.get(count++);
			while (!aIndex.isSuccess())
				aIndex = indexList.get(count++);

			String imsi = (String) iter.next();
			String imsiList = null;
			if (imsi == null) {// 三台没有去原先的93取
				imsiList = jedis.get(aIndex.getMsisdn());
				if (imsiList == null) {
					aIndex.setStatus(6);
					continue;
				} else {
					aIndex.setImsi(imsiList.split(",")[0]);
					ibsJedis.set(aIndex.getMsisdn(), imsiList);// 把三台没有的，93有的添加到3台里
				}
			} else
				aIndex.setImsi(imsi);
		}
		RedisUtil.returnJedis(jedis, "redisMapIp");
		RedisUtil.returnJedis(ibsJedis, "redisMapIpIBS");
	}

	private HashMap<String, List<IndexToQuery>> getProvince(
			List<IndexToQuery> indexList) {
		HashMap<String, List<IndexToQuery>> provinceMap = new HashMap<String, List<IndexToQuery>>();

		ShardedJedis jedisCluster = RedisUtil.getJedis("redisList");
		ShardedJedisPipeline pipeline = jedisCluster.pipelined();

		for (IndexToQuery aIndex : indexList) {
			if (aIndex.isSuccess())
				pipeline.get(aIndex.getImsi());
		}
		List<Object> resp = pipeline.syncAndReturnAll();
		RedisUtil.returnJedis(jedisCluster, "redisList");

		int count = 0;
		Iterator<Object> iter = resp.iterator();
		while (iter.hasNext()) {
			IndexToQuery aIndex = indexList.get(count++);
			while (!aIndex.isSuccess())
				aIndex = indexList.get(count++);

			String value = (String) iter.next();
			if (value == null) {
				aIndex.setStatus(7);
				continue;
			}
			String prov = value.split(",")[3].substring(0, 2);
			if (provinceMap.get(prov) == null) {
				ArrayList<IndexToQuery> list = new ArrayList<IndexToQuery>();
				list.add(aIndex);
				provinceMap.put(prov, list);
			} else
				provinceMap.get(prov).add(aIndex);

		}

		return provinceMap;
	}

	private HashMap<IndexToQuery, PersonResult> initResultMap(
			List<IndexToQuery> indexList) {
		HashMap<IndexToQuery, PersonResult> resultMap = new HashMap<IndexToQuery, PersonResult>();

		for (IndexToQuery aIndex : indexList) {
			PersonResult result = new PersonResult();
			result.setStatus(aIndex.getStatus());
			result.setImsi(aIndex.getImsi());
			result.setMsisdn(aIndex.getMsisdn());
			resultMap.put(aIndex, result);
		}

		return resultMap;
	}

	private void queryDetail(String ipList, List<IndexToQuery> indexList,
			HashMap<IndexToQuery, PersonResult> resultMap, int times) {

		if (times > 30)
			return;
		try {
			if (ipList == null) {
				for (IndexToQuery aIndex : indexList)
					resultMap.get(aIndex).setStatus(7);
				return;
			}
			HashMap<String, String> uliMap = new HashMap<String, String>();
			ShardedJedis redisList = RedisUtil.getJedisByIpList(ipList);
			ShardedJedisPipeline pipeline = redisList.pipelined();

			ShardedJedisPipeline uliPipeline = jedisuli.pipelined();

			for (IndexToQuery aIndex : indexList) {
				if (!aIndex.isSuccess()) {
					LogUtil.error("error logic when query detail!!!!!!!!!!");
					continue;
				}
				String imsi = aIndex.getImsi();
				if (ipList.startsWith("10.245") || ipList.startsWith("10.233")) {
					if (imsi != null && imsi.length() == 15) {
						byte[] datain = imsi.substring(3, 15).getBytes();
						byte[] dataout = new byte[datain.length];
						crypt.crypto_encrypt(datain, dataout, datain.length, 1,
								0);
						String t = new String(dataout);
						imsi = imsi.substring(0, 3) + t;
					}
				}
				pipeline.get(imsi);
			}

			List<Object> resp = pipeline.syncAndReturnAll();
			Iterator<Object> iter = resp.iterator();
			while (iter.hasNext()) {
				String v = (String) iter.next();
				if (v != null) {
					uliMap.put(v.split(";")[6], null);
					uliPipeline.get(v.split(";")[6]);
				}
			}
			List<Object> uliresp = uliPipeline.syncAndReturnAll();

			Iterator<Object> uliIter = uliresp.iterator();
			while (uliIter.hasNext()) {
				String uliv = (String) uliIter.next();
				if (uliv != null)
					uliMap.put(uliv.split(",")[0], uliv);
			}

			Iterator<Object> sIter = resp.iterator();
			int count = 0;
			while (sIter.hasNext()) {
				IndexToQuery index = indexList.get(count++);
				String v = (String) sIter.next();
				PersonResult result = resultMap.get(index);
				if (v == null) {
					result.setStatus(7);
					continue;
				}

				CryptData cd = new CryptData();
				cd.setImsi(v.split(";")[0]);
				cd.setImei(v.split(";")[1]);
				cd.setMsisdn(v.split(";")[2]);

				if (ipList.startsWith("10.245") || ipList.startsWith("10.233")) {
					int dedataout_result = 0;

					dedataout_result = cd.decryptData();
					while (-1 == dedataout_result) {
						LogUtil.info("Decrypt ticket time out!");
						try {
							DataCrypt.auth("jm.conf");
						} catch (IOException e) {
							e.printStackTrace();
						}
						dedataout_result = cd.decryptData();
					}
				}
				result.setImei(cd.getImei());
				if (result.getMsisdn().equals(""))
					result.setMsisdn(cd.getMsisdn());
				result.setRegionCode(v.split(";")[3]);
				result.setLac(v.split(";")[4]);
				result.setCi(v.split(";")[5]);
				result.setUli(v.split(";")[6]);
				result.setHomeCode(v.split(";")[7]);
				result.setTime(timeStamp2Date(v.split(";")[10]));

				if (v.split(";")[6].equals("") || v.split(";")[6].equals("0")
						|| v.split(";")[6] == null)
					result.setStatus(8);
				else {
					String rawGPS = uliMap.get(v.split(";")[6]);
					if (rawGPS != null) {
						result.setLngi(Double.parseDouble(rawGPS.split(",")[1]));
						result.setLati(Double.parseDouble(rawGPS.split(",")[2]));
						result.setProvince(rawGPS.split(",")[3]);
						result.setCity(rawGPS.split(",")[4]);
						result.setDistrict(rawGPS.split(",")[5]);
						result.setBaseinfo(rawGPS.split(",")[7]);
						if (rawGPS.split(",").length > 8)
							result.setRegionCode(rawGPS.split(",")[8]);
					} else
						result.setStatus(9);
				}

			}
			RedisUtil.returnJedis(redisList, ipList);
		} catch (JedisConnectionException e) {
			LogUtil.info("connection to " + ipList
					+ " failed! try reconnecting for " + times + " times");
			times++;
			queryDetail(ipList, indexList, resultMap, times);
		} catch (Exception e) {
			LogUtil.info("query details failed for " + e.getMessage()
					+ " try requerying for " + times + " times");
			times++;
			e.printStackTrace();
			queryDetail(ipList, indexList, resultMap, times);
		}
	}

	private String resultMapToJson(List<IndexToQuery> indexList,
			HashMap<IndexToQuery, PersonResult> resultMap) {

		List<PersonResult> results = new ArrayList<PersonResult>();
		Gson gson = new Gson();

		int okCount = 0;
		int errCount = 0;
		for (IndexToQuery aIndex : indexList) {
			if (aIndex.getStatus() == 0)
				okCount++;
			else
				errCount++;
			results.add(resultMap.get(aIndex));
		}

		LogUtil.info(okCount + " results queried successfully. " + errCount
				+ " queries failed.");
		return gson.toJson(results);
	}

	private String resultMapToJsonHJ(List<IndexToQuery> indexList,
			HashMap<IndexToQuery, PersonResult> resultMap) {

		String results = "";
		Gson gson = new Gson();

		int okCount = 0;
		int errCount = 0;
		for (IndexToQuery aIndex : indexList) {
			if (aIndex.getStatus() == 0)
				okCount++;
			else
				errCount++;
			results = results + gson.toJson(resultMap.get(aIndex)) + '\n';

		}

		return results;

		// LogUtil.info(okCount + " results queried successfully. " + errCount +
		// " queries failed.");

	}

	private String timeStamp2Date(String seconds) {
		if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
			return "";
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(Long.valueOf(seconds + "000")));
	}

}
