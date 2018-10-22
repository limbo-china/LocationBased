package cn.ac.iie.centralserver.trace.handler;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.centralserver.trace.data.QueryRequest;
import cn.ac.iie.centralserver.trace.data.TraceDBData;
import cn.ac.iie.centralserver.trace.data.TracePersonResult;
import cn.ac.iie.centralserver.trace.data.TracePosition;
import cn.ac.iie.centralserver.trace.data.UliAddress;
import cn.ac.iie.centralserver.trace.db.RedisUtil;
import cn.ac.iie.centralserver.trace.log.LogUtil;
import cn.ac.iie.centralserver.trace.server.XClusterDataFetch;

import com.google.gson.Gson;

public class DataTraceDetailQueryHandler extends AbstractHandler {

	private static DataTraceDetailQueryHandler dataTraceDetailQueryHandler = null;

	private DataTraceDetailQueryHandler() {
	}

	public static DataTraceDetailQueryHandler getHandler() {
		if (dataTraceDetailQueryHandler != null)
			return dataTraceDetailQueryHandler;
		dataTraceDetailQueryHandler = new DataTraceDetailQueryHandler();
		return dataTraceDetailQueryHandler;
	}

	@Override
	public void handle(String string, Request baseRequest,
			HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException,
			ServletException {
		QueryRequest request = parseRequest(baseRequest, httpServletRequest);
		List<String> indexList = fetchIndexListByRequest(request);

		if (!checkToken(request, indexList)) {
			TracePersonResult personResult = new TracePersonResult();
			personResult.setStatus(5);
			Gson gson = new Gson();

			httpServletResponse.setContentType("text/json;charset=utf-8");
			httpServletResponse.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			httpServletResponse.getWriter().println(gson.toJson(personResult));
			return;
		}

		ShardedJedis uliJedis = RedisUtil.getJedis("uliRedis");
		ShardedJedisPipeline uliPipeline = uliJedis.pipelined();

		HashMap<String, String> addrMap = new HashMap<String, String>();
		addrMap.put("11", "10.224.69.247");
		addrMap.put("12", "10.238.69.247");
		addrMap.put("13", "10.231.69.247");
		addrMap.put("14", "10.230.69.247");
		addrMap.put("15", "10.225.69.247");
		addrMap.put("21", "10.233.69.247");
		addrMap.put("22", "10.237.69.247");
		addrMap.put("23", "10.236.69.247");
		addrMap.put("31", "10.240.69.247");
		addrMap.put("32", "10.241.69.247");
		addrMap.put("33", "10.244.69.247");
		addrMap.put("34", "10.243.69.247");
		addrMap.put("35", "10.254.69.247");
		addrMap.put("36", "10.251.69.247");
		addrMap.put("37", "10.245.69.247");
		addrMap.put("41", "10.242.69.247");
		addrMap.put("42", "10.247.69.247");
		addrMap.put("43", "10.250.69.247");
		addrMap.put("44", "10.246.69.247");
		addrMap.put("45", "10.253.69.247");
		addrMap.put("46", "10.252.69.247");
		addrMap.put("50", "10.227.69.247");
		addrMap.put("51", "10.228.69.102");
		addrMap.put("52", "10.249.69.247");
		addrMap.put("53", "10.248.69.247");
		addrMap.put("54", "10.226.69.247");
		addrMap.put("61", "10.235.69.247");
		addrMap.put("62", "10.232.69.247");
		addrMap.put("63", "10.234.69.247");
		addrMap.put("64", "10.229.69.247");
		addrMap.put("65", "10.239.69.247");
		ArrayList<TracePersonResult> result = new ArrayList<TracePersonResult>();
		for (String aIndex : indexList) {
			int ret = 0;

			TracePersonResult personResult = new TracePersonResult();
			ArrayList<TraceDBData> dbTraceList = queryTraceList(request, aIndex);
			if (dbTraceList == null || dbTraceList.size() == 0)
				ret = 7;
			HashSet<String> areaSet = new HashSet<String>();
			for (TraceDBData metaData : dbTraceList) {
				if (metaData.getC_areacode() != null
						&& metaData.getC_areacode().length() == 6) {
					areaSet.add(metaData.getC_areacode().substring(0, 2));
				}
			}
			ArrayList<TraceDBData> traceDetailList = new ArrayList<TraceDBData>();
			for (String areacode : areaSet) {
				if (addrMap.containsKey(areacode)) {
					String addr = addrMap.get(areacode);
					ArrayList<TraceDBData> traceDetail = queryTraceDetailList(
							addr, request, aIndex);
					traceDetailList.addAll(traceDetail);
				}
			}
			if (traceDetailList == null || traceDetailList.size() == 0)
				ret = 7;
			ArrayList<TracePosition> tracePositionList = queryUliAddress(
					traceDetailList, uliPipeline);
			personResult.setStatus(ret);
			personResult.setTracelist(tracePositionList);

			result.add(personResult);
		}
		List<Object> resp = uliPipeline.syncAndReturnAll();
		HashMap<String, UliAddress> uliMap = getUliMap(resp);
		fillUliAddress(result, uliMap);

		RedisUtil.returnJedis(uliJedis, "uliRedis");
		Gson gson = new Gson();

		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		httpServletResponse.getWriter().println(gson.toJson(result));
	}

	private QueryRequest parseRequest(Request baseRequest,
			HttpServletRequest httpServletRequest) {

		QueryRequest request = new QueryRequest();
		request.setUrl(new String(httpServletRequest.getRequestURL()));
		request.setToken(httpServletRequest.getParameter("token"));
		request.setQueryType(httpServletRequest.getParameter("querytype"));
		request.setIndex(httpServletRequest.getParameter("index"));
		request.setStartTime(httpServletRequest.getParameter("starttime"));
		request.setEndTime(httpServletRequest.getParameter("endtime"));
		request.setRemoteHost(baseRequest.getRemoteAddr());
		request.generateIndexList();
		LogUtil.info(request.toString());
		return request;
	}

	private List<String> fetchIndexListByRequest(QueryRequest request) {

		return Arrays.asList(request.getIndexList());
	}

	private boolean checkToken(QueryRequest request, List<String> indexList) {

		HashMap<String, String> indexmap = new HashMap<String, String>();
		String token = request.getToken();

		boolean isTokenExist = true;
		ShardedJedis tokenJedis = RedisUtil.getJedis("redisTokenIp");

		if (token == null || token.isEmpty() || !tokenJedis.exists(token))
			isTokenExist = false;

		if (isTokenExist && !token.equals("351370377d867c3b8dba04533b1f7e53")) {
			ShardedJedisPipeline pipeline = tokenJedis.pipelined();
			for (String aIndex : indexList)
				pipeline.hget(token, aIndex);

			List<Object> resp = pipeline.syncAndReturnAll();

			int count = 0;
			Iterator<Object> iter = resp.iterator();
			while (iter.hasNext()) {
				indexmap.put(indexList.get(count++), (String) iter.next());
			}

			for (Iterator<String> i = indexList.iterator(); i.hasNext();) {
				String aIndex = i.next();
				String out = indexmap.get(aIndex);
				if (out == null) {
					i.remove();
				}
			}
		}

		RedisUtil.returnJedis(tokenJedis, "redisTokenIp");

		return isTokenExist;
	}

	private ArrayList<TraceDBData> queryTraceList(QueryRequest request,
			String index) {
		ArrayList<TraceDBData> result = new ArrayList<TraceDBData>();
		try {
			try {
				Class.forName("com.oscar.cluster.BulkDriver");
			} catch (java.lang.ClassNotFoundException e) {
				e.printStackTrace();
			}
			result = new XClusterDataFetch().getTraceData(
					request.getQueryType(), index, request.getStartTime(),
					request.getEndTime());
			;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private ArrayList<TraceDBData> queryTraceDetailList(String addr,
			QueryRequest request, String index) {
		ArrayList<TraceDBData> result = new ArrayList<TraceDBData>();
		try {
			try {
				Class.forName("com.oscar.cluster.BulkDriver");
			} catch (java.lang.ClassNotFoundException e) {
				e.printStackTrace();
			}
			result = new XClusterDataFetch(addr).getTraceData(
					request.getQueryType(), index, request.getStartTime(),
					request.getEndTime());
			;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private ArrayList<TracePosition> queryUliAddress(
			ArrayList<TraceDBData> dbTraceList, ShardedJedisPipeline uliPipeline) {

		ArrayList<TracePosition> tracePositionList = new ArrayList<TracePosition>();

		if (dbTraceList == null || dbTraceList.size() == 0)
			return tracePositionList;

		for (TraceDBData data : dbTraceList) {
			TracePosition tracePosition = new TracePosition();
			tracePosition.setImsi(data.getC_imsi());
			tracePosition.setImei(data.getC_imei());
			tracePosition.setMsisdn(data.getC_msisdn());
			tracePosition.setUli(data.getC_uli());
			tracePosition.setTime(timeStamp2Date(data.getC_timestamp()));
			tracePositionList.add(tracePosition);

			uliPipeline.get(data.getC_uli());
		}

		return tracePositionList;
	}

	private HashMap<String, UliAddress> getUliMap(List<Object> resp) {
		HashMap<String, UliAddress> uliMap = new HashMap<String, UliAddress>();
		for (Object rs : resp) {
			if (rs == null)
				continue;
			String value = (String) rs;
			if (value.length() < 8)
				continue;
			UliAddress uliAddress = new UliAddress();
			uliAddress.setUli(value.split(",")[0]);
			uliAddress.setLongi(Float.parseFloat(value.split(",")[1]));
			uliAddress.setLati(Float.parseFloat(value.split(",")[2]));
			uliAddress.setProvince(value.split(",")[3]);
			uliAddress.setCity(value.split(",")[4]);
			uliAddress.setDistrict(value.split(",")[5]);
			uliAddress.setBaseInfo(value.split(",")[7]);
			if (value.split(",").length > 8)
				uliAddress.setAreacode(value.split(",")[8]);

			uliMap.put(value.split(",")[0], uliAddress);
		}
		return uliMap;
	}

	private void fillUliAddress(ArrayList<TracePersonResult> result,
			HashMap<String, UliAddress> uliMap) {

		for (TracePersonResult personResult : result) {
			if (personResult.isEmpty())
				continue;
			for (TracePosition position : personResult.getTracelist()) {
				if (position.getUli() != null) {
					UliAddress address = uliMap.get(position.getUli());
					if (address != null) {
						position.setLongi(address.getLongi());
						position.setLati(address.getLati());
						position.setProvince(address.getProvince());
						position.setCity(address.getCity());
						position.setDistrict(address.getDistrict());
						position.setBaseInfo(address.getBaseInfo());
						position.setRegionCode(address.getAreacode());
					}
				}
			}
		}
	}

	private String timeStamp2Date(long stamp) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		return sdf.format(new Date(stamp * 1000));

	}
}
