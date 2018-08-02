package cn.ac.iie.centralserver.trace.handler;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.Gson;

import cn.ac.iie.centralserver.trace.data.QueryRequest;
import cn.ac.iie.centralserver.trace.data.TraceDBData;
import cn.ac.iie.centralserver.trace.data.TracePersonResult;
import cn.ac.iie.centralserver.trace.data.TracePosition;
import cn.ac.iie.centralserver.trace.data.UliAddress;
import cn.ac.iie.centralserver.trace.db.RedisUtil;
import cn.ac.iie.centralserver.trace.log.LogUtil;
import cn.ac.iie.centralserver.trace.server.XClusterDataFetch;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

public class DataTraceQueryHandler extends AbstractHandler {

	private static DataTraceQueryHandler dataTraceQueryHandler = null;

	private DataTraceQueryHandler() {
	}

	public static DataTraceQueryHandler getHandler() {
		if (dataTraceQueryHandler != null)
			return dataTraceQueryHandler;
		dataTraceQueryHandler = new DataTraceQueryHandler();
		return dataTraceQueryHandler;
	}

	@Override
	public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException, ServletException {

		QueryRequest request = parseRequest(baseRequest, httpServletRequest);
		List<String> indexList = fetchIndexListByRequest(request);

		if (!checkToken(request)) {
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

		ArrayList<TracePersonResult> result = new ArrayList<TracePersonResult>();
		for (String aIndex : indexList) {
			int ret = 0;

			TracePersonResult personResult = new TracePersonResult();
			ArrayList<TraceDBData> dbTraceList = queryTraceList(request, aIndex);
			if (dbTraceList == null || dbTraceList.size() == 0)
				ret = 7;

			ArrayList<TracePosition> tracePositionList = queryUliAddress(dbTraceList, uliPipeline);
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

	private QueryRequest parseRequest(Request baseRequest, HttpServletRequest httpServletRequest) {

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

	private boolean checkToken(QueryRequest request) {

		// HashMap<String, String> indexmap = new HashMap<String, String>();
		String token = request.getToken();

		boolean isTokenExist = true;
		ShardedJedis tokenJedis = RedisUtil.getJedis("redisTokenIp");
		// ShardedJedisPipeline pipeline = tokenJedis.pipelined();
		if (token == null || token.isEmpty() || !tokenJedis.exists(token))
			isTokenExist = false;

		// if (isTokenExist) {
		// for (IndexToQuery aIndex : indexList)
		// pipeline.hget(token, aIndex.getKeyByQueryType(queryType));
		// }
		// List<Object> resp = pipeline.syncAndReturnAll();
		//
		// int count = 0;
		// Iterator<Object> iter = resp.iterator();
		// while (iter.hasNext()) {
		// indexmap.put(indexList.get(count++).getKeyByQueryType(queryType),
		// (String) iter.next());
		// }

		RedisUtil.returnJedis(tokenJedis, "redisTokenIp");

		return isTokenExist;
	}

	private ArrayList<TraceDBData> queryTraceList(QueryRequest request, String index) {
		ArrayList<TraceDBData> result = new ArrayList<TraceDBData>();
		try {
			try {
				Class.forName("com.oscar.cluster.BulkDriver");
			} catch (java.lang.ClassNotFoundException e) {
				e.printStackTrace();
			}
			result = new XClusterDataFetch().getTraceData(request.getQueryType(), index, request.getStartTime(),
					request.getEndTime());
			;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private ArrayList<TracePosition> queryUliAddress(ArrayList<TraceDBData> dbTraceList,
			ShardedJedisPipeline uliPipeline) {

		ArrayList<TracePosition> tracePositionList = new ArrayList<TracePosition>();

		if (dbTraceList == null || dbTraceList.size() == 0)
			return tracePositionList;

		for (TraceDBData data : dbTraceList) {
			TracePosition tracePosition = new TracePosition();
			tracePosition.setImsi(data.getC_imsi());
			tracePosition.setImei(data.getC_imei());
			tracePosition.setMsisdn(data.getC_msisdn());
			tracePosition.setUli(data.getC_uli());
			tracePosition.setRegionCode(data.getC_areacode());
			tracePosition.setTime(timeStamp2Date(data.getC_timestamp()));
			tracePositionList.add(tracePosition);

			uliPipeline.get(data.getC_uli());
		}

		return tracePositionList;
	}

	private HashMap<String, UliAddress> getUliMap(List<Object> resp) {
		HashMap<String, UliAddress> uliMap = new HashMap<String, UliAddress>();
		try {
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		return uliMap;
	}

	private void fillUliAddress(ArrayList<TracePersonResult> result, HashMap<String, UliAddress> uliMap) {

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