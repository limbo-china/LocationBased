package cn.ac.iie.centralserver.trace.action;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.struts2.ServletActionContext;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.centralserver.trace.bean.QueryRequest;
import cn.ac.iie.centralserver.trace.bean.TraceDBData;
import cn.ac.iie.centralserver.trace.bean.TracePersonResult;
import cn.ac.iie.centralserver.trace.bean.TracePosition;
import cn.ac.iie.centralserver.trace.bean.UliAddress;
import cn.ac.iie.centralserver.trace.dao.XClusterDataFetch;
import cn.ac.iie.centralserver.trace.db.RedisUtil;
import cn.ac.iie.centralserver.trace.log.LogUtil;

import com.google.gson.Gson;
import com.opensymphony.xwork2.ActionSupport;

public class TraceQueryAction extends ActionSupport {

	private static final long serialVersionUID = 1L;

	public String doTraceQuery() throws IOException {

		HttpServletRequest httpServletRequest = ServletActionContext
				.getRequest();
		HttpServletResponse httpServletResponse = ServletActionContext
				.getResponse();
		QueryRequest request = parseRequest(httpServletRequest);
		List<String> indexList = fetchIndexListByRequest(request);

		/*
		 * if (!checkToken(request, indexList)) { TracePersonResult personResult
		 * = new TracePersonResult(); personResult.setStatus(5); Gson gson = new
		 * Gson();
		 * 
		 * httpServletResponse.setContentType("text/json;charset=utf-8");
		 * httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		 * baseRequest.setHandled(true);
		 * httpServletResponse.getWriter().println(gson.toJson(personResult));
		 * return; }
		 * 
		 * ShardedJedis uliJedis = RedisUtil.getJedis("uliRedis");
		 * ShardedJedisPipeline uliPipeline = uliJedis.pipelined();
		 */

		ArrayList<TracePersonResult> result = new ArrayList<TracePersonResult>();
		for (String aIndex : indexList) {
			int ret = 0;

			TracePersonResult personResult = new TracePersonResult();
			ArrayList<TraceDBData> dbTraceList = queryTraceList(request, aIndex);
			if (dbTraceList == null || dbTraceList.size() == 0)
				ret = 7;
			Collections.sort(dbTraceList);
			ArrayList<TracePosition> tracePositionList = queryUliAddress(dbTraceList/*
																					 * ,
																					 * uliPipeline
																					 */);
			personResult.setStatus(ret);
			personResult.setTracelist(tracePositionList);

			result.add(personResult);
		}
		/*
		 * List<Object> resp = uliPipeline.syncAndReturnAll(); HashMap<String,
		 * UliAddress> uliMap = getUliMap(resp);
		 * 
		 * fillUliAddress(result, uliMap);
		 * 
		 * 
		 * RedisUtil.returnJedis(uliJedis, "uliRedis");
		 */
		Gson gson = new Gson();

		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		httpServletResponse.getWriter().println(gson.toJson(result));
		return NONE;
	}

	private QueryRequest parseRequest(HttpServletRequest httpServletRequest) {

		QueryRequest request = new QueryRequest();
		request.setUrl(new String(httpServletRequest.getRequestURL()));
		request.setToken(httpServletRequest.getParameter("token"));
		request.setQueryType(httpServletRequest.getParameter("querytype"));
		request.setIndex(httpServletRequest.getParameter("index"));
		request.setStartTime(httpServletRequest.getParameter("starttime"));
		request.setEndTime(httpServletRequest.getParameter("endtime"));
		request.setRemoteHost(httpServletRequest.getRemoteAddr());
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
				// Class.forName("com.oscar.cluster.BulkDriver");

				// for ssh test
				Class.forName("com.mysql.jdbc.Driver");
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

	private ArrayList<TracePosition> queryUliAddress(
			ArrayList<TraceDBData> dbTraceList/*
											 * , ShardedJedisPipeline
											 * uliPipeline
											 */) {

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

			/* uliPipeline.get(data.getC_uli()); */
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
