package cn.ac.iie.hy.centralserver.handler;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.hy.centralserver.data.TraceDBData;
import cn.ac.iie.hy.centralserver.data.TracePersonResult;
import cn.ac.iie.hy.centralserver.data.TracePosition;
import cn.ac.iie.hy.centralserver.data.UliAddress;
import cn.ac.iie.hy.centralserver.dbutils.RedisUtil;
import cn.ac.iie.hy.centralserver.log.LogUtil;
import cn.ac.iie.hy.centralserver.server.XClusterDataFetch;

import com.google.gson.Gson;

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
	public void handle(String string, Request baseRequest,
			HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException,
			ServletException {
		// TODO Auto-generated method stub
		String token = httpServletRequest.getParameter("token");
		String queryType = httpServletRequest.getParameter("querytype");
		String index = httpServletRequest.getParameter("index");
		String[] indexList = index.split(",");
		String starttime = httpServletRequest.getParameter("starttime");
		String endtime = httpServletRequest.getParameter("endtime");

		String remoteHost = baseRequest.getRemoteAddr();
		String reqID = String.valueOf(System.nanoTime());

		LogUtil.info(remoteHost + "\trequest trace query token:" + token
				+ "\tquerytype:" + queryType + "\tindex:" + index);

		ShardedJedis uliJedis = RedisUtil.getJedis("uliRedis");
		ShardedJedisPipeline uliPipeline = uliJedis.pipelined();

		ArrayList<TracePersonResult> result = new ArrayList<TracePersonResult>();
		for (String aIndex : indexList) {
			/*
			 * Turple2 r = checkToken(token, queryType, index); ret =
			 * r.getRet(); if (ret != 0) { break; } String out = r.getOut(); if
			 * (queryType.equals("msisdn")) { imsi = queryImsi(out); } else {
			 * imsi = out; } if (imsi == null) { ret = 6; break; }
			 */
			int ret = 0;

			TracePersonResult personResult = new TracePersonResult();
			ArrayList<TraceDBData> dbTraceList = queryTraceList(queryType,
					aIndex, starttime, endtime);
			if (dbTraceList == null || dbTraceList.size() == 0)
				ret = 7;

			ArrayList<TracePosition> tracePositionList = queryUliAddress(
					dbTraceList, uliPipeline);
			personResult.setStatus(ret);
			personResult.setTracelist(tracePositionList);

			result.add(personResult);
		}

		List<Object> resp = uliPipeline.syncAndReturnAll();
		HashMap<String, UliAddress> uliMap = getUliMap(resp);
		fillUliAddress(result, uliMap);

		Gson gson = new Gson();

		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		httpServletResponse.getWriter().println(gson.toJson(result));
	}

	private ArrayList<TraceDBData> queryTraceList(String queryType,
			String index, String starttime, String endtime) {
		ArrayList<TraceDBData> result = new ArrayList<TraceDBData>();
		try {
			try {
				// Class.forName("com.oscar.cluster.BulkDriver");

				// for test
				Class.forName("com.mysql.jdbc.Driver");
			} catch (java.lang.ClassNotFoundException e) {
				e.printStackTrace();
			}
			result = new XClusterDataFetch().getTraceData(queryType, index,
					starttime, endtime);
			;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private ArrayList<TracePosition> queryUliAddress(
			ArrayList<TraceDBData> dbTraceList, ShardedJedisPipeline uliPipeline) {

		if (dbTraceList == null || dbTraceList.size() == 0)
			return null;

		ArrayList<TracePosition> tracePositionList = new ArrayList<TracePosition>();
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
			String value = (String) rs;
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

	// private Turple2 checkToken(String token, String queryType, String index)
	// {
	// if (token == null || token.isEmpty()) {
	// return new Turple2(2, null);
	// }
	// if (queryType == null || queryType.isEmpty()) {
	// return new Turple2(2, null);
	// }
	// if (index == null || index.isEmpty()) {
	// return new Turple2(2, null);
	// }
	// Jedis confJedis = RedisUtil.getJedis();
	// if (!confJedis.exists(token)) {
	// RedisUtil.returnResource(confJedis);
	// return new Turple2(5, null);
	// }
	// String out = confJedis.hget(token, index);
	// if (out == null) {
	// RedisUtil.returnResource(confJedis);
	// return new Turple2(3, null);
	// }
	// if (!out.equals(index)) {
	// RedisUtil.returnResource(confJedis);
	// return new Turple2(1, out);
	// }
	// RedisUtil.returnResource(confJedis);
	// return new Turple2(0, out);
	// }

	private String timeStamp2Date(long stamp) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		return sdf.format(new Date(stamp * 1000));
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
			return "token不存在或非法";
		case 6:
			return "手机号映射缺失";
		case 7:
			return "查询结果为空";
		default:
			return "未知错误";
		}
	}
}
