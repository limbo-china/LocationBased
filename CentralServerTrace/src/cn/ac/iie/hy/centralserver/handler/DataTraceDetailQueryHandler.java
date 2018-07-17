package cn.ac.iie.hy.centralserver.handler;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import cn.ac.iie.hy.centralserver.data.TraceDBData;
import cn.ac.iie.hy.centralserver.data.TracePersonResult;
import cn.ac.iie.hy.centralserver.data.TracePosition;
import cn.ac.iie.hy.centralserver.server.XClusterDataFetch;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class DataTraceDetailQueryHandler extends AbstractHandler {

	private static DataTraceDetailQueryHandler dataTraceDetailQueryHandler = null;
	private static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataTraceQueryHandler.class.getName());
	}

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
		// TODO Auto-generated method stub
		String token = httpServletRequest.getParameter("token");
		String queryType = httpServletRequest.getParameter("querytype");
		String index = httpServletRequest.getParameter("index");
		String starttime = httpServletRequest.getParameter("starttime");
		String endtime = httpServletRequest.getParameter("endtime");

		String remoteHost = baseRequest.getRemoteAddr();
		int remotePort = baseRequest.getRemotePort();
		String reqID = String.valueOf(System.nanoTime());
		String jobID = reqID;

		int ret = 0;
		String result = null;
		String imsi = index;

		logger.info(remoteHost + "\trequest trace query token:" + token
				+ "\tquerytype:" + queryType + "\tindex:" + index);

		do {
			/*
			 * Turple2 r = checkToken(token, queryType, index); ret =
			 * r.getRet(); if (ret != 0) { break; } String out = r.getOut(); if
			 * (queryType.equals("msisdn")) { imsi = queryImsi(out); } else {
			 * imsi = out; } if (imsi == null) { ret = 6; break; }
			 */
			HashMap<String, String> addrMap = new HashMap<String, String>();
			addrMap.put("11", "10.244.69.247");
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

			ArrayList<TraceDBData> queryTraceList = queryTraceList(queryType,
					index, starttime, endtime);
			if (queryTraceList == null || queryTraceList.size() == 0) {
				ret = 7;
				break;
			}
			HashSet<String> areaSet = new HashSet<String>();
			for (TraceDBData metaData : queryTraceList) {
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
							addr, queryType, index, starttime, endtime);
					traceDetailList.addAll(traceDetail);
				}
			}
			if (traceDetailList == null || traceDetailList.size() == 0) {
				ret = 7;
				break;
			}
			result = traceList2Json(ret, traceDetailList);

		} while (false);

		if (ret != 0) {
			JsonObject element = new JsonObject();
			element.addProperty("status", ret);
			element.addProperty("reason", getReason(ret));
			element.addProperty("jobid", jobID);

			result = element.toString();
		}

		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		httpServletResponse.getWriter().println(result);
	}

	private ArrayList<TraceDBData> queryTraceDetailList(String addr,
			String queryType, String index, String starttime, String endtime) {
		ArrayList<TraceDBData> result = new ArrayList<TraceDBData>();
		try {
			try {
				Class.forName("com.oscar.cluster.BulkDriver");
			} catch (java.lang.ClassNotFoundException e) {
				e.printStackTrace();
			}
			result = new XClusterDataFetch(addr).getTraceData(queryType, index,
					starttime, endtime);
			;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private ArrayList<TraceDBData> queryTraceList(String queryType,
			String index, String starttime, String endtime) {
		ArrayList<TraceDBData> result = new ArrayList<TraceDBData>();
		try {
			try {
				Class.forName("com.oscar.cluster.BulkDriver");
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

	private String traceList2Json(int status,
			ArrayList<TraceDBData> queryTraceList) {
		if (status != 0) {
			return null;
		}
		TracePersonResult traceResult = new TracePersonResult();
		ArrayList<TracePosition> traceDataList = new ArrayList<TracePosition>();
		for (TraceDBData data : queryTraceList) {
			// if (traceResult.getImsi() == null
			// || traceResult.getImsi().equals("")
			// || traceResult.getImsi().equals("0")) {
			// traceResult.setImsi(data.getC_imsi());
			// }
			// if (traceResult.getImei() == null
			// || traceResult.getImei().equals("")
			// || traceResult.getImei().equals("0")) {
			// traceResult.setImei(data.getC_imei());
			// }
			// if (traceResult.getMsisdn() == null
			// || traceResult.getMsisdn().equals("")
			// || traceResult.getMsisdn().equals("0")) {
			// traceResult.setMsisdn(data.getC_msisdn());
			// }
			TracePosition traceData = new TracePosition();
			traceData.setUli(data.getC_uli());
			// traceData.setTime(data.getC_timestamp());
			traceDataList.add(traceData);
		}
		Gson gson = new Gson();
		traceResult.setTracelist(traceDataList);
		traceResult.setStatus(status);

		return gson.toJson(traceResult);

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
