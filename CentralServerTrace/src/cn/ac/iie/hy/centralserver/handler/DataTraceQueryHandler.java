package cn.ac.iie.hy.centralserver.handler;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import cn.ac.iie.hy.centralserver.data.TraceQueryData;
import cn.ac.iie.hy.centralserver.data.TraceQueryLBS;
import cn.ac.iie.hy.centralserver.data.TraceQueryResult;
import cn.ac.iie.hy.centralserver.dbutils.JedisUtilMap;
import cn.ac.iie.hy.centralserver.dbutils.RedisUtil;
import cn.ac.iie.hy.centralserver.server.XClusterDataFetch;
import redis.clients.jedis.Jedis;

public class DataTraceQueryHandler extends AbstractHandler{
	
	private static DataTraceQueryHandler dataTraceQueryHandler = null;
	private static Logger logger = null;
	
	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataTraceQueryHandler.class.getName());
	}
	
	private DataTraceQueryHandler(){}
	
	public static DataTraceQueryHandler getHandler(){
		if(dataTraceQueryHandler != null)
			return dataTraceQueryHandler;
		dataTraceQueryHandler = new DataTraceQueryHandler();
		return dataTraceQueryHandler;
	}
	
	@Override
	public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException, ServletException {
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
		
        logger.info(remoteHost + "\trequest trace query token:" + token + "\tquerytype:" + queryType + "\tindex:" + index);

		do {
			/*
			Turple2 r = checkToken(token, queryType, index);
			ret = r.getRet();
			if (ret != 0) {
				break;
			}
			String out = r.getOut();
			if (queryType.equals("msisdn")) {
				imsi = queryImsi(out);
			} else {
				imsi = out;
			}
			if (imsi == null) {
				ret = 6;
				break;
			}
			*/
			ArrayList<TraceQueryLBS> queryTraceList = queryTraceList(queryType, index, starttime, endtime);
			if(queryTraceList == null || queryTraceList.size() == 0){
				ret = 7;
				break;
			}
			result = traceList2Json(ret, queryTraceList);

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
	private ArrayList<TraceQueryLBS> queryTraceList(String queryType,String index, String starttime, String endtime){
		ArrayList<TraceQueryLBS> result = new ArrayList<TraceQueryLBS>();
		try {
			try {
				Class.forName("com.oscar.cluster.BulkDriver");
			} catch (java.lang.ClassNotFoundException e) {
				e.printStackTrace();
			}
			result = new XClusterDataFetch().getTraceData(queryType, index, starttime, endtime);;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	private String traceList2Json(int status, ArrayList<TraceQueryLBS> queryTraceList){
		if(status != 0){
			return null;
		}
		TraceQueryResult traceResult = new TraceQueryResult();
		ArrayList<TraceQueryData> traceDataList = new ArrayList<TraceQueryData>();
		for(TraceQueryLBS data : queryTraceList){
			if(traceResult.getImsi() == null || traceResult.getImsi().equals("") || traceResult.getImsi().equals("0")){
				traceResult.setImsi(data.getC_imsi());
			}
			if(traceResult.getImei() == null || traceResult.getImei().equals("") || traceResult.getImei().equals("0")){
				traceResult.setImei(data.getC_imei());
			}
			if(traceResult.getMsisdn() == null || traceResult.getMsisdn().equals("") || traceResult.getMsisdn().equals("0")){
				traceResult.setMsisdn(data.getC_msisdn());
			}
			TraceQueryData traceData = new TraceQueryData();
			traceData.setUli(data.getC_uli());
			traceData.setTime(data.getC_timestamp());
			traceDataList.add(traceData);
		}
		Gson gson = new Gson();
		traceResult.setTracelist(gson.toJson(traceDataList));
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

	private String queryImsi(String msisdn) {
		Jedis jedis = JedisUtilMap.getJedis();
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
