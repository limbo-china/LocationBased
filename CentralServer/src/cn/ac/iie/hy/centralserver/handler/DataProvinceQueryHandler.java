/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.centralserver.handler;


import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.JsonObject;

import cn.ac.iie.hy.centralserver.dbutils.RedisUtil;
import cn.ac.iie.hy.centralserver.task.ProvinceQueryTask;
import redis.clients.jedis.Jedis;

public class DataProvinceQueryHandler extends AbstractHandler {

	private static DataProvinceQueryHandler dataHandler = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataProvinceQueryHandler.class.getName());
	}

	private DataProvinceQueryHandler() {
	}

	public static DataProvinceQueryHandler getHandler() {
		if (dataHandler != null) {
			return dataHandler;
		}
		dataHandler = new DataProvinceQueryHandler();
		return dataHandler;
	}

	@Override
	public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException, ServletException {
		String token = httpServletRequest.getParameter("token");
		String queryType = httpServletRequest.getParameter("querytype");
		String index = httpServletRequest.getParameter("index");
		String remoteHost = baseRequest.getRemoteAddr();
		int remotePort = baseRequest.getRemotePort();
		String reqID = String.valueOf(System.nanoTime());
		int ret = 0;
		String result = null;
		
        logger.info(remoteHost +  "request pro query token:" + token + " querytype:" + queryType + " index:" + index);
		do {
			ret = paramCheck(token, queryType, index);
			if (ret != 0) {
				break;
			}
			ret = checkToken(token);
			if (ret != 0) {
				break;
			}
			//test token
			if(token.equals("hjfc5bd98774413b8ccecf4fc6c60b6a")){
				if(!index.equals("8617710326177")){
					ret = 5;
					break;
				}
			}
			result = new ProvinceQueryTask().queryProvinceResult(queryType, index);
			if (result == null) {
				ret = 6;
				break;
			}

		} while (false);

		if (ret != 0) {
			JsonObject element = new JsonObject();
			element.addProperty("status", ret);
			element.addProperty("reason", getReason(ret));
			result = element.toString();
		}
		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		httpServletResponse.getWriter().println(result);

	}
//寰堝皯鐢ㄥ埌
	private int checkToken(String token) {
		String key = "PRO_QUERY_" + token;
		Jedis jedis = RedisUtil.getJedis();
		String quota = jedis.get(key);
		if(quota == null){
			return 3;
		}
		int result = Integer.parseInt(quota);
		if(result <= 0){
			return 4;
		}
		jedis.decr(key);
		RedisUtil.returnResource(jedis);
		return 0;
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
			return "鏌ヨ缁撴灉涓虹┖";
		default:
			return "鏈煡閿欒";
		}
	}

	private int paramCheck(String token, String querytype, String index) {
		if (token == null || querytype == null || index == null) {
			return 2;
		}

		return 0;
	}

	private String getRegion(Request req) {
		Object val = req.getParameter("region");
		return val == null ? "" : ((String) val).toLowerCase();
	}
}
