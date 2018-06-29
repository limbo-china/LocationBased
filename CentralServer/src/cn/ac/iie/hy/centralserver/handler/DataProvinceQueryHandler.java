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
//很少用到
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
			return "查询结果为空";
		default:
			return "未知错误";
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
