package cn.ac.iie.hy.nodeserver.handler;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.JsonObject;

import cn.ac.iie.hy.nodeserver.config.Configuration;
import cn.ac.iie.hy.nodeserver.dbutils.MySQLUtils;
import cn.ac.iie.hy.nodeserver.task.UserSubTask;


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
 *
 */
public class UserSubHandler extends AbstractHandler {

	private static UserSubHandler dataHandler = null;
	private static MySQLUtils mysqlutil = null;

	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataSQLQueryHandler.class.getName());
	}

	private UserSubHandler() {
	}

	public static UserSubHandler getHandler() {
		if (dataHandler != null) {
			return dataHandler;
		}
		if (mysqlutil == null) {
			String configurationFileName = "data-dispatcher.properties";
			Configuration conf = Configuration.getConfiguration(configurationFileName);
			if (conf == null) {
				logger.error("reading " + configurationFileName + " is failed.");
			}
			String mysqlUrl = conf.getString("mysqlUrl", "");
			if (mysqlUrl.isEmpty()) {
				return null;
			}
			mysqlutil = new MySQLUtils(mysqlUrl);
		}
		dataHandler = new UserSubHandler();
		return dataHandler;
	}

	@Override
	public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException, ServletException {

		String remoteHost = baseRequest.getRemoteAddr();
		int remotePort = baseRequest.getRemotePort();
		String reqID = String.valueOf(System.nanoTime());

		String token = httpServletRequest.getParameter("token");
		String action = httpServletRequest.getParameter("action");
		String indextype = httpServletRequest.getParameter("indextype");
		String indexlist = httpServletRequest.getParameter("indexlist");
		String targetHost = httpServletRequest.getParameter("targethost");
		String jobID = httpServletRequest.getParameter("jobid");
		if (jobID == null) {
			jobID = reqID;
		}

		logger.info("get user sub request from + " + remoteHost + " token:" + token + " action:" + action
				+ " indextype:" + indextype + " indexlist:" + indexlist + " host:" + targetHost + " jobID:" + jobID);

		if (jobID == null) {
			jobID = reqID;
		}

		int ret;
		do {
			ret = paramCheck(token, action, indextype, indexlist, targetHost, jobID);
			if (ret != 0) {
				break;
			}
			ret = checkToken(token, indexlist.split(";").length);
			if (ret != 0) {
				break;
			}
			if(action.equals("sub")){
				new UserSubTask().subUserTaskRedis(token, indextype, indexlist, jobID, targetHost);
			}
			else{
				new UserSubTask().cancelUserTaskRedis(token, indextype, indexlist, jobID, targetHost);

			}
		} while (false);
		// System.out.println(sql);

		JsonObject element = new JsonObject();
		element.addProperty("status", ret);
		element.addProperty("reason", getReason(ret));
		element.addProperty("jobid", jobID);

		String result = element.toString();
		logger.info(jobID + " response:" + result);

		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		httpServletResponse.getWriter().println(result);

	}

	private int checkToken(String token, int length) {
		String sql = "select user_name, usub_quota, usub_cur from t_user where token = '" + token + "';";
		Object[] param = new Object[] {};
		int[] type = {};
		List<HashMap> result = mysqlutil.executeQuery(sql, param, type);
		if (result.size() != 1) {
			return 5;
		}
		HashMap record = result.get(0);
		String userName = record.get("user_name").toString();
		int usub_quota = Integer.parseInt(record.get("usub_quota").toString());
		int usub_cur = Integer.parseInt(record.get("usub_cur").toString());

		if (length > usub_quota - usub_cur) {
			return 4;
		}
		int newCur = usub_cur + length;
		String updateSQL = "update t_user set usub_cur = " + length + " where token = '" + token + "';";
		int count = mysqlutil.executeUpdate(updateSQL, param);
		if (count != 1) {
			return 1;
		}

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

	private int paramCheck(String token, String action, String indextype, String indexlist, String targetHost,
			String jobID) {

		if (token == null || action == null || indextype == null || indexlist == null || targetHost == null
				|| jobID == null) {
			logger.info(jobID + " [PARAM ERROR]some param is null");
			return 2;
		}

		if (!action.equals("sub") && !action.equals("cancel")) {
			logger.info(jobID + " [PARAM ERROR]sub cancel only");
			return 2;
		}

		if (!indextype.equals("msisdn") && !indextype.equals("imsi") && !indextype.equals("imei")) {
			logger.info(jobID + " [PARAM ERROR]only support msisdn,imsi,imei");
			return 2;
		}

		if (indexlist.split(";").length == 0) {
			logger.info(jobID + " [PARAM ERROR]indexlist should not be zero");
			return 2;
		}

		return 0;
	}

}
