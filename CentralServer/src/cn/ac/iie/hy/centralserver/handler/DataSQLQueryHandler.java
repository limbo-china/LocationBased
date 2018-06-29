/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.centralserver.handler;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.JsonObject;

import cn.ac.iie.hy.centralserver.dbutils.RedisUtil;
import cn.ac.iie.hy.centralserver.task.SQLProTask;
import cn.ac.iie.hy.centralserver.task.ThreadPoolManager;
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
public class DataSQLQueryHandler extends AbstractHandler {

    
    private static DataSQLQueryHandler dataHandler = null;
    static Logger logger = null;
    private static ThreadPoolManager threadpool = ThreadPoolManager.newInstance();

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataSQLQueryHandler.class.getName());
    }

    private DataSQLQueryHandler() {
    }

    public static DataSQLQueryHandler getHandler() {
        if (dataHandler != null) {
            return dataHandler;
        }
        dataHandler = new DataSQLQueryHandler();
        return dataHandler;
    }

    @Override
    public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
    	String token = httpServletRequest.getParameter("token");
    	String sql = httpServletRequest.getParameter("sql");
    	String region = httpServletRequest.getParameter("region");
    	String targetHost = httpServletRequest.getParameter("targethost");
    	//String jobID = httpServletRequest.getParameter("jobid");
    	String remoteHost = baseRequest.getRemoteAddr();
        int remotePort = baseRequest.getRemotePort();
        String reqID = String.valueOf(System.nanoTime());
        String jobID = reqID;
        
        logger.info(remoteHost +  "request sql query token:" + token + " sql:" + sql + " region:" + region);
        
        int ret = 0;
        do{
        	ret = paramCheck(token, region, sql, targetHost, jobID);
        	if(ret != 0){
        		break;
        	}
        	ret = checkToken(token);
        	if(ret != 0){
        		break;
        	}
        	
        	String dbURL = getURL(region);
        	if(dbURL == null){
        		ret = 7;
        		break;
        	}
        	String taskHost =getHost(region);
        	threadpool.addExecuteTask(new SQLProTask(token, dbURL, sql, targetHost, jobID,taskHost));
        	
        }while(false);
        
        System.out.println("region:"+region);
        JsonObject element = new JsonObject();
        element.addProperty("status", ret);
        element.addProperty("reason", getReason(ret));
        element.addProperty("jobid", jobID);
        String result = element.toString();
        
        httpServletResponse.setContentType("text/json;charset=utf-8");  
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);  
        baseRequest.setHandled(true);  
        httpServletResponse.getWriter().println(result);
        
    }
    
    private int checkToken(String token) {
		String key = "SQL_QUERY_" + token;
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

    private int paramCheck(String token, String region, String sql, String targetHost, String jobID){
    	if(token == null || region == null || sql == null || targetHost == null || jobID == null){
    		return 2;
    	}
    	return 0;
    }
    
    private String getURL(String region) {
		FileInputStream fis = null;
		String result = null;
		try {
			fis = new FileInputStream("nodelist.properties");
			Properties pros = new Properties();
			pros.load(fis);
			result = pros.getProperty(region, null);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return result;
	}
    private String getHost(String region) {
		FileInputStream fis = null;
		String result = null;
		try {
			fis = new FileInputStream("hostlist.properties");
			Properties pros = new Properties();
			pros.load(fis);
			result = pros.getProperty(region, null);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return result;
	}
    
    
    private String getReason(int ret){
    	switch(ret){
    	case 0:
    		return "Right";
		case 1:
    		return "服务器错误";
		case 2:
			return "请求参数非法";
		case 3:
			return "权限校验失败";
		case 4:
			return "配额校验失败";
		case 5:
			return "token 不存在或非法";
		case 7:
			return "节点没有开启服务";
		default:
			return "未知错误";
    	}
    }
}
