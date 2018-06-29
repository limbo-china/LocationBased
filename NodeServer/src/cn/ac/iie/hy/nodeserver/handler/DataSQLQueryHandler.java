/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.nodeserver.handler;


import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.JsonObject;


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
    	String targetHost = httpServletRequest.getParameter("targethost");
    	String jobID = httpServletRequest.getParameter("jobid");
    	
    	String remoteHost = baseRequest.getRemoteAddr();
        int remotePort = baseRequest.getRemotePort();
        String reqID = String.valueOf(System.nanoTime());
        
        if(jobID == null){
    		jobID = reqID;
    	}

        int ret;
        do{
        	ret = paramCheck(token, sql, targetHost, jobID);
        	if(ret != 0){
        		break;
        	}
        	ret = checkToken(token);
        	if(ret != 0){
        		break;
        	}
        	
        }while(false);
        //System.out.println(sql);
        
        //System.out.println(query);
        JsonObject element = new JsonObject();
        element.addProperty("status", ret);
        
        String result = element.toString();
        
        httpServletResponse.setContentType("text/json;charset=utf-8");  
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);  
        baseRequest.setHandled(true);
        httpServletResponse.getWriter().println(result);
        
    }
    //临时 测试token
    private int checkToken(String token){
    	if(token.equals("10fc5bd98774413b8ccecf4fc6c60b6a")){
    		return 0;
    	}
    	else{
    		return 5;
    	}
    }

    private int paramCheck(String token, String sql, String targetHost, String jobID){
    	if(token == null  || sql == null || targetHost == null || jobID == null){
    		return 2;
    	}
    	return 0;
    }
}
