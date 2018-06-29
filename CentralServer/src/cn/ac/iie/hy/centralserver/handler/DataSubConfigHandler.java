package cn.ac.iie.hy.centralserver.handler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.JsonObject;

import cn.ac.iie.hy.centralserver.task.SQLProTask;
import cn.ac.iie.hy.centralserver.task.SubConfigTask;
import cn.ac.iie.hy.centralserver.task.ThreadPoolManager;

public class DataSubConfigHandler extends AbstractHandler{

	private static DataSubConfigHandler dataHandler = null;
	static Logger logger = null;
	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataSubConfigHandler.class.getName());
	}
	
	public static DataSubConfigHandler getHandler() {
		if (dataHandler != null) {
			return dataHandler;
		}
		dataHandler = new DataSubConfigHandler();
		return dataHandler;
	}
	
	public void addTask(String configId){
		ThreadPoolManager threadManager = ThreadPoolManager.newInstance();
		threadManager.addExecuteTask(new SubConfigTask(configId));
	}
	
	@Override
	public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException, ServletException {
		String time = httpServletRequest.getParameter("time");
    	String indexType = httpServletRequest.getParameter("indexType");
    	String configId = httpServletRequest.getParameter("configId");
    	String crowdName = httpServletRequest.getParameter("crowdName");
    	String token = httpServletRequest.getParameter("token");
    	String remoteHost = baseRequest.getRemoteAddr();
        int remotePort = baseRequest.getRemotePort();
        String reqID = String.valueOf(System.nanoTime());
        String jobID = reqID;
        
        logger.info(remoteHost +  "request sql query token:" + token + " configId:" + configId + " crowdName:" + crowdName);
        
        addTask(configId);
        
        JsonObject element = new JsonObject();
        element.addProperty("status", 0);
        element.addProperty("jobid", jobID);
        String result = element.toString();
        
        httpServletResponse.setContentType("text/json;charset=utf-8");  
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);  
        baseRequest.setHandled(true);  
        httpServletResponse.getWriter().println(result);
		
	}

}
