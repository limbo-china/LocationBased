/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.datadispatch.server;

import cn.ac.iie.hy.datadispatch.config.Configuration;
import cn.ac.iie.hy.datadispatch.crypt.DataCrypt;
import cn.ac.iie.hy.datadispatch.handler.DataDispatchHandler;
import cn.ac.iie.hy.datadispatch.handler.DataDispatcher;
import cn.ac.iie.hy.datadispatch.metadata.RoamData;
import cn.ac.iie.hy.datadispatch.metadata.SMetaData;
import cn.ac.iie.hy.datadispatch.task.DBLoadTask;
import cn.ac.iie.hy.datadispatch.utils.HomeCodeMap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import com.google.gson.Gson;



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
public class DataDispatchServer {

	static Server server = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDispatchServer.class.getName());
	}

	public static void showUsage() {
		System.out.println("Usage:java -jar ");
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {
		try {
			init();
			startup();
		} catch (Exception ex) {
			logger.error("starting data dispatcher server is failed for " + ex.getMessage(), ex);
		}
		
		System.exit(0);
	}

	public static void loadUrl() {
		HttpClient httpClient = new DefaultHttpClient();
		String pubUrl = "http://10.213.69.20:8010/infopub/?op=getLoadServerList";
		Boolean ret = true;
		try {

			HttpPost httppost = new HttpPost(pubUrl);
			HttpResponse response = httpClient.execute(httppost);
			HttpEntity entity = response.getEntity();  
	        
	        String body = null;  
	        try {  
	            body = EntityUtils.toString(entity);  
	        } catch (ParseException e) {  
	            e.printStackTrace();  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }
	        
			//logger.warn(body);
	        body = body.replaceAll("#", "");
	        String[] urls = body.split("\n");
	        DBLoadTask.setUrlsList(urls);
			httppost.releaseConnection();
		} catch (Exception ex) {
			ret = false;
			ex.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}
	}
	
	private static void startup() throws Exception {
		HomeCodeMap.loadIndex("homelist.txt");
		
//		List<RoamData> rdList = new ArrayList<RoamData>();
//		
//		rdList.add(new RoamData("53","530000","0000","8618638585876",1527474897,0));
//		rdList.add(new RoamData("41","410105","0371","8618638585876",1527666122,1));
//		
//        if(!rdList.isEmpty()){
//			DataDispatcher.runRabbitTask(rdList);
//		}
        
		logger.info("starting data dispatch server...");
		server.start();
		logger.info("start data dispatch server successfully");
    	
		server.join();
		
		
    	
	}

	private static void init() throws Exception {
		//DataCrypt.auth("jm.conf");
		
		String configurationFileName = "data-dispatcher.properties";
		logger.info("initializing data dispatch server...");
		logger.info("getting configuration from configuration file " + configurationFileName);
		Configuration conf = Configuration.getConfiguration(configurationFileName);
		if (conf == null) {
			throw new Exception("reading " + configurationFileName + " is failed.");
		}

		String serverIP = conf.getString("jettyServerIP", "");
		if (serverIP.isEmpty()) {
			throw new Exception("definition jettyServerIP is not found in " + configurationFileName);
		}

		int serverPort = conf.getInt("jettyServerPort", -1);
		if (serverPort == -1) {
			throw new Exception("definition jettyServerPort is not found in " + configurationFileName);
		}

		int serverThreadPoolSize = conf.getInt("jettyServerThreadPoolSize", -1);
		if (serverThreadPoolSize == -1) {
			throw new Exception("definition jettyServerThreadPoolSize is not found in " + configurationFileName);
		}

		loadUrl();
		
		server = new Server(serverPort);

		ContextHandler dataLoadContext = new ContextHandler("/dataload");
		DataDispatchHandler dataDispatchHandler = DataDispatchHandler.getDataDispatchHandler();

		if (dataDispatchHandler == null) {
			throw new Exception("initializing dataDispatchHandler failed");
		}
		dataLoadContext.setHandler(dataDispatchHandler);

		ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.setHandlers(new Handler[] { dataLoadContext });
		server.setHandler(contexts);
		logger.info("intialize data dispatch server successfully");
	}
}
