/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.datadispatch.server;

import cn.ac.iie.hy.datadispatch.config.Configuration;
import cn.ac.iie.hy.datadispatch.handler.DataDispatchHandler;
import cn.ac.iie.hy.datadispatch.task.DBLoadTask;

import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;



/**
 * 鈹佲攣鈹佲攣鈹佲攣绁炲吔鍑烘病鈹佲攣鈹佲攣鈹佲攣
 * 銆�銆�銆�鈹忊敁銆�銆�銆�鈹忊敁
 * 銆�銆�鈹忊敍鈹烩攣鈹佲攣鈹涒敾鈹�
 * 銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹�
 * 銆�銆�鈹冦��銆�銆�鈹併��銆�銆�鈹�
 * 銆�銆�鈹冦��鈹斥敍銆�鈹椻敵銆�鈹�
 * 銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹�
 * 銆�銆�鈹冦��銆�銆�鈹汇��銆�銆�鈹�
 * 銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹�
 * 銆�銆�鈹椻攣鈹撱��銆�銆�鈹忊攣鈹�
 * 銆�銆�銆�銆�鈹冦��銆�銆�鈹冪鍏戒繚浣�, 姘告棤BUG!
 * 銆�銆�銆�銆�鈹冦��銆�銆�鈹僀ode is far away from bug with the animal protecting
 * 銆�銆�銆�銆�鈹冦��銆�銆�鈹椻攣鈹佲攣鈹�
 * 銆�銆�銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹ｂ敁
 * 銆�銆�銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹忊敍
 * 銆�銆�銆�銆�鈹椻敁鈹撯攺鈹佲敵鈹撯攺鈹�
 * 銆�銆�銆�銆�銆�鈹冣敨鈹��鈹冣敨鈹�
 * 銆�銆�銆�銆�銆�鈹椻敾鈹涖��鈹椻敾鈹�
 * 鈹佲攣鈹佲攣鈹佲攣鎰熻钀岃悓鍝掆攣鈹佲攣鈹佲攣鈹�
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
	        String[] urls = new String[body.split("\n").length -1];
	        System.arraycopy(body.split("\n"), 1, urls, 0, body.split("\n").length -1); ;
	        //System.out.println(urls);
	        for(String url : urls){
	        	System.out.println(url);
	        }
	        
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
		logger.info("starting data dispatch server...");
		server.start();
		logger.info("start data dispatch server successfully");
		server.join();
	}

	private static void init() throws Exception {
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
