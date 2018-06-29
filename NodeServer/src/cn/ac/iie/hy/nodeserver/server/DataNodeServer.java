/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.nodeserver.server;

import cn.ac.iie.hy.nodeserver.config.Configuration;
import cn.ac.iie.hy.nodeserver.data.BSDataMap;
import cn.ac.iie.hy.nodeserver.handler.DataSQLQueryHandler;
import cn.ac.iie.hy.nodeserver.handler.SubUserQueryHandler;
//import cn.ac.iie.hy.nodeserver.handler.SubUserQueryHandler2;
import cn.ac.iie.hy.nodeserver.handler.UserSubHandler;
import cn.ac.iie.hy.nodeserver.task.CSCSRPushTask;
import cn.ac.iie.hy.nodeserver.task.ChangAnCdrPushTask;
import cn.ac.iie.hy.nodeserver.task.ThreadPoolManager;
import cn.ac.iie.hy.nodeserver.task.UserSubPushTask;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

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
public class DataNodeServer {

    static Server server = null;
    static Logger logger = null;
    static ThreadPoolManager threadpool = ThreadPoolManager.newInstance();

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataNodeServer.class.getName());
    }

    public static void showUsage() {
        System.out.println("Usage:java -jar ");
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            init();
            startup();
        } catch (Exception ex) {
            logger.error("starting data pro server is failed for " + ex.getMessage(), ex);
        }

        System.exit(0);
    }

    private static void startup() throws Exception {
    	
    	BSDataMap.loadIndex("index.csv");
    	threadpool.addExecuteTask(new UserSubPushTask());
   // 	threadpool.addExecuteTask(new UserSubPushTask());
 //   	threadpool.addExecuteTask(new UserSubPushTask());
 //   	threadpool.addExecuteTask(new CSCSRPushTask());
//    	for(int i = 0; i < 10 ; i++){
//    		threadpool.addExecuteTask(new ChangAnCdrPushTask());
//    	}
        logger.info("starting data pro server...");
        server.start();
        logger.info("start data pro server successfully");
        server.join();
    }

    private static void init() throws Exception {
        String configurationFileName = "data-node.properties";
        logger.info("initializing data node server...");
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

        Connector connector = new SelectChannelConnector();
        connector.setHost(serverIP);
        connector.setPort(serverPort);

		server = new Server();
		server.setConnectors(new Connector[] { connector });
		server.setThreadPool(new QueuedThreadPool(serverThreadPoolSize));

		ContextHandler dataSQLQueryContext = new ContextHandler("/sqlquery");
		DataSQLQueryHandler dataSQLQueryHandler = DataSQLQueryHandler.getHandler();
		if (dataSQLQueryHandler == null) {
			throw new Exception("initializing dataSQLQUeryHandler failed");
		}
		dataSQLQueryContext.setHandler(dataSQLQueryHandler);
		
		ContextHandler userSubContext = new ContextHandler("/usersubcribe");
		UserSubHandler userSubHandler = UserSubHandler.getHandler();
		if(userSubHandler == null){
			throw new Exception("initializing userSubHandler failed");
		}
		userSubContext.setHandler(userSubHandler);
		
		ContextHandler userSubQueryContext = new ContextHandler("/usersubquery");
		SubUserQueryHandler userSubQeuryHandler = SubUserQueryHandler.getHandler();
		if(userSubQeuryHandler == null){
			throw new Exception("initializing userSubQeuryHandler failed");
		}
		userSubQueryContext.setHandler(userSubQeuryHandler);
		ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.setHandlers(new Handler[] { dataSQLQueryContext, userSubContext, userSubQueryContext });
		server.setHandler(contexts);
        logger.info("intialize data node server successfully");
    }
}
