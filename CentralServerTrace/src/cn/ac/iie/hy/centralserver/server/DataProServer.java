/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.centralserver.server;

import cn.ac.iie.hy.centralserver.config.Configuration;
import cn.ac.iie.hy.centralserver.handler.DataTraceDetailQueryHandler;
import cn.ac.iie.hy.centralserver.handler.DataTraceQueryHandler;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

public class DataProServer {

    static Server server = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataProServer.class.getName());
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
        logger.info("starting data pro server...");
        server.start();
        logger.info("start data pro server successfully");
        server.join();
    }

    private static void init() throws Exception {
        String configurationFileName = "data-pro.properties";
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

        Connector connector = new SelectChannelConnector();
        connector.setHost(serverIP);
        connector.setPort(serverPort);

		server = new Server();
//		server.setConnectors(new Connector[] { connector });
		server.setThreadPool(new QueuedThreadPool(serverThreadPoolSize));

		SslSocketConnector ssl_connector = new SslSocketConnector();
        ssl_connector.setPort(serverPort);
        SslContextFactory cf = ssl_connector.getSslContextFactory();
        cf.setKeyStorePath("keystore");
        cf.setKeyStorePassword("123456");
        cf.setKeyManagerPassword("123456");
        server.addConnector(ssl_connector);

        

        ContextHandler dataTraceDetailQueryContext = new ContextHandler("/tracedetailquery");
        DataTraceDetailQueryHandler dataTraceDetailQueryHandler = DataTraceDetailQueryHandler.getHandler();
        if(dataTraceDetailQueryHandler == null){
            throw new Exception("initializing dataTraceDetailQueryHandler failed");
        }
        dataTraceDetailQueryContext.setHandler(dataTraceDetailQueryHandler);
        
        ContextHandler dataTraceQueryContext = new ContextHandler("/tracequery");
        DataTraceQueryHandler dataTraceQueryHandler = DataTraceQueryHandler.getHandler();
        if(dataTraceQueryHandler == null){
            throw new Exception("initializing dataTraceQueryHandler failed");
        }
        dataTraceQueryContext.setHandler(dataTraceQueryHandler);

        /**
         * Total姹囨��
         */
		ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.setHandlers(new Handler[] {dataTraceDetailQueryContext , dataTraceQueryContext});

		server.setHandler(contexts);
        logger.info("intialize data dispatch server successfully");

    }
}
