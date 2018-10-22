/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.centralserver.server;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import cn.ac.iie.centralserver.config.ConfigUtil;
import cn.ac.iie.centralserver.crypt.DataCrypt;
import cn.ac.iie.centralserver.handler.DataDetailQueryHandler;
import cn.ac.iie.centralserver.log.LogUtil;

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
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {
		try {
			init();
			startup();
		} catch (Exception ex) {
			logger.error(
					"starting data pro server is failed for " + ex.getMessage(),
					ex);
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
		LogUtil.info("initializing data dispatch server...");

		int serverPort = ConfigUtil.getInt("jettyServerPort");

		int serverThreadPoolSize = ConfigUtil
				.getInt("jettyServerThreadPoolSize");

		DataCrypt.auth("jm.conf");

		server = new Server();
		server.setThreadPool(new QueuedThreadPool(serverThreadPoolSize));

		SslSocketConnector ssl_connector = new SslSocketConnector();
		ssl_connector.setPort(serverPort);
		SslContextFactory cf = ssl_connector.getSslContextFactory();
		cf.setKeyStorePath("keystore");
		cf.setKeyStorePassword("123456");
		cf.setKeyManagerPassword("123456");
		server.addConnector(ssl_connector);

		ContextHandler dataDetailQueryContext = new ContextHandler(
				"/detailquery");
		DataDetailQueryHandler dataDetailQueryHandler = DataDetailQueryHandler
				.getHandler();
		if (dataDetailQueryHandler == null) {
			throw new Exception("initializing dataDetailQueryHandler failed");
		}
		dataDetailQueryContext.setHandler(dataDetailQueryHandler);

		ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.setHandlers(new Handler[] { dataDetailQueryContext });
		server.setHandler(contexts);
		LogUtil.info("intialize data dispatch server successfully");
	}
}
