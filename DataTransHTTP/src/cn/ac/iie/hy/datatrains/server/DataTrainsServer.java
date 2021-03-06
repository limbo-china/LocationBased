/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.datatrains.server;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import cn.ac.iie.hy.datatrains.config.Configuration;
import cn.ac.iie.hy.datatrains.handler.DataDispatchHandler;

public class DataTrainsServer {

	static Server server = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataTrainsServer.class.getName());
	}

	public static void showUsage() {
		System.out.println("Usage:java -jar ");
	}

	public static void main(String[] args) {
		try {
			init();
			startup();
		} catch (Exception ex) {
			logger.error(
					"starting data dispatcher server is failed for "
							+ ex.getMessage(), ex);
		}

		System.exit(0);
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
		logger.info("getting configuration from configuration file "
				+ configurationFileName);
		Configuration conf = Configuration
				.getConfiguration(configurationFileName);
		if (conf == null) {
			throw new Exception("reading " + configurationFileName
					+ " is failed.");
		}

		int serverPort = conf.getInt("jettyServerPort", -1);
		if (serverPort == -1) {
			throw new Exception("definition jettyServerPort is not found in "
					+ configurationFileName);
		}

		int serverThreadPoolSize = conf.getInt("jettyServerThreadPoolSize", -1);
		if (serverThreadPoolSize == -1) {
			throw new Exception(
					"definition jettyServerThreadPoolSize is not found in "
							+ configurationFileName);
		}

		server = new Server(serverPort);

		ContextHandler dataLoadContext = new ContextHandler("/dataload");
		DataDispatchHandler dataDispatchHandler = DataDispatchHandler
				.getDataDispatchHandler();

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
