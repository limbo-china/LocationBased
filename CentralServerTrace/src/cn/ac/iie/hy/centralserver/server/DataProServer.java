/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.centralserver.server;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import cn.ac.iie.hy.centralserver.config.ConfigUtil;
import cn.ac.iie.hy.centralserver.handler.DataTraceDetailQueryHandler;
import cn.ac.iie.hy.centralserver.handler.DataTraceQueryHandler;
import cn.ac.iie.hy.centralserver.log.LogUtil;

public class DataProServer {

	static Server server = null;

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
			LogUtil.error("starting data pro server is failed for "
					+ ex.getMessage());
		}

		System.exit(0);
	}

	private static void startup() throws Exception {
		LogUtil.info("starting data pro server...");
		server.start();
		LogUtil.info("start data pro server successfully");
		server.join();
	}

	private static void init() throws Exception {
		LogUtil.info("initializing data dispatch server...");

		int serverPort = ConfigUtil.getInt("jettyServerPort");
		int serverThreadPoolSize = ConfigUtil
				.getInt("jettyServerThreadPoolSize");

		server = new Server();
		server.setThreadPool(new QueuedThreadPool(serverThreadPoolSize));

		SslSocketConnector ssl_connector = new SslSocketConnector();
		ssl_connector.setPort(serverPort);
		SslContextFactory cf = ssl_connector.getSslContextFactory();
		cf.setKeyStorePath("keystore");
		cf.setKeyStorePassword("123456");
		cf.setKeyManagerPassword("123456");
		server.addConnector(ssl_connector);

		ContextHandler dataTraceDetailQueryContext = new ContextHandler(
				"/tracedetailquery");
		DataTraceDetailQueryHandler dataTraceDetailQueryHandler = DataTraceDetailQueryHandler
				.getHandler();
		if (dataTraceDetailQueryHandler == null) {
			throw new Exception(
					"initializing dataTraceDetailQueryHandler failed");
		}
		dataTraceDetailQueryContext.setHandler(dataTraceDetailQueryHandler);

		ContextHandler dataTraceQueryContext = new ContextHandler("/tracequery");
		DataTraceQueryHandler dataTraceQueryHandler = DataTraceQueryHandler
				.getHandler();
		if (dataTraceQueryHandler == null) {
			throw new Exception("initializing dataTraceQueryHandler failed");
		}
		dataTraceQueryContext.setHandler(dataTraceQueryHandler);

		ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.setHandlers(new Handler[] { dataTraceDetailQueryContext,
				dataTraceQueryContext });

		server.setHandler(contexts);
		LogUtil.info("intialize data dispatch server successfully");

	}
}
