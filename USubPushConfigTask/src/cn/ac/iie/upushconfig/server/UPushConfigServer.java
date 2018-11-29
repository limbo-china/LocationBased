/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.upushconfig.server;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import cn.ac.iie.upushconfig.config.ConfigUtil;
import cn.ac.iie.upushconfig.handler.UPushConfigHandler;
import cn.ac.iie.upushconfig.log.LogUtil;

public class UPushConfigServer {

	static Server server = null;

	public static void main(String[] args) {
		try {
			init();
			startup();
		} catch (Exception ex) {
			LogUtil.error("starting upush config server is failed for "
					+ ex.getMessage());
		}

		System.exit(0);
	}

	private static void startup() throws Exception {
		LogUtil.info("starting upush config server...");
		server.start();
		LogUtil.info("start upush config server successfully");
		server.join();
	}

	private static void init() throws Exception {
		LogUtil.info("initializing upush config server...");

		int serverPort = ConfigUtil.getInt("jettyServerPort");

		int serverThreadPoolSize = ConfigUtil
				.getInt("jettyServerThreadPoolSize");

		server = new Server(serverPort);
		server.setThreadPool(new QueuedThreadPool(serverThreadPoolSize));

		ContextHandler uPushConfigContext = new ContextHandler("/subconfig");
		UPushConfigHandler uPushConfigHandler = UPushConfigHandler.getHandler();
		if (uPushConfigHandler == null) {
			throw new Exception("initializing uPushConfigHandler failed");
		}
		uPushConfigContext.setHandler(uPushConfigHandler);

		ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.setHandlers(new Handler[] { uPushConfigContext });
		server.setHandler(contexts);
		LogUtil.info("intialize upush config server successfully");
	}
}
