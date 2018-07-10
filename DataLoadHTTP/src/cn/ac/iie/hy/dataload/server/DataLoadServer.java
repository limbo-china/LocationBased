/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.dataload.server;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.dataload.config.Configuration;
import cn.ac.iie.hy.dataload.task.DataLoadTask;
import cn.ac.iie.hy.dataload.task.ThreadPoolManager;

/**
 * 鈹佲攣鈹佲攣鈹佲攣绁炲吔鍑烘病鈹佲攣鈹佲攣鈹佲攣 銆�銆�銆�鈹忊敁銆�銆�銆�鈹忊敁 銆�銆�鈹忊敍鈹烩攣鈹佲攣鈹涒敾鈹�
 * 銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹� 銆�銆�鈹冦��銆�銆�鈹併��銆�銆�鈹� 銆�銆�鈹冦��鈹斥敍銆�鈹椻敵銆�鈹�
 * 銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹� 銆�銆�鈹冦��銆�銆�鈹汇��銆�銆�鈹� 銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹�
 * 銆�銆�鈹椻攣鈹撱��銆�銆�鈹忊攣鈹� 銆�銆�銆�銆�鈹冦��銆�銆�鈹冪鍏戒繚浣�, 姘告棤BUG! 銆�銆�銆�銆�鈹冦��銆�銆�鈹僀ode
 * is far away from bug with the animal protecting 銆�銆�銆�銆�鈹冦��銆�銆�鈹椻攣鈹佲攣鈹�
 * 銆�銆�銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹ｂ敁 銆�銆�銆�銆�鈹冦��銆�銆�銆�銆�銆�銆�鈹忊敍
 * 銆�銆�銆�銆�鈹椻敁鈹撯攺鈹佲敵鈹撯攺鈹� 銆�銆�銆�銆�銆�鈹冣敨鈹��鈹冣敨鈹� 銆�銆�銆�銆�銆�鈹椻敾鈹涖��鈹椻敾鈹�
 * 鈹佲攣鈹佲攣鈹佲攣鎰熻钀岃悓鍝掆攣鈹佲攣鈹佲攣鈹�
 * 
 * @author zhangyu
 */
public class DataLoadServer {

	static DataLoadTask task = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataLoadServer.class.getName());
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
					"starting data laod server is failed for "
							+ ex.getMessage(), ex);
		}

		System.exit(0);
	}

	private static void startup() throws Exception {
		logger.info("starting data load server...");
		while (true)
			;
	}

	private static void init() throws Exception {

		String configurationFileName = "data-load.properties";
		logger.info("initializing data load server...");
		logger.info("getting configuration from configuration file "
				+ configurationFileName);
		Configuration conf = Configuration
				.getConfiguration(configurationFileName);
		if (conf == null) {
			throw new Exception("reading " + configurationFileName
					+ " is failed.");
		}

		String loadURL = conf.getString("loadURL", "");
		if (loadURL.isEmpty()) {
			throw new Exception("definition loadURL is not found in "
					+ configurationFileName);
		}

		int loadBatchCount = conf.getInt("loadBatchCount", -1);
		if (loadBatchCount == -1) {
			throw new Exception("definition jettyServerPort is not found in "
					+ configurationFileName);
		}

		int loadTimeInternal = conf.getInt("loadTimeInternal", -1);
		if (loadTimeInternal == -1) {
			throw new Exception(
					"definition jettyServerThreadPoolSize is not found in "
							+ configurationFileName);
		}

		String areaID = conf.getString("areaID", "");
		if (areaID.isEmpty()) {
			throw new Exception("definition areaID is not found in "
					+ configurationFileName);
		}

		// task = new DataLoadTask(loadBatchCount, loadTimeInternal, loadURL,
		// areaID);
		ThreadPoolManager threadPool = ThreadPoolManager.newInstance();
		threadPool.addExecuteTask(new DataLoadTask(loadBatchCount,
				loadTimeInternal, loadURL, areaID));

		logger.info("intialize data load server successfully");

	}
}
