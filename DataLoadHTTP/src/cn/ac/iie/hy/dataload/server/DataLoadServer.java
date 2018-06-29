/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.dataload.server;

import cn.ac.iie.hy.dataload.config.Configuration;
import cn.ac.iie.hy.dataload.task.DataLoadTask;
import cn.ac.iie.hy.dataload.task.ThreadPoolManager;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


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
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            init();
            startup();
        } catch (Exception ex) {
            logger.error("starting data laod server is failed for " + ex.getMessage(), ex);
        }

        System.exit(0);
    }

    private static void startup() throws Exception {
        logger.info("starting data load server...");
        while(true);
    }

    private static void init() throws Exception {
    	
        String configurationFileName = "data-load.properties";
        logger.info("initializing data load server...");
        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }

        String loadURL = conf.getString("loadURL", "");
        if (loadURL.isEmpty()) {
            throw new Exception("definition loadURL is not found in " + configurationFileName);
        }

        int loadBatchCount = conf.getInt("loadBatchCount", -1);
        if (loadBatchCount == -1) {
            throw new Exception("definition jettyServerPort is not found in " + configurationFileName);
        }

        int loadTimeInternal = conf.getInt("loadTimeInternal", -1);
        if (loadTimeInternal == -1) {
            throw new Exception("definition jettyServerThreadPoolSize is not found in " + configurationFileName);
        }
        
        String areaID = conf.getString("areaID", "");
        if(areaID.isEmpty()){
        	throw new Exception("definition areaID is not found in " + configurationFileName);
        }
        
        //task = new DataLoadTask(loadBatchCount, loadTimeInternal, loadURL, areaID);
        ThreadPoolManager threadPool = ThreadPoolManager.newInstance();
        threadPool.addExecuteTask(new DataLoadTask(loadBatchCount, loadTimeInternal, loadURL, areaID));
        
        
        
        logger.info("intialize data load server successfully");
        
    }
}
