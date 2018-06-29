package cn.ac.iie.hy.USubTask;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.nodeserver.config.Configuration;
import cn.ac.iie.hy.nodeserver.task.BSDataMap;
import cn.ac.iie.hy.nodeserver.task.ThreadPoolManager;
import cn.ac.iie.hy.nodeserver.task.UserSubPushTask;

public class USubTask {

	static Logger logger = null;
    static ThreadPoolManager threadpool = ThreadPoolManager.newInstance();

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(USubTask.class.getName());
    }
    
	public static void main(String[] args) {
		String configurationFileName = "data-node.properties";
        logger.info("initializing data node server...");
        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            logger.error("reading " + configurationFileName + " is failed.");
            System.exit(-1);
        }
        //BSDataMap.loadIndex("index.csv");
        
        String rKey = conf.getString("redisKey", "");
        if(rKey.isEmpty()){
        	logger.error("redisKey is Empty");
            System.exit(-1);
        }
        threadpool.addExecuteTask(new UserSubPushTask(rKey));
        
        while(true){
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
	}

}
