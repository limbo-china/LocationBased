package cn.ac.iie.hy.datatrains.handler;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.datastrains.task.CSCDRTask;
import cn.ac.iie.hy.datastrains.task.DBUpdateTask;
import cn.ac.iie.hy.datastrains.task.ThreadPoolManager;
import cn.ac.iie.hy.datatrains.metadata.SMetaData;

public class DataDispatcher {

	private static ThreadPoolManager threadpool = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDispatchHandler.class.getName());
		threadpool = ThreadPoolManager.newInstance();
	}

	public static void runTask(List<SMetaData> al) {
		threadpool.addExecuteTask(new DBUpdateTask(al));
	}

	public static void runCsCdrTask(List<String> al) {
		threadpool.addExecuteTask(new CSCDRTask(al));
	}

}
