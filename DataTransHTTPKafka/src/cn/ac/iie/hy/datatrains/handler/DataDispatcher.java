package cn.ac.iie.hy.datatrains.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.datastrains.task.CDRFilterTask;
import cn.ac.iie.hy.datastrains.task.CSCDRTask;
import cn.ac.iie.hy.datastrains.task.DBUpdateTask;
import cn.ac.iie.hy.datastrains.task.ThreadPoolManager;
import cn.ac.iie.hy.datastrains.task.UserSubTask;
import cn.ac.iie.hy.datatrains.metadata.SMetaData;
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
 *
 */
public class DataDispatcher {

	private static ThreadPoolManager threadpool =  null;
	static Logger logger = null;
	
	static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataDispatchReceiver.class.getName());
        threadpool = ThreadPoolManager.newInstance();
    }
	
	public static void runTask(List<SMetaData> al) {
		threadpool.addExecuteTask(new DBUpdateTask(al));
		//threadpool.addExecuteTask(new CDRFilterTask(al));
	}
	public static void runCsCdrTask(List<String> al){
		threadpool.addExecuteTask(new CSCDRTask(al));
	}
	

}
