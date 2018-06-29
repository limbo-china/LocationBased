package cn.ac.iie.hy.datadispatch.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import cn.ac.iie.hy.datadispatch.handler.DataDispatcher;
import cn.ac.iie.hy.datadispatch.data.SMetaData;


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
public class DispatchTask implements Runnable {

	List<SMetaData> al = null;
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DispatchTask.class.getName());
	}
	public  DispatchTask(List<SMetaData> al) {
		this.al = al;
	}
	ThreadPoolManager threadpool =  ThreadPoolManager.newInstance();

    @Override
    public void run() {

//       for(SMetaData smd : al){
//    	   System.out.println(smd.getC_imsi());
//       }
    	threadpool.addExecuteTask(new DBLoadTask(al));
    	threadpool.addExecuteTask(new DataFileterTask(al));
    	threadpool.addExecuteTask(new DataTmpTask(al));
    	threadpool.addExecuteTask(new HiveLoadTask(al, "http://10.224.82.62:10080", "t_lbs_trace_history", "t_lbs_trace_history.json"));
      // threadpool.addExecuteTask(new TraceDumpTask(al));
    }
}

