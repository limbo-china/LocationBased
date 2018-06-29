package cn.ac.iie.hy.datadispatch.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import cn.ac.iie.hy.datadispatch.data.SMetaData;
import cn.ac.iie.hy.datadispatch.task.DBLoadTask;
import cn.ac.iie.hy.datadispatch.task.DispatchTask;
import cn.ac.iie.hy.datadispatch.task.ThreadPoolManager;
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

	private static ThreadPoolManager threadpool = ThreadPoolManager.newInstance();
	
	private static boolean isRabbitServiceRunning = false;
	
	
	public static void runTask(List<SMetaData> al) {
		
		threadpool.addExecuteTask(new DispatchTask(al));
	}
	
	
	
}
