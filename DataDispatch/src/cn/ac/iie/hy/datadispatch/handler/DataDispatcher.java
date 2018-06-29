package cn.ac.iie.hy.datadispatch.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.datadispatch.rabbit.DataFactory;
import cn.ac.iie.datadispatch.rabbit.RabbitServiceTask;
import cn.ac.iie.hy.datadispatch.crypt.DataCrypt;
import cn.ac.iie.hy.datadispatch.metadata.RoamData;
import cn.ac.iie.hy.datadispatch.metadata.SMetaData;
import cn.ac.iie.hy.datadispatch.server.DataDispatchServer;
import cn.ac.iie.hy.datadispatch.task.DBLoadTask;
import cn.ac.iie.hy.datadispatch.task.DBUpdateTask;
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
	private static ThreadPoolManager threadpool2 = ThreadPoolManager.newInstance2();

	private static DataFactory df = new DataFactory(100000);
	
	private static boolean isRabbitServiceRunning = false;
	
	public static boolean isChinaPhoneLegal(String str) throws PatternSyntaxException {  
        String regExp = "^((\\+86)|(86))?((13[0-9])|(15[^4])|(18[0,2,3,5-9])|(17[0-8])|(147))\\d{8}$";  
        Pattern p = Pattern.compile(regExp);  
        Matcher m = p.matcher(str);  
        return m.matches();  
    }
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDispatchServer.class.getName());
	}
//	public static boolean isNotJM(String str) throws PatternSyntaxException {  
//        String regExp = "^1[0-9]*$";  
//        Pattern p = Pattern.compile(regExp);  
//        Matcher m = p.matcher(str);  
//        return m.matches();  
//    }
	
	public static void runTask(List<SMetaData> al) {
		
		threadpool.addExecuteTask(new DBUpdateTask(al));
		threadpool.printThreadPoolQueueSize();
	}
	
	public static void runLoadTask(List<String> al){
		threadpool2.addExecuteTask(new DBLoadTask(al));
	}
	
	public static void runRabbitTask(List<RoamData> al){
		if(!isRabbitServiceRunning){
			threadpool2.addExecuteTask(new RabbitServiceTask(isRabbitServiceRunning,  df));
			isRabbitServiceRunning = true;
		}
		
		try {
			List<RoamData> fdata = new ArrayList(al.size());
			//int dedataout_len = 0;
			for(RoamData rd : al){
				if(rd.getUserNumber() != null && isChinaPhoneLegal(rd.getUserNumber())){
//					rd.encryptData();
//					System.out.println("Encrypt:");
//					rd.print();
//					if(!isNotJM(rd.getUserNumber())){
//						dedataout_len = rd.decryptData();
//						while(-1 == dedataout_len){
//							try {
//								DataCrypt.auth("jm.conf");
//							} catch (IOException e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							}
//							dedataout_len = rd.decryptData();
//						}
//						System.out.println("Decrypt:");
//					}
//					rd.print();
					fdata.add(rd);
				}
			}
			df.sendData(fdata);
//			logger.info("执行任务1111111111111"+"size:"+al.size());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
}
