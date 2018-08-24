/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.datadispatch.server;





import cn.ac.iie.hy.datadispatch.config.Configuration;
import cn.ac.iie.hy.datadispatch.dbutils.GJRuleRedisUtil;
import cn.ac.iie.hy.datadispatch.handler.DataDispatchHandler;
import cn.ac.iie.hy.datadispatch.metadata.GJDetail;
import cn.ac.iie.hy.datadispatch.metadata.GJRule;
import cn.ac.iie.hy.datadispatch.task.DBLoadTask;
import cn.ac.iie.hy.datadispatch.utils.HomeCodeMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import redis.clients.jedis.Jedis;
import com.google.gson.Gson;



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
public class DataDispatchServer {

	static Server server = null;
	static Logger logger = null;
	public static HashMap<String, HashMap> gjRuleMap;//arg[0]:手机号码 arg[1]:province-detail

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataDispatchServer.class.getName());		
		
	}
	
	public static void updateGJrule() {
		
		Timer time = new Timer();
		time.scheduleAtFixedRate(new TimerTask()
				{

					@Override
					public void run() {
						// TODO Auto-generated method stub						
						logger.info("monitorwarning updating gjrule ");
						gjRuleMap= new HashMap<>();
						//GJ规则表		
						Jedis jedis = null;
						jedis = GJRuleRedisUtil.getJedis();		
						Set<String> keys = jedis.keys("*"); Iterator<String>
						it=keys.iterator() ;		 
						while(it.hasNext()){
						  
							String key = it.next(); //System.out.println(key); 
							//System.out.println("[获取对象数据]key："+ key);	
							
							if(key.startsWith("BEIJING_07"))
							{
							//	System.out.println("[获取对象数据]key:"+ key);	
							logger.info("monitorwarning [获取对象数据]key:"+ key+" len: "+jedis.llen(key));
							List<String> list  = jedis.lrange(key, 0, -1);			
						 
							for(String str:list){	 			
								
								
								String groupid="";
								String groupname="";
								String phone="";
								String provinceId="";
								String provinceName="";				
								int source=0;
							    Gson gson  = new Gson(); 
							    GJRule gjrule = gson.fromJson(str, GJRule.class);			 
							 //   System.out.println("[json]："+gjrule.getPhone()+' '+gjrule.getProvinceId());
							     groupid = gjrule.getGroupid();
							     groupname = gjrule.getGroupname();
							     phone = gjrule.getPhone();
							     provinceId = gjrule.getProvinceId();
							     provinceName=gjrule.getProvinceName();
							     source = gjrule.getSource();				
								
							 
								if(gjRuleMap.containsKey(phone)){
									HashMap<String, ArrayList<GJDetail>> groupMap = new HashMap<>();
									groupMap=gjRuleMap.get(phone);
									if(groupMap.containsKey(provinceId)){
										ArrayList<GJDetail> arraytemp= new ArrayList<GJDetail>();
										arraytemp = groupMap.get(provinceId);
										GJDetail djdetail = new GJDetail();
										djdetail.setGroupid(groupid);
										djdetail.setGroupname(groupname);
										djdetail.setProvinceName(provinceName);
										djdetail.setSource(source);					
										arraytemp.add(djdetail);				
										groupMap.put(provinceId,arraytemp);	
										gjRuleMap.put(phone, groupMap);
										
																
									}else{
										ArrayList<GJDetail> arraytemp= new ArrayList<GJDetail>();
										GJDetail djdetail = new GJDetail();
										djdetail.setGroupid(groupid);
										djdetail.setGroupname(groupname);
										djdetail.setProvinceName(provinceName);
										djdetail.setSource(source);
										arraytemp.add(djdetail);
										groupMap.put(provinceId, arraytemp);
										gjRuleMap.put(phone, groupMap);
									}
								    if(phone.equals("8618810809256"))
								    {
								    	logger.info("monitorwarning"+provinceId);
								    }
								
								}
								else{
									HashMap<String, ArrayList<GJDetail>> groupMap = new HashMap<>();
									ArrayList<GJDetail> arraytemp= new ArrayList<GJDetail>();
									GJDetail djdetail = new GJDetail();
									djdetail.setGroupid(groupid);
									djdetail.setGroupname(groupname);
									djdetail.setProvinceName(provinceName);
									djdetail.setSource(source);
									arraytemp.add(djdetail);
									groupMap.put(provinceId,arraytemp);
									gjRuleMap.put(phone, groupMap);
								}
							 
							  } 
							
						}
							  }
					}
			
			
				}
				, 0, 1000*60*5);//1hour更新一次
		
		
		
		
	
		
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
			updateGJrule();
			startup();
		} catch (Exception ex) {
			logger.error("starting data dispatcher server is failed for " + ex.getMessage(), ex);
		}
		
		System.exit(0);
	}

	public static void loadUrl() {
		HttpClient httpClient = new DefaultHttpClient();
		String pubUrl = "http://10.213.69.20:8010/infopub/?op=getLoadServerList";
		//String pubUrl = "http://172.16.18.34:8010/infopub/?op=getLoadServerList";
		Boolean ret = true;
		try {

			HttpPost httppost = new HttpPost(pubUrl);
			HttpResponse response = httpClient.execute(httppost);
			HttpEntity entity = response.getEntity();  
	        
	        String body = null;  
	        try {  
	            body = EntityUtils.toString(entity);  
	        } catch (ParseException e) {  
	            e.printStackTrace();  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }
	        
			//logger.warn(body);
	        body = body.replaceAll("#", "");
	        String[] urls = body.split("\n");
	        DBLoadTask.setUrlsList(urls);
			httppost.releaseConnection();
		} catch (Exception ex) {
			ret = false;
			ex.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}
	}
	
	private static void startup() throws Exception {
		HomeCodeMap.loadIndex("homelist.txt");
		
//		List<RoamData> rdList = new ArrayList<RoamData>();
//		
//		rdList.add(new RoamData("53","530000","0000","8618638585876",1527474897,0));
//		rdList.add(new RoamData("41","410105","0371","8618638585876",1527666122,1));
//		
//        if(!rdList.isEmpty()){
//			DataDispatcher.runRabbitTask(rdList);
//		}
        
		logger.info("starting data dispatch server...");
		server.start();
		logger.info("start data dispatch server successfully");
    	
		server.join();
		
		
    	
	}

	private static void init() throws Exception {
		//DataCrypt.auth("jm.conf");
		
		String configurationFileName = "data-dispatcher.properties";
		logger.info("initializing data dispatch server...");
		logger.info("getting configuration from configuration file " + configurationFileName);
		Configuration conf = Configuration.getConfiguration(configurationFileName);
		if (conf == null) {
			throw new Exception("reading " + configurationFileName + " is failed.");
		}

		String serverIP = conf.getString("jettyServerIP", "");
		if (serverIP.isEmpty()) {
			throw new Exception("definition jettyServerIP is not found in " + configurationFileName);
		}

		int serverPort = conf.getInt("jettyServerPort", -1);
		if (serverPort == -1) {
			throw new Exception("definition jettyServerPort is not found in " + configurationFileName);
		}

		int serverThreadPoolSize = conf.getInt("jettyServerThreadPoolSize", -1);
		if (serverThreadPoolSize == -1) {
			throw new Exception("definition jettyServerThreadPoolSize is not found in " + configurationFileName);
		}

		loadUrl();
		
		server = new Server(serverPort);

		ContextHandler dataLoadContext = new ContextHandler("/dataload");
		DataDispatchHandler dataDispatchHandler = DataDispatchHandler.getDataDispatchHandler();

		if (dataDispatchHandler == null) {
			throw new Exception("initializing dataDispatchHandler failed");
		}
		dataLoadContext.setHandler(dataDispatchHandler);

		ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.setHandlers(new Handler[] { dataLoadContext });
		server.setHandler(contexts);
		logger.info("intialize data dispatch server successfully");
	}
}
