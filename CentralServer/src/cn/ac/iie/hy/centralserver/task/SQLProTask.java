package cn.ac.iie.hy.centralserver.task;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;

import org.apache.http.impl.client.DefaultHttpClient;

import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;


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

public class SQLProTask implements Runnable{

	String token;
	String URL;
	String sql;
	String targetHost;
	String jobID;
	String taskHost;
	
//	private static int SocketTimeout = 5000;// 5秒
//	private static int ConnectTimeout = 5000;// 5秒
//	private static Boolean SetTimeOut = false;
	
	public SQLProTask(String token, String uRL, String sql, String targetHost, String jobID,String taskHost) {
		super();
		this.token = token;
		URL = uRL;
		this.sql = sql;
		this.targetHost = targetHost;
		this.jobID = jobID;
		this.taskHost = taskHost;
	}
	
	public SQLProTask(){
		
	}
	

//	private void sqlQueryPush(String result) {
//		HttpClient httpClient = new DefaultHttpClient();
//		try {
//			HttpPost httppost = new HttpPost(targetHost);
//			httppost.addHeader("Content-type", "application/json; charset=utf-8");
//			httppost.setEntity(new StringEntity(result, Charset.forName("UTF-8")));
//			HttpResponse response = httpClient.execute(httppost);
//			System.out.println(response.getStatusLine());
//			httppost.releaseConnection();
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			httpClient.getConnectionManager().shutdown();
//		}
//	}

	public class SSLClient extends DefaultHttpClient {
		public SSLClient() throws Exception {
			super();
			SSLContext ctx = SSLContext.getInstance("SSL");
			X509TrustManager tm = new X509TrustManager() {
				@Override
				public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				}

				@Override
				public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				}

				@Override
				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}
			};
			ctx.init(null, new TrustManager[] { tm }, null);
			SSLSocketFactory ssf = new SSLSocketFactory(ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			ClientConnectionManager ccm = this.getConnectionManager();
			SchemeRegistry sr = ccm.getSchemeRegistry();
			sr.register(new Scheme("https", 443, ssf));
		}
	}

	public class HttpClientUtil {
		public String doPost(String url, Map<String, String> map, String charset) {
			HttpClient httpClient = null;
			HttpPost httpPost = null;
			String result = null;
			try {
				httpClient = new SSLClient();
				httpPost = new HttpPost(url);
				// 设置参数
				List<NameValuePair> list = new ArrayList<NameValuePair>();
				Iterator iterator = map.entrySet().iterator();
				while (iterator.hasNext()) {
					Entry<String, String> elem = (Entry<String, String>) iterator.next();
					list.add(new BasicNameValuePair(elem.getKey(), elem.getValue()));
				}
				if (list.size() > 0) {
					UrlEncodedFormEntity entity = new UrlEncodedFormEntity(list, charset);
					httpPost.setEntity(entity);
				}
				HttpResponse response = httpClient.execute(httpPost);
				if (response != null) {
					HttpEntity resEntity = response.getEntity();
					if (resEntity != null) {
						result = EntityUtils.toString(resEntity, charset);
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return result;
		}
	}

	@Override
	public void run() {
		HttpClientUtil httpClientUtil = new HttpClientUtil();
		String charset = "utf-8";
		Map<String, String> createMap = new HashMap<String, String>();
		createMap.put("token", token);
		createMap.put("dbURL", URL);
		createMap.put("sql", sql);
		createMap.put("targethost", targetHost);
		createMap.put("jobID", jobID);
		String url = taskHost + "/sqlquery/";
		String httpOrgCreateTestRtn = httpClientUtil.doPost(url, createMap, charset);
		System.out.println("result:" + httpOrgCreateTestRtn);
	}
	
	public void test(){
		HttpClientUtil httpClientUtil = new HttpClientUtil();  
		String charset = "utf-8";  
        Map<String,String> createMap = new HashMap<String,String>();   
        createMap.put("token","123123123123123");  
        createMap.put("indexType","Msisdn");  
        createMap.put("configId","dddd");  
        createMap.put("crowdName","sdfdsf"); 
        createMap.put("time","sdfsdfa"); 
		String url = "https://172.16.10.201:8009/subconfig/";
		String httpOrgCreateTestRtn = httpClientUtil.doPost(url,createMap,charset);
		System.out.println("result:"+httpOrgCreateTestRtn); 
	}
	
	public static void main(String[] argv){
		new SQLProTask().test();
	}
}
