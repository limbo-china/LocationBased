package cn.ac.iie.hy.datadispatch.task;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.gson.Gson;

import cn.ac.iie.hy.datadispatch.data.SMetaData;

public class DataFileterTask implements Runnable {

	List<SMetaData> allList = null;
	
	static Gson gson= new Gson();
	static Logger logger = null;
	static String url = null;
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataFileterTask.class.getName());
        Properties pps = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream("data-dispatcher.properties"));
			pps.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		url = pps.getProperty("loadUrl");
    }
    
    
    
	public DataFileterTask(List<SMetaData> allList) {
		super();
		this.allList = allList;
	}
	
	public Boolean sendData(List<SMetaData> result) {
		// long startTime = System.currentTimeMillis();
		// System.out.println(startTime);
		HttpClient httpClient = new DefaultHttpClient();
		httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,2000);
		httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,2000);
		Boolean ret = true;
		try {

			HttpPost httppost = new HttpPost(url);
			
			httppost.setHeader("Accept", "application/json");
			httppost.setEntity(new StringEntity(gson.toJson(result), Charset.forName("UTF-8")));
			HttpResponse response = httpClient.execute(httppost);
			logger.warn(response.getStatusLine());
			httppost.releaseConnection();

		} catch (Exception ex) {
			ret = false;
			ex.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}

		return ret;
		// long endTime = System.currentTimeMillis();

		// System.out.println("use " + (endTime - startTime) + " ms ");
	}
	@Override
	public void run() {
		List<SMetaData> resultList = new ArrayList<>();
		for(SMetaData smd : allList){
			if(!smd.getC_imsi().startsWith("460")){
				resultList.add(smd);
				//logger.info(smd.getC_imsi());
			}
		}
		
		if(resultList.size() != 0){
			//logger.info("********"+resultList.size());
			sendData(resultList);
		}
	}

}
