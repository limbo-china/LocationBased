package cn.ac.iie.hy.nodeserver.task;

import java.nio.charset.Charset;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * 
 * @zhangyu
 */
public class DBQueryTask implements Runnable {

	String host = null;
	String sql = null;
	String jobid = null;

	public DBQueryTask(String host, String sql, String jobid) {
		super();
		this.host = host;
		this.sql = sql;
		this.jobid = jobid;
	}

	public DBQueryTask() {
	}

	private void sqlQueryPush(String result) {
		HttpClient httpClient = new DefaultHttpClient();
		try {
			HttpPost httppost = new HttpPost(host);
			httppost.addHeader("Content-type", "application/json; charset=utf-8");
			httppost.setEntity(new StringEntity(result, Charset.forName("UTF-8")));
			HttpResponse response = httpClient.execute(httppost);
			System.out.println(response.getStatusLine());
			httppost.releaseConnection();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}
	}

	@Override
	public void run() {
		long startTime = System.currentTimeMillis();

		// needSomeTime();
		long endTime = System.currentTimeMillis();
		System.out.println("执行任务耗时：" + (endTime - startTime));
	}

}
