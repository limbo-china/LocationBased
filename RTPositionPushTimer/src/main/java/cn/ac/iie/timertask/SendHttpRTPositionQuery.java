package cn.ac.iie.timertask;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;

public class SendHttpRTPositionQuery {

	public static void main(String[] args) throws Exception {
		// SslContextFactory cf = new SslContextFactory();
		// cf.setKeyStorePath("keystore");
		// cf.setKeyStorePassword("123456");
		// cf.setKeyManagerPassword("123456");

		HttpClient httpClient = new HttpClient(/* cf */);

		// Start HttpClient
		httpClient.start();

		sendQuery(httpClient);
	}

	public static void sendQuery(HttpClient client) {
		try {
			List<String> msisdnList = GetMsisdnFromFile.getInstance(
					"msisdn.txt").getMsisdnList();

			OutputStreamWriter writer = new OutputStreamWriter(
					new FileOutputStream("result.txt"), "UTF-8");
			for (String msisdn : msisdnList) {
				long starttime = System.currentTimeMillis();
				ContentResponse response = client
						.newRequest("http://localhost:8888/tracequery")
						.param("querytype", "msisdn").param("index", msisdn)
						.send();

				writer.write(ResultParse.parseResult("{\"results\":"
						+ response.getContentAsString() + "}"));
				long endtime = System.currentTimeMillis();
				long elapsedtime = endtime - starttime;

			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
