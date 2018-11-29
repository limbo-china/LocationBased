package cn.ac.iie.ssl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class HttpsWithSSL {

	private static Logger logger = Logger.getLogger(HttpsWithSSL.class);

	static HostnameVerifier hv = new HostnameVerifier() {
		public boolean verify(String urlHostName, SSLSession session) {
			return true;
		}
	};

	public static String doGet(String url, String params,
			Map<String, String> headers) throws Exception {
		HttpsURLConnection conn = null;
		OutputStream out = null;
		String rsp = null;
		try {
			try {
				trustAllHttpsCertificates();
				HttpsURLConnection.setDefaultHostnameVerifier(hv);

				String urlStr = url;
				if (StringUtils.isNotEmpty(params))
					urlStr += "?" + params;
				conn = getConnection(new URL(urlStr), "GET", headers);
				// conn.setConnectTimeout(connectTimeout);
				// conn.setReadTimeout(readTimeout);
			} catch (Exception e) {
				logger.error("GET_CONNECTOIN_ERROR, URL = " + url, e);
				throw e;
			}
			try {
				out = conn.getOutputStream();
				rsp = getResponseAsString(conn);
			} catch (IOException e) {
				logger.error("REQUEST_RESPONSE_ERROR, URL = " + url, e);
				throw e;
			}

		} finally {
			if (out != null) {
				out.close();
			}
			if (conn != null) {
				conn.disconnect();
			}
		}

		return rsp;
	}

	public static String doPost(String url, String params,
			Map<String, String> headers) throws Exception {
		HttpsURLConnection conn = null;
		OutputStream out = null;
		String rsp = null;
		try {
			try {
				trustAllHttpsCertificates();
				HttpsURLConnection.setDefaultHostnameVerifier(hv);
				conn = getConnection(new URL(url), "POST", headers);
				// conn.setConnectTimeout(connectTimeout);
				// conn.setReadTimeout(readTimeout);
			} catch (Exception e) {
				logger.error("GET_CONNECTOIN_ERROR, URL = " + url, e);
				throw e;
			}
			try {
				out = conn.getOutputStream();
				byte[] content = {};
				if (StringUtils.isNotEmpty(params)) {
					content = params.getBytes("utf-8");
				}
				out.write(content);
				rsp = getResponseAsString(conn);
			} catch (IOException e) {
				logger.error("REQUEST_RESPONSE_ERROR, URL = " + url, e);
				throw e;
			}

		} finally {
			if (out != null) {
				out.close();
			}
			if (conn != null) {
				conn.disconnect();
			}
		}

		return rsp;
	}

	private static HttpsURLConnection getConnection(URL url, String method,
			Map<String, String> headers) throws IOException {
		HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
		conn.setRequestMethod(method);
		conn.setDoInput(true);
		conn.setDoOutput(true);
		conn.setRequestProperty("Accept", "text/xml,text/javascript,text/html");
		conn.setRequestProperty("User-Agent", "stargate");
		// conn.setRequestProperty("Content-Type", ctype);

		for (Entry<String, String> entry : headers.entrySet()) {
			conn.setRequestProperty(entry.getKey(), entry.getValue());
		}
		return conn;
	}

	private static String getResponseAsString(HttpURLConnection conn)
			throws IOException {
		String charset = getResponseCharset(conn.getContentType());
		InputStream es = conn.getErrorStream();
		if (es == null) {
			return getStreamAsString(conn.getInputStream(), charset);
		} else {
			String msg = getStreamAsString(es, charset);
			if (StringUtils.isEmpty(msg)) {
				throw new IOException(conn.getResponseCode() + ":"
						+ conn.getResponseMessage());
			} else {
				throw new IOException(msg);
			}
		}
	}

	private static String getStreamAsString(InputStream stream, String charset)
			throws IOException {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					stream, charset));
			StringWriter writer = new StringWriter();

			char[] chars = new char[256];
			int count = 0;
			while ((count = reader.read(chars)) > 0) {
				writer.write(chars, 0, count);
			}

			return writer.toString();
		} finally {
			if (stream != null) {
				stream.close();
			}
		}
	}

	private static String getResponseCharset(String ctype) {
		String charset = "utf-8";

		if (!StringUtils.isEmpty(ctype)) {
			String[] params = ctype.split(";");
			for (String param : params) {
				param = param.trim();
				if (param.startsWith("charset")) {
					String[] pair = param.split("=", 2);
					if (pair.length == 2) {
						if (!StringUtils.isEmpty(pair[1])) {
							charset = pair[1].trim();
						}
					}
					break;
				}
			}
		}

		return charset;
	}

	private static void trustAllHttpsCertificates() throws Exception {
		// Properties properties = new Properties();
		// properties.load(new FileInputStream("ketstore"));
		// String key = properties.getProperty("orderReceipt.key.path");
		// String pass = properties.getProperty("orderReceipt.key.pass");
		String key = "keystore";
		String pass = "123456";
		KeyStore keystore = KeyStore.getInstance("JKS"); // 创建一个keystore来管理密钥库
		keystore.load(new FileInputStream(key), pass.toCharArray());
		// 创建jkd密钥访问库
		TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
		tmf.init(keystore); // 验证数据，可以不传入key密码
		// 创建TrustManagerFactory,管理授权证书
		SSLContext sslc = SSLContext.getInstance("SSLv3");
		// 构造SSL环境，指定SSL版本为3.0，也可以使用TLSv1，但是SSLv3更加常用。
		sslc.init(null, tmf.getTrustManagers(), null);
		// KeyManager[] 第一个参数是授权的密钥管理器，用来授权验证。第二个是被授权的证书管理器，
		// 用来验证服务器端的证书。只验证服务器数据，第一个管理器可以为null
		// 构造ssl环境
		HttpsURLConnection.setDefaultSSLSocketFactory(sslc.getSocketFactory());
	}

	public static void main(String[] args) {

	}
}