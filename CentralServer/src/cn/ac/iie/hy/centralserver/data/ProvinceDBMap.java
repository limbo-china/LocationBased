package cn.ac.iie.hy.centralserver.data;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ProvinceDBMap {

	// private static Map<String, String> proDBMap = null;
	private static Properties pps = null;
	private final static String confFilePath = "pro_db_list.properties";
	static {

		pps = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(
					confFilePath));
			pps.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static String getProDBIP(String proCode) {

		// return (String) pps.get(proCode);

		// for test !!!
		return "172.16.18.34:6379 172.16.18.34:6380";
	}
}
