package cn.ac.iie.jc.query.config;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ProvinceRedisMap {

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

	public static String getProRedisIP(String proCode) {

		return pps.getProperty(proCode);

	}
}
