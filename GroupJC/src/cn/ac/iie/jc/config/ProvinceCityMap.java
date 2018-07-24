package cn.ac.iie.jc.config;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Properties;

import cn.ac.iie.jc.log.LogUtil;

public class ProvinceCityMap {

	private static Properties prop = null;
	private static HashMap<String, String> provinceCityMap = null;

	static {
		prop = new Properties();
		provinceCityMap = new HashMap<String, String>();
		loadFile("city.conf");
	}

	private static void loadFile(String filename) {
		try {
			prop.load(new InputStreamReader(new FileInputStream(filename),
					"UTF-8"));
			for (Object key : prop.keySet())
				provinceCityMap.put((String) key,
						prop.getProperty((String) key));
		} catch (Exception e) {
			LogUtil.error("load config file failed for [" + e.getMessage()
					+ "]!");
		}
	}

	public static String getProvCity(String para) {
		String value = provinceCityMap.get(para);
		return value;
	}

}
