package cn.ac.iie.jc.query.config;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Properties;

import cn.ac.iie.jc.query.log.LogUtil;

public class ConfigUtil {

	private static Properties prop = null;
	private static HashMap<String, String> configMap = null;

	static {
		prop = new Properties();
		configMap = new HashMap<String, String>();
		loadFile("jc-query.conf");
	}

	private static void loadFile(String filename) {
		try {
			LogUtil.info("loading config file " + filename);
			prop.load(new FileInputStream(filename));
			for (Object key : prop.keySet())
				configMap.put((String) key, prop.getProperty((String) key));
		} catch (Exception e) {
			LogUtil.error("load config file failed for [" + e.getMessage()
					+ "]!");
		}
	}

	public static String getString(String para) {
		String value = configMap.get(para);
		if (value == null) {
			LogUtil.error("parameter [" + para + "] in config file not found!");
			throw new RuntimeException();
		}
		return value;
	}

	public static int getInt(String para) {
		int res = 0;
		try {
			res = Integer.parseInt(getString(para));
		} catch (Exception e) {
			LogUtil.error("parameter [" + para + "] cannot be parsed into Int!");
		}
		return res;
	}

}
