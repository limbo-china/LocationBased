package cn.ac.iie.jc.config;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Properties;

import cn.ac.iie.jc.log.LogUtil;

public class ProvinceMap {

	private static Properties prop = null;
	private static HashMap<String, String> provinceMap = null;

	static {
		prop = new Properties();
		provinceMap = new HashMap<String, String>();
		loadFile("province.conf");
	}

	private static void loadFile(String filename) {
		try {
			prop.load(new InputStreamReader(new FileInputStream(filename),
					"UTF-8"));
			for (Object key : prop.keySet())
				provinceMap.put((String) key, prop.getProperty((String) key));
		} catch (Exception e) {
			LogUtil.error("load config file failed for [" + e.getMessage()
					+ "]!");
		}
	}

	public static String getString(String para) {
		String value = provinceMap.get(para);
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
