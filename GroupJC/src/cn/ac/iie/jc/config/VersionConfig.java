package cn.ac.iie.jc.config;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import cn.ac.iie.jc.log.LogUtil;

public class VersionConfig {

	private static Properties prop = null;
	private static HashMap<String, Integer> versionMap = null;

	static {
		prop = new Properties();
		versionMap = new HashMap<String, Integer>();
		loadFile("version.conf");
		rewriteFile("version.conf");
	}

	private static void loadFile(String filename) {
		try {
			prop.load(new FileInputStream(filename));
			for (Object key : prop.keySet()) {
				int version = Integer.parseInt(stampToDate(System.currentTimeMillis())) + 1;
				versionMap.put((String) key, (version > 24) ? 1 : version);
			}
		} catch (Exception e) {
			LogUtil.error("load config file failed for [" + e.getMessage() + "]!");
		}
	}

	private static void rewriteFile(String filename) {
		FileOutputStream output;
		OutputStreamWriter writer;
		try {
			output = new FileOutputStream(filename);
			writer = new OutputStreamWriter(output, "UTF-8");
			writer.write("provinceVersion=" + String.valueOf(versionMap.get("provinceVersion")) + "\n");
			writer.write("cityVersion=" + String.valueOf(versionMap.get("cityVersion")) + "\n");
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static int getInt(String para) {
		int res = 0;
		try {
			res = versionMap.get(para);
		} catch (Exception e) {
			LogUtil.error("parameter [" + para + "] cannot be parsed into Int!");
		}
		return res;
	}
	private static String stampToDate(long stamp) {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}
}
