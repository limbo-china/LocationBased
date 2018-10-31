package cn.ac.iie.centralserver.trace.log;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class LogUtil {

	private static Logger logger = null;
	static {
		Properties pps = new Properties();
		InputStream in = LogUtil.class.getClassLoader().getResourceAsStream(
				"log4j.properties");
		try {
			pps.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		PropertyConfigurator.configure(pps);
		logger = Logger.getLogger(Object.class.getName());
	}

	private LogUtil() {
	}

	public static void info(String msg) {
		logger.info(msg);
	}

	public static void info(int msg) {
		logger.info(msg);
	}

	public static void warn(String msg) {
		logger.warn(msg);
	}

	public static void debug(String msg) {
		logger.debug(msg);
	}

	public static void error(String msg) {
		logger.error(msg);
	}

	public static void main(String[] args) {
		LogUtil.warn("test");
	}
}
