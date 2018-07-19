package cn.ac.iie.jc.query.log;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class LogUtil {

	private static Logger logger = null;
	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(Object.class.getName());
	}

	private LogUtil() {
	}

	public static void info(String msg) {
		logger.info(msg);
	}

	public static void info(StringBuffer msg) {
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
