package cn.ac.iie.jc.log;

import org.apache.http.StatusLine;
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

	public static void info(StatusLine statusLine) {
		logger.info(statusLine);

	}

	public static void info(Object o) {
		logger.info(o);

	}

}
