package cn.ac.iie.jc.db;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import cn.ac.iie.jc.config.ConfigUtil;



public class XClusterDataFetch implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9103341452512641012L;

	
	
	private static String userName = ConfigUtil.getString("dbUserName");
	private static String passWord = ConfigUtil.getString("dbPassWord");
	private static String dbName = ConfigUtil.getString("dbName"); 
	private static String port = ConfigUtil.getString("dbPort");
	private static String host = ConfigUtil.getString("dbHost");

	private static Connection conn = null;
	private static Statement stmt = null;
	private static Properties p = new Properties();
	
	static{
		try {
			try {
				Class.forName("com.oscar.cluster.BulkDriver");
			} catch (ClassNotFoundException e1) {
				e1.printStackTrace();
			}
			
			String url = "jdbc:oscarclusterbulk://" + host + ":" + port + "/" + dbName;

			p.setProperty("user", userName);
			p.setProperty("password", passWord);
			p.setProperty("BulkBufferSize", "100");
			p.setProperty("NodeLoginTimeout", "60");
			conn = DriverManager.getConnection(url, p);
			stmt = conn.createStatement();

			truncateOriginPositionTable();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public static void truncateOriginPositionTable() {	
		
		StringBuffer sentence = new StringBuffer();
		sentence.append("Truncate T_MONITOR_RTPOSITION_T");

		try {
			stmt.executeQuery(sentence.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
}