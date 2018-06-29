package cn.ac.iie.hy.centralserver.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import cn.ac.iie.hy.centralserver.data.TraceQueryLBS;

public class XClusterDataFetch implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 9103341452512641012L;
	// 缂虹渷涓� 5 涓嚎绋�
	private static int THREADS_NUM = 5;
	private static String configfile = "ST.properties";
	private static String selectSQL = null;
	private static String targetdir = null;
	private static String userName;
	private static String passWord;
	private static String dbName;
	private static String port;
	private static String host;

	private static String tableName = "t_lbs_trace_history";
	
	// 姣忎釜绾跨▼鐨� id
	private int threadId;
	private static int c_nextId = 1;

	// 浣挎墍鏈夌嚎绋嬩竴璧疯繍琛岀殑鏍囧織
	private static boolean greenLight = false;

	private synchronized static int getNextId() {
		return c_nextId++;
	}

	public static boolean readConfig() {
		try {
			Properties pps = new Properties();

			InputStream in;
			in = new FileInputStream(configfile);
			pps.load(in);
			userName = pps.getProperty("username");
			passWord = pps.getProperty("passwd");
			port = pps.getProperty("port");
			dbName = pps.getProperty("dbname");
			host = pps.getProperty("host");
			
//			System.out.println(userName);
//			System.out.println(passWord);
//			System.out.println(port);
//			System.out.println(dbName);
//			System.out.println(host);
//			System.out.println("thread num " + THREADS_NUM);

			in.close();
			
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}
	
//	public static void main(String args[]) {
//		try {
//			try {
//				Class.forName("com.oscar.cluster.BulkDriver");
//			} catch (java.lang.ClassNotFoundException e) {
//				e.printStackTrace();
//			}
//			String imsi = "460010192518027";
//			String starttime = "1497177244";
//			String endtime = "1500552635";
//			new XClusterDataFetch().getTraceData("imsi", imsi, starttime, endtime);;
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//	 
	public XClusterDataFetch() {
		readConfig();
	}
	public XClusterDataFetch(String addr) {
		readConfig();
		host = addr;
	}
	public ArrayList<TraceQueryLBS> getTraceData(String queryType, String index, String starttime, String endtime) {
		
		Connection conn = null;
		ResultSet rs = null;
		Statement stmt = null;
		Properties p = new Properties();
		ArrayList<TraceQueryLBS> resList = new ArrayList<TraceQueryLBS>();
		try {
			// 鍒涘缓 Statment 瀵硅薄
			String url = "jdbc:oscarclusterbulk://" + host + ":" + port + "/" + dbName;
			p.setProperty("user", userName);
			p.setProperty("password", passWord);
			p.setProperty("BulkBufferSize", "100");
			p.setProperty("NodeLoginTimeout", "60");
			conn = DriverManager.getConnection(url, p);
			stmt = conn.createStatement();

			Long start_time = Long.parseLong(starttime);
			Long end_time = Long.parseLong(endtime);
			// 鑾峰緱缁撴灉闆�
			StringBuffer sentence = new StringBuffer();
			sentence.append("SELECT * FROM ");
			sentence.append(tableName);
			sentence.append(" where");
			sentence.append(" c_" + queryType + " = ").append(index);
			sentence.append(" and c_timestamp >= ").append(start_time);
			sentence.append(" and c_timestamp < ").append(end_time);
			
			System.out.println(sentence.toString());
			rs = stmt.executeQuery(sentence.toString());
			
			String f = null;
			while (rs.next()) {
				if(rs.getString("c_imsi") == null || rs.getString("c_imsi").equals("0") || rs.getString("c_imsi").equals("") ||
						rs.getString("c_uli") == null || rs.getString("c_uli").equals("0") || rs.getString("c_uli").equals("") ||
						rs.getLong("c_timestamp") <= 0){
					continue;
				}
				TraceQueryLBS metaData = new TraceQueryLBS();
				metaData.setC_imsi(rs.getString("c_imsi"));
				metaData.setC_uli(rs.getString("c_uli"));
				metaData.setC_timestamp(rs.getLong("c_timestamp"));
				if(rs.getString("c_imei") != null)
					metaData.setC_imei(rs.getString("c_imei"));
				if(rs.getString("c_msisdn") != null)
					metaData.setC_msisdn(rs.getString("c_msisdn"));
				if(rs.getString("c_areacode") != null)
					metaData.setC_areacode(rs.getString("c_areacode"));
				
//				f = rs.getString("c_imsi") + ":" + rs.getString("c_imei") + ":" + rs.getString("c_msisdn") + ":" + rs.getString("c_uli") + ":" +  rs.getString("c_areacode") + ":" + rs.getString("c_timestamp");
				resList.add(metaData);
//				System.out.println(f);					
			}
			// 鍏抽棴璧勬簮
			rs.close();
			rs = null;
			stmt.close();
			stmt = null;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resList;
	}

}