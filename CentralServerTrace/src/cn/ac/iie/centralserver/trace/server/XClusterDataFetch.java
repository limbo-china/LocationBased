package cn.ac.iie.centralserver.trace.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import cn.ac.iie.centralserver.trace.data.TraceDBData;
import cn.ac.iie.centralserver.trace.log.LogUtil;

public class XClusterDataFetch implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9103341452512641012L;

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

	private int threadId;
	private static int c_nextId = 1;

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

			in.close();

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public XClusterDataFetch() {
		readConfig();
	}

	public XClusterDataFetch(String addr) {
		readConfig();
		host = addr;
	}

	public ArrayList<TraceDBData> getTraceData(String queryType, String index, String starttime, String endtime) {

		Connection conn = null;
		ResultSet rs = null;
		Statement stmt = null;
		Properties p = new Properties();
		ArrayList<TraceDBData> resList = new ArrayList<TraceDBData>();
		try {

			String url = "jdbc:oscarclusterbulk://" + host + ":" + port + "/" + dbName;

			p.setProperty("user", userName);
			p.setProperty("password", passWord);
			p.setProperty("BulkBufferSize", "100");
			p.setProperty("NodeLoginTimeout", "60");
			conn = DriverManager.getConnection(url, p);
			stmt = conn.createStatement();

			Long start_time = Long.parseLong(starttime);
			Long end_time = Long.parseLong(endtime);

			StringBuffer sentence = new StringBuffer();
			sentence.append("SELECT * FROM ");
			sentence.append(tableName);
			sentence.append(" where");
			sentence.append(" c_" + queryType + " = ").append(index);
			sentence.append(" and c_timestamp >= ").append(start_time);
			sentence.append(" and c_timestamp < ").append(end_time);
			sentence.append(" order by c_timestamp asc");

			LogUtil.info(sentence.toString());
			rs = stmt.executeQuery(sentence.toString());

			String f = null;
			while (rs.next()) {
				if (rs.getString("c_imsi") == null || rs.getString("c_imsi").equals("0")
						|| rs.getString("c_imsi").equals("") || rs.getString("c_uli") == null
						|| rs.getString("c_uli").equals("0") || rs.getString("c_uli").equals("")
						|| rs.getLong("c_timestamp") <= 0) {
					continue;
				}
				TraceDBData metaData = new TraceDBData();
				metaData.setC_imsi(rs.getString("c_imsi"));
				metaData.setC_uli(rs.getString("c_uli"));
				metaData.setC_timestamp(rs.getLong("c_timestamp"));
				if (rs.getString("c_imei") != null)
					metaData.setC_imei(rs.getString("c_imei"));
				if (rs.getString("c_msisdn") != null)
					metaData.setC_msisdn(rs.getString("c_msisdn"));
				if (rs.getString("c_areacode") != null)
					metaData.setC_areacode(rs.getString("c_areacode"));
				resList.add(metaData);
			}

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