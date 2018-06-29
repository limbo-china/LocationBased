package cn.ac.iie.hy.centralserver.dbutils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class GPSDataFromOscar {
	private static String configfile = "data-pro.properties";

	String ULI = null;

	static {
		try {
			Class.forName("com.oscar.Driver");
		} catch (java.lang.ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static String userName;
	private static String passWord;
	private static String dbName;
	private static String port;
	private static String host;

	private String lngi = "0.0";
	private String lati = "0.0";
	private String province = "null";
	private String city = "null";
	private String district = "null";
	private String name = "null";

	static {
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
		}
	}

	public static void main(String args[]) {
		try {

			System.out.println(new GPSDataFromOscar("460-00-26641-31662").getGPS());
			//System.out.println(new GPSDataFromOscar("460-00-26641-31662").getGPS());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public GPSDataFromOscar(String uli) {
		super();
		this.ULI = uli;
	}

	public String getGPS() {
		Connection conn = null;
		ResultSet rs = null;
		Statement stmt = null;

		String sql = null;
		try {

			String url = "jdbc:oscarcluster://" + host + ":" + port + "/" + dbName;
			System.out.println(userName + "-" + passWord);
			conn = DriverManager.getConnection(url, userName, passWord);

			stmt = conn.createStatement();

			sql = "select * from carrier_cell_info_t where id = '" + ULI + "'";
			rs = stmt.executeQuery(sql);

			while (rs.next()) {

				lngi = rs.getString("longitude");
				lati = rs.getString("latitude");
				province = rs.getString("prov_name");
				city = rs.getString("city_name");
				district = rs.getString("district_name");
				name = rs.getString("address");
			}

			rs.close();
			rs = null;
			stmt.close();
			stmt = null;
			conn.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return lngi + "," + lati + "," +province + ","+city+","+district+","+name;

	}

}