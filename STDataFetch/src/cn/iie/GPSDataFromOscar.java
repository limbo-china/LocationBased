package cn.iie;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Scanner;

public class GPSDataFromOscar {
	private static String configfile = "ST.properties";

	String number = null;

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

			System.out.println(userName);
			System.out.println(passWord);
			System.out.println(port);
			System.out.println(dbName);
			System.out.println(host);

			in.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private static int count = 0;

	public static void main(String args[]) {
//		try {
//
//			new GPSDataFromOscar("8615201291012").getGPS();
//			//System.out.println(new GPSDataFromOscar("460-00-58274-18811").getGPS());
//			//System.out.println(new GPSDataFromOscar("460-00-26641-31662").getGPS());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		String fileName = "2017-07-22.txt";
//		//Url = args[1];
//		//long start = Long.parseLong(args[1]);
//		long num = 0;
//		//conn = getConn();
//		FileInputStream inputStream = null;
//		Scanner sc = null;
//		try {
//		    inputStream = new FileInputStream(fileName);
//		    sc = new Scanner(inputStream, "UTF-8");
//		    //List<String> cache = new ArrayList<String>();
//		    while (sc.hasNextLine()) {
//		    	String line = sc.nextLine();
//		    	new GPSDataFromOscar(line).getGPS();
//		    	System.out.println(++count);
//		    }
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} finally {
//		    if (inputStream != null) {
//		        try {
//					inputStream.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//		    }
//		    if (sc != null) {
//		        sc.close();
//		    }
//		}
//		Long start = 1506096000L;
//		 
//		 for(int i = 0; i < 30; i++){
//			 new GPSDataFromOscar("").getCal(start);
//			 start+=3600*24;
//		 }
		//new GPSDataFromOscar("").getCal(1508688000);
		new GPSDataFromOscar("").getGPS();
	}

	public GPSDataFromOscar(String number) {
		super();
		this.number = number;
	}
	
	public String timeStamp2Date(String seconds, String format) {
		if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
			return "";
		}
		if (format == null || format.isEmpty())
			format = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		if(seconds.contains("-")){
			sdf.format(new Date(System.currentTimeMillis()));
		}
		
		return sdf.format(new Date(Long.valueOf(seconds + "000")));
	}
	
	public  void getCal(long date){
		Connection conn = null;
		ResultSet rs = null;
		Statement stmt = null;
		String sql = null;
		try {

			String url = "jdbc:oscarcluster://" + host + ":" + port + "/" + dbName;
			//System.out.println(url);

			conn = DriverManager.getConnection(url, userName, passWord);

			stmt = conn.createStatement();
			long nextDate = date + 3600*24;
			sql = "select count(*) from (select count(*) from t_lbs_trace_history where substr(c_imsi, 1, 3) <> '460' and c_timestamp > " + date + " and c_timestamp < "+nextDate+" group by c_msisdn);";
			//System.out.println(sql);
			rs = stmt.executeQuery(sql);
			File file =new File("result-100w" +".txt");
			FileWriter fileWritter = new FileWriter(file.getName(),true);

			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			
			while (rs.next()) {
				Long count = rs.getLong(1);
				//rs.get
				//lati = rs.getString("latitude");
				System.out.println(count);
				//bufferWritter.write(timeStamp2Date(date+"", null) + "," + count + ",");
				//bufferWritter.write(rs.getString("c_msisdn") + "," + rs.getString("c_uli") + "," + rs.getString("c_timestamp") + "\n");
			}
	
			
			//sql = "select count(*) from (select count(*) from t_lbs_trace_history where c_timestamp > " + date + " and c_timestamp < "+nextDate+" group by c_msisdn);";
			
			
			
			rs = stmt.executeQuery(sql);
		

			
			while (rs.next()) {
				Long count = rs.getLong(1);
				//rs.get
				//lati = rs.getString("latitude");
				System.out.println(count);
				//bufferWritter.write(count+"\n");
			}
			bufferWritter.close();
			

			rs.close();
			rs = null;
			stmt.close();
			stmt = null;
			conn.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getGPS() {
		Connection conn = null;
		ResultSet rs = null;
		Statement stmt = null;
		String sql = null;
		try {
			String url = "jdbc:oscarcluster://" + host + ":" + port + "/" + dbName;
			conn = DriverManager.getConnection(url, userName, passWord);
			stmt = conn.createStatement();
			 String sql1 = "select count(*) from t_lbs_trace_history where substr(c_imsi, 1, 3) <> '460'  "
					+ "and  c_timestamp >= unix_timestamp('2017-10-30 00:00:00') and c_timestamp < unix_timestamp('2017-10-31 00:00:00'); ";
			rs = stmt.executeQuery(sql1);
			while (rs.next()) {
				Long count = rs.getLong(1);
				System.out.println("10.30: "+count);
			}
			rs.close();
			rs = null;
			stmt.close();
			stmt = null;
			conn.close();

		} catch (Exception e) {
			e.printStackTrace();
	   }
		return lngi + "," + lati;
	}
}