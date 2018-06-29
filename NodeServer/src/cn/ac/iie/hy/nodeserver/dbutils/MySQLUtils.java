package cn.ac.iie.hy.nodeserver.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　　　　　┃
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　　　　　┃
 * 　　┃　　　┻　　　┃
 * 　　┃　　　　　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃神兽保佑, 永无BUG!
 * 　　　　┃　　　┃Code is far away from bug with the animal protecting
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * @author zhangyu
 */
public class MySQLUtils {

	private static DataSource DS;

	/**
	 *
	 * @return 获得数据库连接
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public Connection getConn() {
		Connection con = null;
		if (DS != null) {
			try {
				con = DS.getConnection();
			} catch (Exception e) {
				e.printStackTrace(System.err);
			}

			try {
				con.setAutoCommit(false);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return con;
		}
		return con;
	}

	/** 构造函数，初始化了 DS ，指定 所有参数 */
	public MySQLUtils(String connectURI, String username, String pswd, String driverClass, int initialSize,
			int maxActive, int maxIdle, int maxWait, int minIdle) {
		initDS(connectURI, username, pswd, driverClass, initialSize, maxActive, maxIdle, maxWait, minIdle);
	}

	public MySQLUtils(String connectURI) {
		initDS(connectURI);
	}

	/**
	 * 指定所有参数连接数据源
	 * 
	 * @param connectURI
	 *            数据库
	 * @param username
	 *            用户名
	 * @param pswd
	 *            密码
	 * @param driverClass
	 *            数据库连接驱动名
	 * @param initialSize
	 *            初始连接池连接个数
	 * @param maxtotal
	 *            最大活动连接数
	 * @param maxIdle
	 *            最大连接数
	 * @param maxWaitMillis
	 *            获得连接的最大等待毫秒数
	 * @param minIdle
	 *            最小连接数
	 * @return
	 */
	public static void initDS(String connectURI, String username, String pswd, String driverClass, int initialSize,
			int maxtotal, int maxIdle, int maxWaitMillis, int minIdle) {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(driverClass);
		ds.setUsername(username);
		ds.setPassword(pswd);
		ds.setUrl(connectURI);
		ds.setInitialSize(initialSize); // 初始的连接数；
		// ds.setMaxTotal(maxtotal);
		ds.setMaxIdle(maxIdle);
		// ds.setMaxWaitMillis(maxWaitMillis);
		ds.setMinIdle(minIdle);
		DS = ds;
	}

	/**
	 * 创建数据源，除了数据库外，都使用硬编码默认参数；
	 * 
	 * @param connectURI
	 *            数据库
	 * @return
	 */
	public static void initDS(String connectURI) {
		initDS(connectURI, "iie_usr", "iiecas", "com.mysql.jdbc.Driver", 5, 100, 30, 10000, 1);
	}

	/** 获得数据源连接状态 */
	public static Map<String, Integer> getDataSourceStats() throws SQLException {
		BasicDataSource bds = (BasicDataSource) DS;
		Map<String, Integer> map = new HashMap<String, Integer>(2);
		map.put("active_number", bds.getNumActive());
		map.put("idle_number", bds.getNumIdle());
		return map;
	}

	/** 关闭数据源 */
	protected static void shutdownDataSource() throws SQLException {
		BasicDataSource bds = (BasicDataSource) DS;
		bds.close();
	}

	/**
	 * 关闭数据库连接
	 * 
	 * @param conn数据库连接
	 * @param prsts
	 *            PreparedStatement 对象
	 * @param rs结果集
	 */
	public void closeAll(Connection conn, PreparedStatement prsts, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (prsts != null) {
			try {
				prsts.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 执行增、删、改SQL语句
	 *
	 * @param sql
	 *            sql语句
	 * @param param
	 *            值集
	 * @param type
	 *            值类型集
	 * @return 受影响的行数
	 */
	public int executeUpdate(String sql, Object[] param) {

		int rows = 0;
		Connection conn = this.getConn();
		PreparedStatement prsts = null;
		try {
			prsts = conn.prepareStatement(sql);
			
			for (int i = 1; i <= param.length; i++) {
				prsts.setObject(i, param[i - 1]);
			}
			rows = prsts.executeUpdate();
			conn.commit();
			//System.out.println("受影响行数:" + rows);
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.closeAll(conn, prsts, null);
		}
		return rows;
	}

	/**
	 * 执行查询SQL语句
	 *
	 * @param sql
	 *            sql语句
	 * @param param
	 *            值集
	 * @param type
	 *            值类型集
	 * @return 结果集
	 */
	public List executeQuery(String sql, Object[] param, int[] type) {
		ResultSet rs = null;
		List list = null;
		Connection conn = this.getConn();
		PreparedStatement prsts = null;
		try {
			prsts = conn.prepareStatement(sql);
			for (int i = 1; i <= param.length; i++) {
				prsts.setObject(i, param[i - 1], type[i - 1]);
			}
			rs = prsts.executeQuery();
			list = new ArrayList();
			ResultSetMetaData rsm = rs.getMetaData();
			Map map = null;
			while (rs.next()) {
				map = new HashMap();
				for (int i = 1; i <= rsm.getColumnCount(); i++) {
					map.put(rsm.getColumnName(i), rs.getObject(rsm.getColumnName(i)));
				}
				list.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.closeAll(conn, prsts, rs);
		}
		return list;
	}

}