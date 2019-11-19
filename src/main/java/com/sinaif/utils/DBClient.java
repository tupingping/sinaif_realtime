package com.sinaif.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.sinaif.utils.Constants.Table;

public class DBClient {

	private static BoneCP connectionPool = null;

	private static volatile boolean initDone = false;
	/**
	 * 注意静态代码块执行时间
	 * 
	 */
	static {
		init();
	}

	public static synchronized void init() {
		if (!initDone) {
			try {
				Properties pro = new Properties();
				Class.forName("com.mysql.jdbc.Driver");
				pro.load(DBClient.class.getClassLoader().getResourceAsStream("jdbc.properties"));
				BoneCPConfig config = new BoneCPConfig(pro);
				connectionPool = new BoneCP(config);
				initDone = true;
			} catch (Exception e) {
				try {
					throw new Exception("init BoneCP failed, please check config", e);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	};

	public static void shutdown() {
		connectionPool.shutdown();
	}

	public static int insert(String sql, Object... args) {
		return executeUpdate(sql, args);
	}

	public static int delete(String sql, Object... args) {
		return executeUpdate(sql, args);
	}

	public static int update(String sql, Object... args) {
		return executeUpdate(sql, args);
	}

	public static void select(String sql, ResultHandler resultHandler, Object... args) {
		executeQuery(sql, resultHandler, args);
	}

	public static void execute(String sql, Object... args) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = getConnection();
			if (conn != null) {
				stmt = conn.prepareStatement(sql);
				handleParams(stmt, args);
				stmt.execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeAll(null, stmt, conn);
		}
	}

	public static int executeUpdate(String sql, Object args) {
		Connection conn = null;
		PreparedStatement stmt = null;
		int result = -1;
		try {
			conn = getConnection();
			if (conn != null) {
				stmt = conn.prepareStatement(sql);
				handleParams(stmt, args);
				result = stmt.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeAll(null, stmt, conn);
		}
		return result;
	}

	public static void executeQuery(String sql, ResultHandler resultHandler, Object args) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			if (conn != null) {
				stmt = conn.prepareStatement(sql);
				handleParams(stmt, args);
				rs = stmt.executeQuery();
				resultHandler.handler(rs);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeAll(rs, stmt, conn);
		}
	}

	private static void handleParams(PreparedStatement stmt, Object... args) throws SQLException {
		int i = 1;
		if (args != null) {
			for (Object arg : args) {
				if (arg instanceof String) {
					stmt.setString(i++, (String) arg);
				} else if (arg instanceof Integer) {
					stmt.setInt(i++, (Integer) arg);
				} else if (arg instanceof Date) {
					stmt.setTimestamp(i++, new java.sql.Timestamp(((Date) arg).getTime()));
				} else if (arg instanceof Boolean) {
					stmt.setBoolean(i++, (Boolean) arg);
				}
			}
		}
	}

	// 从池中获取一个连接
	private static Connection getConnection() throws SQLException {
		return connectionPool.getConnection();
	}

	private static void closeAll(ResultSet rs, Statement stmt, Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (conn != null) {
			try {
				conn.close();// 关闭
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		Date now = new Date();
		System.out.println(now);
		String sql = "insert into " + Table.WEIBO_REALTIME_STRING
				+ "(dt, channel, pv, uv, new_uv, utime) values (?, ?, ?, ?, ?, ?) on duplicate key update pv = ?, uv = ?, new_uv = ?, utime = ?";
		execute(sql, "20190520", "test_channel", 2019, 5, 20, now, 2019, 5, 21, now);
	}
}