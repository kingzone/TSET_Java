package com.teradata.tset2.pgsql;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class PgSQLUtils {

	/**
	 * Get connection to PostgreSQL according to pgPropertyFile
	 * @param pgPropertyFile : The configuration file of PostgreSQL DB
	 * @return connection to PostgreSQL
	 */
	public Connection getPgConnection(String pgPropertyFile) {
		Connection conn = null;
		try {
			Class.forName("org.postgresql.Driver");
			Properties props = new Properties();
			props.load(new FileReader(pgPropertyFile));
			
			String url = props.getProperty("url", 
					"jdbc:postgresql://localhost/postgres");
			props.setProperty("user", props.getProperty("user", "postgres"));
			props.setProperty("password", props.getProperty("password", ""));
			//props.setProperty("ssl","true");
			conn = DriverManager.getConnection(url, props);
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			
		}
		return conn;
	}
	
	/**
	 * Get connection to PostgreSQL according to the params
	 * @param url
	 * @param user
	 * @param password
	 * @return Connection to PostgreSQL, null when failed
	 */
	public Connection getPgConnection(String url, String user, String password) {
		Connection conn = null;
		try {
			Class.forName("org.postgresql.Driver");
			Properties props = new Properties();
			props.setProperty("user", user);
			props.setProperty("password", password);
			//props.setProperty("ssl","true");
			conn = DriverManager.getConnection(url, props);
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			
		}
		return conn;
	}
	
	/**
	 * Close the connection 
	 * @param conn : Connection needs to be closed
	 * @return true if close success, false otherwise
	 */
	public boolean closeConnection(Connection conn) {
		try {
			if(conn!=null && !conn.isClosed()) conn.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public Connection getPgConnection() {
		Connection conn = null;
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/postgres";
			Properties props = new Properties();
			props.setProperty("user","postgres");
			props.setProperty("password","123");
			//props.setProperty("ssl","true");
			conn = DriverManager.getConnection(url, props);
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(conn!=null && !conn.isClosed()) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return conn;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/postgres";
			Properties props = new Properties();
			props.setProperty("user","postgres");
			props.setProperty("password","123");
			//props.setProperty("ssl","true");
			conn = DriverManager.getConnection(url, props);
			
			String sql = "select * from \"TSET2_History\"";
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()) {
				System.out.println(rs.getString("TableName"));
			}
			sql = "insert into \"TSET2_History\" values(1,1,1,1,1,1," +
					"'2013-03-20','2013/03/20 15:00:00')";
			ps = conn.prepareStatement(sql);
			ps.executeUpdate();
			//String url = "jdbc:postgresql://localhost/test?
			//user=fred&password=secret&ssl=true";
			//Connection conn = DriverManager.getConnection(url);
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs!=null && !rs.isClosed()) rs.close();
				if(ps!=null && !ps.isClosed()) ps.close();
				if(conn!=null && !conn.isClosed()) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

}
