package com.teradata.tset2.pgsql.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.teradata.tset2.pgsql.PgSQLUtils;

public class BaseDAO {

	protected Connection conn;
	
	public BaseDAO() {
		this.conn = (new PgSQLUtils()).getPgConnection(
				"src/com/teradata/tset2/pgsql/PgSQL.properties");
	}
	
	public void execSQL(String sql) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(ps!=null && !ps.isClosed()) ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public PreparedStatement initPreaparedStatement(String sql) 
			throws SQLException {
		return this.conn.prepareStatement(sql);
	}
	
	public void insertBatch(PreparedStatement ps) throws SQLException {
		ps.executeBatch();
	}
	
	public void closeConn() {
		(new PgSQLUtils()).closeConnection(this.conn);
	}
	
}
