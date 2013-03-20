package com.teradata.qaf.tset.run;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.teradata.tset2.pgsql.PgSQLUtils;

public class PgSQLUtilsRun {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PgSQLUtils pgu = new PgSQLUtils();
		Connection conn = pgu.getPgConnection(
				"src/com/teradata/tset2/pgsql/PgSQL.properties");
		String sql = "select * from \"TSET2_History\"";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()) {
				System.out.println(rs.getString("TableName"));
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs!=null && !rs.isClosed()) rs.close();
				if(ps!=null && !ps.isClosed()) ps.close();
				if(conn!=null && !conn.isClosed()) conn.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

}
