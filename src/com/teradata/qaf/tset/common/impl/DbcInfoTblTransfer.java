package com.teradata.qaf.tset.common.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.Transferable;
import com.teradata.tset2.pgsql.dao.DbcInfoTblDAO;
import com.teradata.tset2.pgsql.pojo.DbcInfoTbl;

public class DbcInfoTblTransfer implements Transferable {

	private Connection conn = null;
	private Logger logger = Logger.getLogger(this.getClass());
	
	public DbcInfoTblTransfer(Connection conn) {
		this.conn = conn;
	}
	
	
	@Override
	public String getGeneratedSQL() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void doExport() throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = "select * from dbc.dbcinfotbl";
			ps = conn.prepareStatement(sql);
			logger.info(sql);
			rs = ps.executeQuery();
			DbcInfoTblDAO dbcInfoTblDao = new DbcInfoTblDAO();
			while(rs.next()) {
				DbcInfoTbl dbcInfoTbl = new DbcInfoTbl();
				dbcInfoTbl.setInfoKey(rs.getString(1));
				dbcInfoTbl.setInfoData(rs.getString(2));
				
				dbcInfoTblDao.insert(dbcInfoTbl);
			}
			
			dbcInfoTblDao.closeConn();
			
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			logger.error("ERROR while exporting dbcinfotbl, " +
					"ROLLBACK automatically " +
					"and handle the exception outside.");
			throw new SQLException();
			//System.exit(-1);
		} finally {
			try {
				if(!rs.isClosed()) rs.close();
				if(!ps.isClosed()) ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
			
		}
	}

	@Override
	public void doImport() throws Exception {
		// TODO Auto-generated method stub

	}

}
