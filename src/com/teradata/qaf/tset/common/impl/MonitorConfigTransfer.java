package com.teradata.qaf.tset.common.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.common.Transferable;
import com.teradata.qaf.tset.utils.TSETCSVWriter;

public class MonitorConfigTransfer implements Transferable {

	private static Logger logger = Logger.getLogger(MonitorConfigTransfer.class.getName());
	private Connection conn = null;
	
	public MonitorConfigTransfer() {
		
	}
	
	public MonitorConfigTransfer(Connection conn) {
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
		// 1.generate SQL; 2.execute SQL and write the results into csv files
		try {
			
//			String sql = "SELECT t2.*  FROM TABLE (MonitorPhysicalConfig()) AS t2;";
			String sql = CommonConfig.sqlQueryMonitorPhysicalConfig();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
//			TSETCSVWriter csvWriter = new TSETCSVWriter("TSETInfoTables/" + "MonitorPhysicalConfig.csv");
			TSETCSVWriter csvWriter = new TSETCSVWriter(CommonConfig.path() + 
					CommonConfig.MonitorPhysicalConfig);
			csvWriter.writeCSV(rs);
			logger.info("execute sql : " + sql);
			
//			sql = "SELECT t2.*  FROM TABLE (MonitorvirtualConfig()) AS t2;";
			sql = CommonConfig.sqlQueryVirtualPhysicalConfig();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
//			csvWriter = new TSETCSVWriter("TSETInfoTables/" + "MonitorvirtualConfig.csv");
			csvWriter = new TSETCSVWriter(CommonConfig.path() + 
					CommonConfig.MonitorvirtualConfig);
			csvWriter.writeCSV(rs);
			logger.info("execute sql : " + sql);
				
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			logger.error("ERROR while exporting MonitorXXXConfig, " +
					"ROLLBACK automatically and handle the exception outside.");
			throw new SQLException();
		} finally {
			try {
				if(rs!=null) rs.close();
				if(ps!=null) ps.close();
				//conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
			
		}
	}

	@Override
	public void doImport() {
		// TODO Auto-generated method stub

	}

}
