package com.teradata.qaf.tset.common.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.common.Transferable;
import com.teradata.qaf.tset.utils.TSETCSVWriter;
import com.teradata.tset2.pgsql.dao.PhysicalConfigurationDAO;
import com.teradata.tset2.pgsql.dao.VirtualConfigurationDAO;
import com.teradata.tset2.pgsql.pojo.PhysicalConfiguration;
import com.teradata.tset2.pgsql.pojo.VirtualConfiguration;

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
		return null;
	}

	@Override
	public void doExport() throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;
		// 1.generate SQL; 2.execute SQL and write the results into csv files
		try {
			
			String sql = CommonConfig.sqlQueryMonitorPhysicalConfig();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			TSETCSVWriter csvWriter = new TSETCSVWriter(CommonConfig.path() + 
					CommonConfig.MonitorPhysicalConfig);
			csvWriter.writeCSV(rs);
			
			// Export PhysicalConfiguration to PostgreSQL
			List<PhysicalConfiguration> pcList = 
					new ArrayList<PhysicalConfiguration>();
			rs = ps.executeQuery();
			while(rs.next()) {
				PhysicalConfiguration pc = new PhysicalConfiguration();
				pc.setSystem_id(1);
				pc.setProcid(rs.getInt(1));
				pc.setStatus(rs.getString(2));
				pc.setCPUType(rs.getString(3));
				pc.setCPUCount(rs.getInt(4));
				pc.setSystemType(rs.getString(5));
				pc.setCliqueNo(rs.getInt(6));
				pc.setNetAUP(rs.getString(7));
				pc.setNetBUP(rs.getString(8));
				pcList.add(pc);
				logger.info("Add to pcList.");
			}
			PhysicalConfigurationDAO pcd = new PhysicalConfigurationDAO();
			pcd.insert(pcList);
			pcd.closeConn();
			
			logger.info("execute sql : " + sql);
			
			sql = CommonConfig.sqlQueryMonitorVirtualConfig();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			csvWriter = new TSETCSVWriter(CommonConfig.path() + 
					CommonConfig.MonitorvirtualConfig);
			csvWriter.writeCSV(rs);
			
			// Export VirtualConfiguration to PostgreSQL
			List<VirtualConfiguration> vcList = 
					new ArrayList<VirtualConfiguration>();
			rs = ps.executeQuery();
			while(rs.next()) {
				VirtualConfiguration vc = new VirtualConfiguration();
				vc.setSystem_id(1);
				vc.setProcid(rs.getInt(1));
				vc.setVprocNo(rs.getInt(2));
				vc.setVprocType(rs.getString(3));
				vc.setHostid(rs.getInt(4));
				vc.setStatus(rs.getString(5));
				vc.setDiskSlice(rs.getInt(6));
				vcList.add(vc);
				logger.info("Add to vcList.");
			}
			VirtualConfigurationDAO vcd = new VirtualConfigurationDAO();
			vcd.insert(vcList);
			vcd.closeConn();
			
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

	}

}
