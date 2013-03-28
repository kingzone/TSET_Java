package com.teradata.tset2.pgsql.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.teradata.tset2.pgsql.pojo.PhysicalConfiguration;

public class PhysicalConfigurationDAO extends BaseDAO {

	private final Logger logger = Logger.getLogger(this.getClass());
	
	public PreparedStatement addBatch(
			PreparedStatement ps, 
			PhysicalConfiguration pc) 
			throws SQLException {
		ps.setInt(1, pc.getSystem_id());
		ps.setInt(2, pc.getProcid());
		ps.setString(3, pc.getStatus());
		ps.setString(4, pc.getCPUType());
		ps.setInt(5, pc.getCPUCount());
		
		// for 14.00
		//ps.setString(6, pc.getSystemType());
		//ps.setInt(7, pc.getCliqueNo());
		//ps.setString(8, pc.getNetAUP());
		//ps.setString(9, pc.getNetBUP());
		
		ps.addBatch();
		return ps;
	}
	
	public void insert(List<PhysicalConfiguration> pcList) throws SQLException {
		// for 14.00
		//String sql = "insert into PhysicalConfiguration " +
		//		"values(?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		// for 13.10
		String sql = "insert into PhysicalConfiguration " +
						"values(?, ?, ?, ?, ?)";
		PreparedStatement ps = super.conn.prepareStatement(sql);
		super.conn.setAutoCommit(false);
		Iterator<PhysicalConfiguration> it = pcList.iterator();
		while(it.hasNext()) {
			PhysicalConfiguration pc = it.next();
			this.addBatch(ps, pc);
		}
		logger.info(sql);
		int []count = ps.executeBatch();
		logger.info("Update rows: " + count.length); 
		super.conn.commit();
		if(ps!=null && !ps.isClosed()) ps.close();
	}
	
}
