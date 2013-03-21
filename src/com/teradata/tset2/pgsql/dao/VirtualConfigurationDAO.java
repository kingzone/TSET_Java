package com.teradata.tset2.pgsql.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.teradata.tset2.pgsql.pojo.VirtualConfiguration;

public class VirtualConfigurationDAO extends BaseDAO {
	private final Logger logger = Logger.getLogger(this.getClass());
	
	public PreparedStatement addBatch(
			PreparedStatement ps, 
			VirtualConfiguration vc) 
			throws SQLException {
		ps.setInt(1, vc.getSystem_id());
		ps.setInt(2, vc.getProcid());
		ps.setInt(3, vc.getVprocNo());
		ps.setString(4, vc.getVprocType());
		ps.setInt(5, vc.getHostid());
		ps.setString(6, vc.getStatus());
		ps.setInt(7, vc.getDiskSlice());
		
		ps.addBatch();
		return ps;
	}
	
	public void insert(List<VirtualConfiguration> vcList) throws SQLException {
		String sql = "insert into VirtualConfiguration " +
				"values(?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = super.conn.prepareStatement(sql);
		super.conn.setAutoCommit(false);
		Iterator<VirtualConfiguration> it = vcList.iterator();
		while(it.hasNext()) {
			VirtualConfiguration vc = it.next();
			this.addBatch(ps, vc);
		}
		logger.info(sql);
		int []count = ps.executeBatch();
		logger.info("Update rows: " + count.length); 
		super.conn.commit();
		if(ps!=null && !ps.isClosed()) ps.close();
	}
}
