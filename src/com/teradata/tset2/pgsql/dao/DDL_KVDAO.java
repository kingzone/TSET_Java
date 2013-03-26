package com.teradata.tset2.pgsql.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.teradata.tset2.pgsql.pojo.DDL_KV;

public class DDL_KVDAO extends BaseDAO {

	private final Logger logger = Logger.getLogger(DDL_KVDAO.class);
	
	public DDL_KVDAO() {
		super();
	}
	
//	public PreparedStatement initPreaparedStatement(String sql) 
//			throws SQLException {
//		return super.conn.prepareStatement(sql);
//	}
	
	public PreparedStatement addBatch(PreparedStatement ps, DDL_KV ddlkv) 
			throws SQLException {
		ps.setString(1, ddlkv.getKey());
		ps.setTimestamp(2, (Timestamp) ddlkv.getDdl_createTimestamp());
		//logger.info(ddlkv.getDdl_createTimestamp());
		ps.setString(3, ddlkv.getDdl_txt());
		ps.addBatch();
		return ps;
	}
	
//	public void insertBatch(PreparedStatement ps) throws SQLException {
//		ps.executeBatch();
//	}
	
	public void insert(List<DDL_KV> ddlkvs) throws SQLException {
		String sql = "insert into DDL_KV(key, ddl_createtimestamp, ddl_text) " +
				"values(?, ?, ?)";
		PreparedStatement ps = super.conn.prepareStatement(sql);
		super.conn.setAutoCommit(false);
		Iterator<DDL_KV> it = ddlkvs.iterator();
		while(it.hasNext()) {
			DDL_KV ddlkv = it.next();
			this.addBatch(ps, ddlkv);
			//logger.info(ddlkv.getKey());
		}
		logger.info(sql);
		int []count = ps.executeBatch();
		super.conn.commit();
		logger.info("Totally insert " + count.length + " DDL statements.");
		if(ps!=null && !ps.isClosed()) ps.close();
	}
	
	public void insert(DDL_KV ddlkv) {
		String sql = "insert into DDL_KV values('" + 
				ddlkv.getKey() + "', '" + 
				ddlkv.getDdl_createTimestamp() + "', '" + 
				ddlkv.getDdl_txt() + "')";
		super.execSQL(sql);
	}
	
	public void delete() {
		
	}
	
	public void update() {
		
	}
	
	public void select() {
		
	}
	
}
