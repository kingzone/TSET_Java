package com.teradata.qaf.tset.importer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.Authority;
import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.common.RollBack;
import com.teradata.qaf.tset.pojo.MetaDB;
import com.teradata.qaf.tset.pojo.TSETInfoTables;
import com.teradata.qaf.tset.pojo.Table;

public class ImpRollBackImpl implements RollBack {

	private final Logger logger = Logger.getLogger(this.getClass());
	private Authority au;
	private TSETInfoTables tsetInfoTables;
	private Connection conn;
	
	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public TSETInfoTables getTsetInfoTables() {
		return tsetInfoTables;
	}

	public void setTsetInfoTables(TSETInfoTables tsetInfoTables) {
		this.tsetInfoTables = tsetInfoTables;
	}

	public ImpRollBackImpl(Authority au) {
		this.au = au;
	}
	
	@Override
	public void doRollBack() {
		// drop tables, delete records, revoke if necessary
		
		this.dropDDLs();
		logger.info("RollBack: Drop tables success.");
		
		try {
			this.deleteMetaDBRecords();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		logger.info("RollBack: Delete metaDBs' records success.");
		
		this.au.revoke();
		logger.info("RollBack: Revoke success.");
		
		//System.exit(-1);
	}
	
	// clear all the metaDBs' tables
	private void deleteMetaDBRecords() throws SQLException {
		for(MetaDB metaDB : tsetInfoTables.getMetaDBList()) {
			logger.info("Now clearing MetaDB: " + metaDB.getName());
			for(Table table : metaDB.getTableList()) {
				PreparedStatement ps = conn.prepareStatement(CommonConfig.sqlClearMetaDBTable(table));
				ps.execute();
				ps.close();
			}
			
		}
		conn.close();
	}
	
	// drop tables that already created 
	private void dropDDLs() {
		
	}

}
