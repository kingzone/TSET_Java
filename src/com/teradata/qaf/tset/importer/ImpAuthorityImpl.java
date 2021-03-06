package com.teradata.qaf.tset.importer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.common.DBConn;
import com.teradata.qaf.tset.common.Authority;
import com.teradata.qaf.tset.pojo.MetaDB;
import com.teradata.qaf.tset.pojo.TSETInfoTables;
import com.teradata.qaf.tset.pojo.Table;

public class ImpAuthorityImpl implements Authority {

private static final Logger logger = Logger.getLogger(ImpAuthorityImpl.class.getName());
	
	private String databaseName;
	private TSETInfoTables tsetInfoTables;
	private String userName;
	private Connection conn;
	
	private List<String> needGrant;
	
	public ImpAuthorityImpl(Connection conn, TSETInfoTables tsetInfoTables, 
			String databaseName, String userName) {
		this.needGrant = new ArrayList<String>();
		this.conn = conn;
		this.databaseName = databaseName;
		this.tsetInfoTables = tsetInfoTables;
		this.userName = userName;
	}
	
	@Override
	public void check() {
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean flag = true;// Needs Grant I
		try {
			// Check I on ALL of DBC
			String sql = CommonConfig.sqlQueryAccessrightonALL("DBC.*", this.userName);
			logger.info(sql);
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()) {
				if(rs.getString("accessRight").trim().equalsIgnoreCase("I")) {
					flag = false;
					logger.info("NO Need Grant I on ALL tables of DBC.");
					break;
				}
			}
			if(flag) {
				Iterator<MetaDB> it = this.tsetInfoTables.getMetaDBList().iterator();
				while(it.hasNext()) {
					MetaDB metaDB = it.next();
					// Needs I on either ALL(all tables of specified DataBase) 
					// or the specified table.
					Iterator<Table> itTable = metaDB.getTableList().iterator();
					while(itTable.hasNext()) {
						String tableName = itTable.next().getName();
						
						// Check I on specified table
						sql = CommonConfig.sqlQueryAccessright(tableName, this.userName);
						logger.info(sql);
						ps = conn.prepareStatement(sql);
						rs = ps.executeQuery();
						flag = true;
						while(rs.next()) {
							if(rs.getString("accessRight").trim().equalsIgnoreCase("I")) {
								flag = false;
								logger.info("NO Need Grant I on table: " + tableName);
								break;
							}
						}
						rs.close();
						ps.close();
						
						// Check R on ALL
						sql = CommonConfig.sqlQueryAccessrightonALL(tableName, this.userName);
						logger.info(sql);
						ps = conn.prepareStatement(sql);
						rs = ps.executeQuery();
						while(rs.next()) {
							if(rs.getString("accessRight").trim().equalsIgnoreCase("I")) {
								flag = false;
								logger.info("NO Need Grant I on tables of DB: " + tableName);
								break;
							}
						}
						
						if(flag) {
							logger.info("Need Grant I on table: " + tableName);
							this.needGrant.add(tableName);
							
						}
					}
					
				}
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			try {
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			// Exit if exception
			logger.error("Check Import privileges ERROR, " +
					"ROLLBACK automatically and Exit the application.");
			System.exit(-1);
		} finally {
			try {
				if(rs!=null) rs.close();
				if(ps!=null) ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
	}

	@Override
	public void grant() {
		PreparedStatement ps = null;
		Iterator<String> it = this.needGrant.iterator();
		
		// cannot put DDL and DCL into one transaction, only DML can
		while(it.hasNext()) {
			String tableName = it.next();
			String sql = CommonConfig.sqlGrantInsert(tableName, this.userName);
			try {
				ps = conn.prepareStatement(sql);
				ps.execute();
				logger.info(sql);
			} catch (SQLException e) {
				
				e.printStackTrace();
				logger.error(e.getMessage());
				this.revoke();
				
				DBConn.closeConnection(conn);
				
				logger.error("Grant Import privileges ERROR, " +
						"ROLLBACK automatically and Exit the application.");
				System.exit(-1);
			} finally {
				try {
					if(!ps.isClosed()) ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}
			}
			
		}
		
	}

	@Override
	public void revoke() {

		PreparedStatement ps = null;
		Iterator<String> it = this.needGrant.iterator();
		
		
		while(it.hasNext()) {
			String tableName = it.next();
			String sql = CommonConfig.sqlRevokeInsert(tableName, this.userName);
			try {
				ps = conn.prepareStatement(sql);
				ps.execute();
				logger.info(sql);
			} catch (SQLException e) {
				
				e.printStackTrace();
				logger.error(e.getMessage());
				
				DBConn.closeConnection(conn);
				logger.error("Revoke Import privileges ERROR, " +
						"ROLLBACK automatically and Exit the application.");
				System.exit(-1);
			} finally {
				try {
					if(ps!=null) ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}
			}
		}
	}

	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public TSETInfoTables getTsetInfoTables() {
		return tsetInfoTables;
	}

	public void setTsetInfoTables(TSETInfoTables tsetInfoTables) {
		this.tsetInfoTables = tsetInfoTables;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

}
