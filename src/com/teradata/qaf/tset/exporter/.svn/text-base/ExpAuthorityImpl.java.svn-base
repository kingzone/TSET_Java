package com.teradata.qaf.tset.exporter;

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

public class ExpAuthorityImpl implements Authority {

	private static final Logger logger = Logger.getLogger(ExpAuthorityImpl.class.getName());
	
	private String databaseName;
	private TSETInfoTables tsetInfoTables;
	private String userName;
	private Connection conn;
	
	private List<String> needGrant;
	private List<String> needGrantEF;
	
	public ExpAuthorityImpl(Connection conn, TSETInfoTables tsetInfoTables, 
			String databaseName, String userName) {
		this.needGrant = new ArrayList<String>();
		this.needGrantEF = new ArrayList<String>();
		this.conn = conn;
		this.databaseName = databaseName;
		this.tsetInfoTables = tsetInfoTables;
		this.userName = userName;
	}
	
	/**
	 * Check accessRight of userName on tableName(mainly EF privilege)
	 * @param tableName
	 * @param userName
	 * @param accessRight
	 * @throws SQLException
	 */
	private void check(String tableName, String userName, String accessRight) 
			throws SQLException {
		// Check accessRight on specified table
		String sql = CommonConfig.sqlQueryAccessright(tableName, userName);
		logger.info(sql);
		PreparedStatement ps = conn.prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		boolean flag = true;
		while(rs.next()) {
			if(rs.getString("accessRight").trim().equalsIgnoreCase(accessRight)) {
				flag = false;
				logger.info("NO Need Grant " + accessRight + " on table: " + tableName);
				break;
			}
		}
		
		// Check accessRight on ALL
		sql = CommonConfig.sqlQueryAccessrightonALL(tableName, this.userName);
		logger.info(sql);
		ps = conn.prepareStatement(sql);
		rs = ps.executeQuery();
		while(rs.next()) {
			if(rs.getString("accessRight").trim().equalsIgnoreCase(accessRight)) {
				flag = false;
				logger.info("NO Need Grant " + accessRight + " on table: " + tableName);
				break;
			}
		}
		
		if(flag) {
			logger.info("Need Grant " + accessRight + " on table: " + tableName);
			// This method is mainly used to check EF privilege 
			this.needGrantEF.add(tableName);
			
		}
		rs.close();
		ps.close();
	}
	
	@Override
	public void check() {
		check(true);
	}
	
	public void check(boolean checkEF) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean flag = true;// Needs Grant R
		try {
			// Check R on ALL of DBC
			String sql = CommonConfig.sqlQueryAccessrightonALL("DBC.*", this.userName);
			logger.info(sql);
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()) {
				if(rs.getString("accessRight").trim().equalsIgnoreCase("R")) {
					flag = false;
					logger.info("NO Need Grant R on ALL tables of DBC.");
					break;
				}
			}
			if(flag) {
				Iterator<MetaDB> it = this.tsetInfoTables.getMetaDBList().iterator();
				while(it.hasNext()) {
					MetaDB metaDB = it.next();
					// Needs R on either ALL(all tables of specified DataBase) 
					// or the specified table.
					Iterator<Table> itTable = metaDB.getTableList().iterator();
					while(itTable.hasNext()) {
						String tableName = itTable.next().getName();
						
						// Check R on specified table
						sql = CommonConfig.sqlQueryAccessright(tableName, this.userName);
						logger.info(sql);
						ps = conn.prepareStatement(sql);
						rs = ps.executeQuery();
						flag = true;
						while(rs.next()) {
							if(rs.getString("accessRight").trim().equalsIgnoreCase("R")) {
								flag = false;
								logger.info("NO Need Grant R on table: " + tableName);
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
							if(rs.getString("accessRight").trim().equalsIgnoreCase("R")) {
								flag = false;
								logger.info("NO Need Grant R on tables of DB: " + tableName);
								break;
							}
						}
						
						if(flag) {
							logger.info("Need Grant R on table: " + tableName);
							this.needGrant.add(tableName);
							
						}
					}
					
				}
			}
			
			// Check EF on SYSLIB FUNCTIONs
			if(checkEF) {
				this.check("SYSLIB.MonitorPhysicalConfig", this.userName, "EF");
				this.check("SYSLIB.MonitorVirtualConfig", this.userName, "EF");
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
			logger.error("Check Export privileges ERROR, " +
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

	/**
	 * Grant accessRight on tableName to userName
	 * @param tableName
	 * @param userName
	 * @param accessRight
	 * @throws SQLException
	 */
	private void grant(String tableName, String userName, String accessRight) 
			throws SQLException {
		PreparedStatement ps = null;
		
		String sql = CommonConfig.sqlGrant(tableName, userName, accessRight);
		ps = conn.prepareStatement(sql);
		ps.execute();
		logger.info(sql);
		ps.close();
	}
	
	/**
	 * Grant EXECUTE FUNCTION privilege to userName on tableNames
	 * @param userName
	 * @param accessRight
	 */
	private void grant(String userName, String accessRight) {
		Iterator<String> it = this.needGrantEF.iterator();
		
		while(it.hasNext()) {
			String tableName = it.next();
			try {
				this.grant(tableName, userName, accessRight);
			} catch (SQLException e) {
				
				e.printStackTrace();
				logger.error(e.getMessage());
				this.revoke();
				
				DBConn.closeConnection(conn);
				
				logger.error("Grant Export privileges ERROR, " +
						"ROLLBACK automatically and Exit the application.");
				System.exit(-1);
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
			String sql = CommonConfig.sqlGrantSelect(tableName, this.userName);
			try {
				ps = conn.prepareStatement(sql);
				ps.execute();
				logger.info(sql);
			} catch (SQLException e) {
				
				e.printStackTrace();
				logger.error(e.getMessage());
				this.revoke();
				
				DBConn.closeConnection(conn);
				
				logger.error("Grant Export privileges ERROR, " +
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
		
		// Grant EF on MonitorPhysicalConfig or MonitorVirtualConfig
		this.grant(this.userName, "EXECUTE FUNCTION");
		
	}

	/**
	 * Revoke accessRight on tableName from userName
	 * @param tableName
	 * @param userName
	 * @param accessRight
	 * @throws SQLException
	 */
	private void revoke(String tableName, String userName, String accessRight) 
			throws SQLException {
		PreparedStatement ps = null;
		
		String sql = CommonConfig.sqlRevoke(tableName, userName, accessRight);
		ps = conn.prepareStatement(sql);
		ps.execute();
		logger.info(sql);
		ps.close();
	}
	
	/**
	 * 
	 * @param userName
	 * @param accessRight
	 */
	private void revoke(String userName, String accessRight) {
		Iterator<String> it = this.needGrantEF.iterator();
		
		while(it.hasNext()) {
			String tableName = it.next();
			try {
				this.revoke(tableName, userName, accessRight);
			} catch (SQLException e) {
				
				e.printStackTrace();
				logger.error(e.getMessage());
				
				DBConn.closeConnection(conn);
				
				logger.error("Revoke Export privileges ERROR, " +
						"ROLLBACK automatically and Exit the application.");
				System.exit(-1);
			}
		}
	}
	
	@Override
	public void revoke() {

		PreparedStatement ps = null;
		Iterator<String> it = this.needGrant.iterator();
		
		
		while(it.hasNext()) {
			String tableName = it.next();
			String sql = CommonConfig.sqlRevokeSelect(tableName, this.userName);
			try {
				ps = conn.prepareStatement(sql);
				ps.execute();
				logger.info(sql);
			} catch (SQLException e) {
				
				e.printStackTrace();
				logger.error(e.getMessage());
				
				DBConn.closeConnection(conn);
				logger.error("Revoke Export privileges ERROR, " +
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
		// Revoke EF privilege
		this.revoke(this.userName, "EXECUTE FUNCTION");
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

	public List<String> getNeedGrant() {
		return needGrant;
	}

	public void setNeedGrant(List<String> needGrant) {
		this.needGrant = needGrant;
	}

}
