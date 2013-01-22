package com.teradata.qaf.tset.exporter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

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
	
	public ExpAuthorityImpl() {
		this.needGrant = new ArrayList<String>();
	}
	
	public ExpAuthorityImpl(Connection conn, TSETInfoTables tsetInfoTables, String databaseName, String userName) {
		this();
		this.conn = conn;
		this.databaseName = databaseName;
		this.tsetInfoTables = tsetInfoTables;
		this.userName = userName;
	}
	
	@Override
	public void check() {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			//conn.setAutoCommit(false);
			Iterator<MetaDB> it = this.tsetInfoTables.getMetaDBList().iterator();
			while(it.hasNext()) {
				MetaDB metaDB = it.next();
				// Tables in CostProfiles have no R
				if(!metaDB.getName().equalsIgnoreCase("CostProfiles")) {
					Iterator<Table> itTable = metaDB.getTableList().iterator();
					while(itTable.hasNext()) {
//						String sql = "select accessright from dbc.allrights where databasename='"+this.databaseName+"' and username='"+this.userName+"' and tablename='"+itTable.next().getName()+"'";
//						String sql = "select accessright from dbc.allrights where username='"+this.userName+"' and tablename='"+itTable.next().getName().split("\\.")[1]+"'";
						String tableName = itTable.next().getName();
						String sql = "select accessright from dbc.allrights where databasename='"+tableName.split("\\.")[0]+"' and username='"+this.userName+"' and tablename='"+tableName.split("\\.")[1]+"'";
						logger.info(sql);
						ps = conn.prepareStatement(sql);
						rs = ps.executeQuery();
						boolean flag = true;
						while(rs.next()) {
							//System.out.println(rs.getString("accessRight"));
							if(rs.getString("accessRight").trim().equalsIgnoreCase("R")) {
								flag = false;
								logger.info("NO Need Grant R on table: " + tableName);
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
			
			
			//conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			try {
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
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
		
		// must grant in one transaction
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
			logger.error(e1.getMessage());
		}
		while(it.hasNext()) {
			String tableName = it.next();
//			String sql = "GRANT RETRIEVE/SELECT ON '"+this.databaseName+"'.'"+tableName+"' to '"+this.userName+"';";
			String sql = "GRANT RETRIEVE/SELECT ON '"+tableName+"' to '"+this.userName+"';";
			try {
				ps = conn.prepareStatement(sql);
				ps.execute();
				logger.info(sql);
			} catch (SQLException e) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
				logger.error(e.getMessage());
				DBConn.closeConnection(conn);
			} finally {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
		try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	@Override
	public void revoke() {

		PreparedStatement ps = null;
		Iterator<String> it = this.needGrant.iterator();
		
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
			logger.error(e1.getMessage());
		}
		
		while(it.hasNext()) {
			String tableName = it.next();
//			String sql = "REVOKE RETRIEVE/SELECT ON '"+this.databaseName+"'.'"+tableName+"' to '"+this.userName+"';";
			String sql = "REVOKE RETRIEVE/SELECT ON '"+tableName+"' to '"+this.userName+"';";
			try {
				ps = conn.prepareStatement(sql);
				ps.execute();
				logger.info(sql);
			} catch (SQLException e) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
				logger.error(e.getMessage());
				DBConn.closeConnection(conn);
			} finally {
				try {
					if(ps!=null) ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
		try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
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
