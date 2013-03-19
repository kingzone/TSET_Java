package com.teradata.qaf.tset.common.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.common.DBConn;
import com.teradata.qaf.tset.common.Transferable;
import com.teradata.qaf.tset.utils.SQLReader;
import com.teradata.qaf.tset.utils.SQLWriter;

public class DDLTransfer implements Transferable {

	private static Logger logger = Logger.getLogger(DDLTransfer.class.getName());
	private Connection conn;
	private List<String> sqlListDrop;
	
	public DDLTransfer(Connection conn) {
		this.conn = conn;
		this.sqlListDrop = new ArrayList<String>();
	}
	
	@Override
	public String getGeneratedSQL() {
		return this.generateSQL(DBConn.getDatabase());
	}
	
	// generate SQL statement for exporting
	public String generateSQL(String database) {
		String sql = CommonConfig.sqlQueryDDL(database);;
		return sql;
	}
	
	// Execute show table to get DDLs(CREATE and DROP)
	/**
	 * 
	 * @param TableName : The name of Production DB's table
	 * @param TableKind : The kind of table 
	 * @return
	 * @throws SQLException
	 */
	private String showTable(String TableName, String TableKind) 
			throws SQLException {
		String sqlCREATE = "";
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = null;
		switch(TableKind) {
		case "T":
			sql = CommonConfig.sqlShowTable + "\"" + TableName + "\"";
			this.sqlListDrop.add(CommonConfig.sqlDropTable + 
					"\"" + TableName + "\"");
			break;
		case "V":
			sql = CommonConfig.sqlShowView + "\"" + TableName + "\"";
			this.sqlListDrop.add(CommonConfig.sqlDropView + 
					"\"" + TableName + "\"");
			break;
		case "M":
			sql = CommonConfig.sqlShowMacro + "\"" + TableName + "\"";
			this.sqlListDrop.add(CommonConfig.sqlDropMacro + 
					"\"" + TableName + "\"");
			break;
		case "P":
		case "E":
			sql = CommonConfig.sqlShowProcedure + "\"" + TableName + "\"";
			this.sqlListDrop.add(CommonConfig.sqlDropProcedure + 
					"\"" + TableName + "\"");
			break;
		case "D":
			break;
		case "R":
		case "F":
			sql = CommonConfig.sqlShowFunction + "\"" + TableName + "\"";
			this.sqlListDrop.add(CommonConfig.sqlDropFunction + 
					"\"" + TableName + "\"");
			break;
		default:
			break;
		}
		if(sql == null) return null;
		ps = conn.prepareStatement(sql);
		logger.info(sql);
		rs = ps.executeQuery();
		while(rs.next()) {
			sqlCREATE = rs.getString(1);
		}
		
		// Remove the DB name in the CREATE statement
		sqlCREATE = sqlCREATE.replace(DBConn.getDatabase() + ".", "");
		sqlCREATE = sqlCREATE.replace(DBConn.getDatabase().toUpperCase() + ".", "");
		sqlCREATE = sqlCREATE.replace("\"" + DBConn.getDatabase() + "\".", "");
		sqlCREATE = sqlCREATE.replace("\"" + DBConn.getDatabase().toUpperCase() + "\".", "");
		rs.close();
		ps.close();
		return sqlCREATE;
	}

	/**
	 * Reverse the tables sequence to generate DROP statements.
	 * @param list
	 * @return the reversed list
	 */
	private List<String> reverseList(List<String> list) {
		List<String> li = new ArrayList<String>();
		ListIterator<String> iter = list.listIterator(list.size());
		while(iter.hasPrevious()) {
			li.add(iter.previous());
		}
		return li;
	}
	
	@Override
	public void doExport() throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(getGeneratedSQL());
			logger.info(getGeneratedSQL());
			rs = ps.executeQuery();
			List<String> sqlList = new ArrayList<String>();
			while(rs.next()) {
				
				//sqlList.add(rs.getString("requesttext"));
				sqlList.add(this.showTable(rs.getString("TableName").trim(), 
						rs.getString("TableKind").trim()));
			}
			// can use multi-thread, fork a new thread to write file
			SQLWriter.setFileName(CommonConfig.path() + "CREATE.sql");
					//DBConn.getDatabase() + ".sql");
			SQLWriter.writeSQL(sqlList);
			SQLWriter.setFileName(CommonConfig.path() + "DROP.sql");
			SQLWriter.writeSQL(this.reverseList(this.sqlListDrop));
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			logger.error("ERROR while exporting DDLs, ROLLBACK automatically " +
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

	/**
	 * Create all the tables firstly order by createtimestamp, and then create views.
	 */
	@Override
	public void doImport() throws SQLException {
		PreparedStatement ps = null;
		boolean res = false;
		try {
			
			List<String> sqlList = SQLReader.readSQL(CommonConfig.path("CREATE.sql"));
					//DBConn.getDatabase() + ".sql");
			Iterator<String> it = sqlList.iterator();
			int countAll = 1;
			conn.setAutoCommit(false);
			// Firstly, create all the tables
			while(it.hasNext()) {
				
				String sqlTemp = it.next();
				
				// skip the specific lines. 
				if(sqlTemp.matches("\\s+") || sqlTemp.toUpperCase().startsWith("BEGIN LOADING") 
						|| sqlTemp.toLowerCase().matches("create.*as.*with.*data.*") 
						|| sqlTemp.contains("CREATE SET TABLE pct_hist_usr.Test_Case")) continue;
				
				// split the string with one or more spaces
				String []arr = sqlTemp.trim().split("\\s+");
				if ((arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("table"))
						|| (arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("multiset") && arr[2].equalsIgnoreCase("table"))
						|| (arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("set") && arr[2].equalsIgnoreCase("table")))
				{
					logger.info("["+countAll+"/"+sqlList.size()+"]" + sqlTemp);
					ps = conn.prepareStatement(sqlTemp);
					
					res = ps.execute();
					conn.commit();
					++ countAll;
				}
			}
			
			conn.commit();
			logger.info("Totally create table count: " + countAll);
			
			// Secondly, create all the other Objects(views, etc.) except tables 
			ps = null;
			it = sqlList.iterator();
			conn.setAutoCommit(false);
			while(it.hasNext()) {
				
				String sqlTemp = it.next();
				
				// empty string 
				if(sqlTemp.matches("\\s+") || sqlTemp.toUpperCase().startsWith("BEGIN LOADING") 
						|| sqlTemp.toLowerCase().matches("create.*as.*with.*data.*")
						|| sqlTemp.contains("REPLACE VIEW PCT_PTE_VIEW")) {
					// log only once and increment the variable countAll.
					logger.info("*["+countAll+"/"+sqlList.size()+"]" + sqlTemp);
					++ countAll;
					continue;
				} 
				
				String []arr = sqlTemp.trim().split("\\s+");
				if (!((arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("table"))
						|| (arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("multiset") && arr[2].equalsIgnoreCase("table"))
						|| (arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("set") && arr[2].equalsIgnoreCase("table"))))
				{
					logger.info("["+countAll+"/"+sqlList.size()+"]" + sqlTemp);
					ps = conn.prepareStatement(sqlTemp);
					res = ps.execute();
					conn.commit();
					++ countAll;
				}
			}
			
			conn.commit();
			logger.info("Totally create objects(tables,views,etc.) count: " + countAll);
			
			logger.info("return value: " + res);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			logger.error("ERROR while importing DDLs, ROLLBACK automatically " +
					"and handle the exception outside.");
			throw new SQLException("3807");
			
		} finally {
			try {
				if(ps!=null) ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
		
	}
	/**
	// Create all the tables and view order by createtimestamp
	public void doImport() {
		PreparedStatement ps = null;
		boolean res = false;
		try {
			//conn.setAutoCommit(false);
			String sql = SQLReader.readSQL("TSETInfoTables/" + DBConn.getDatabase() + ".sql");
			List<String> sqlList = this.generateImportSQL(sql);
			Iterator<String> it = sqlList.iterator();
			int countAll = 1;
			conn.setAutoCommit(false);
			// Firstly, create all the tables
			while(it.hasNext()) {
				String sqlTemp = it.next();
				// BEGIN LOADING and empty string 
				//if(sqlTemp.equals("") || sqlTemp.startsWith("BEGIN LOADING")) continue;
				//if(sqlTemp.equals("") || sqlTemp.equals(" ")) continue;
				if(sqlTemp.matches("\\s+")) continue;
//				if(sqlTemp.startsWith("CREATE VIEW")) 
//				{
//					int[] count = ps.executeBatch();
//					
//					conn.commit();
//					countAll += count.length;
//					logger.info(count.length + " tables was created.");
//				}
				String []arr = sqlTemp.trim().split("\\s+");
				//if(sqlTemp.substring(0, 11).equalsIgnoreCase("create table") || sqlTemp.substring(0, 21).equalsIgnoreCase("create multiset table"))
				if ((arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("table"))
						|| (arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("multiset") && arr[1].equalsIgnoreCase("table")))
				{
					logger.info("["+countAll+"/"+sqlList.size()+"]" + sqlTemp);
					if(sqlTemp.toLowerCase().indexOf("with") >= 0) {
						ps.executeBatch();
						conn.commit();
					}
					ps = conn.prepareStatement(sqlTemp);
					ps.addBatch();
					//res = ps.execute();
					//conn.commit();
					++ countAll;
				}
			}
//			int[] count = ps.executeBatch();
//			logger.info(count.length + " tables was created.");
//			countAll += count.length;
			ps.executeBatch();
			conn.commit();
			logger.info("Totally create table count: " + countAll);
			
			// Secondly, create all the other Objects(views, etc.) except tables 
			ps = null;
			it = sqlList.iterator();
			conn.setAutoCommit(false);
			while(it.hasNext()) {
				String sqlTemp = it.next();
				// empty string 
				if(sqlTemp.matches("\\s+")) continue;
				String []arr = sqlTemp.split("\\s+");
				if (!((arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("table"))
						|| (arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("multiset") && arr[1].equalsIgnoreCase("table"))))
				{
					logger.info("["+countAll+"/"+sqlList.size()+"]" + sqlTemp);
					ps = conn.prepareStatement(sqlTemp);
					ps.addBatch();
					++ countAll;
				}
			}
			ps.executeBatch();
			conn.commit();
			logger.info("Totally create objects(tables,views,etc.) count: " + countAll);
			
			logger.info("return value: " + res);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			try {
				conn.rollback();
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			System.exit(1);
		} finally {
			try {
				if(ps!=null) ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
*/
}
