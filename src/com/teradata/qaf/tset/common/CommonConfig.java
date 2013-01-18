package com.teradata.qaf.tset.common;

public class CommonConfig {

	// 
	public static final String PATH = "TSETInfoTables/";
	// SQL statement to query DDL
	public static String sqlQueryDDL = "select requesttext from dbc.tables " +
			"where databasename='" + DBConn.getDatabase() + 
			"' order by createtimestamp";
	// SQL statement to query Access Right(Authority)
	public static String sqlQueryAccessright = "select accessright from dbc.allrights " +
			"where databasename='" + tableName.split("\\.")[0] + 
			"' and username='" + this.userName + 
			"' and tablename='" + tableName.split("\\.")[1] + "'";
	// SQL statement to grant select of table to user
	//public static String sqlGrantSelect = "GRANT RETRIEVE/SELECT ON '" +
	public static String sqlGrantSelect = "GRANT SELECT ON '" + tableName + 
			"' to '" + this.userName + "';";
	// SQL statement to revoke select of table to user
	//public static String sqlRevokeSelect = "REVOKE RETRIEVE/SELECT ON '" +
	public static String sqlRevokeSelect = "REVOKE SELECT ON '" + tableName + 
			"' to '" + this.userName + "';";
	// SQL statement to grant insert of table to user
	public static String sqlGrantInsert = "GRANT INSERT ON '" + tableName + 
			"' to '" + this.userName + "';";
	//SQL statement to revoke insert of table to user
	public static String sqlRevokeInsert = "REVOKE INSERT ON '" + tableName + 
			"' to '" + this.userName + "';";
	// 
	public static String sqlQueryMetaDB = "select * from " + table.getName() + ";";
	//
	public static String sqlInsertMetaDB = "insert into " + table.getName() + 
			" values(" + cols + ")";
	// 
	public static String sqlQueryMonitorPhysicalConfig = "SELECT t2.* " +
			"FROM TABLE (MonitorPhysicalConfig()) AS t2;";
	// 
	
}
