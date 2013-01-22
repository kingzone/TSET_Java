package com.teradata.qaf.tset.common;

import com.teradata.qaf.tset.pojo.TSETInfoTables;

public class CommonConfig {

	// 
	//public static final String PATH = "TSETInfoTables/";
	// SQL statement to query DDL
	public static String sqlQueryDDL = "select requesttext from dbc.tables " +
			"where databasename='" + DBConn.getDatabase() + 
			"' order by createtimestamp";
	
	// Config files path
	public static String DBConfig() {
		return "DBConfig.xml";
	}
	public static String DBConfig_IMP() {
		return "DBConfig_IMP.xml";
	}
	public static String ConfFile_schema(String version) {
		return "ConfFile_schema_" + version + ".xml";
	}
	
	// Connection String
	public static String connectionString(String url, String database, String charset, String tmode) { 
		return url + "database=\"" + database + "\", charset=" + charset + ", tmode=" + tmode;
	}
	
	// Path to store the exported files
	public static String path() {
		return DBConn.getDatabase() + "_" + DBConn.getUsername() + "/TSETInfoTables/";
	}
	
	// SQL statement to query Access Right(Authority)
	public static String sqlQueryAccessright(String tableName, String userName) {
		return "select accessright from dbc.allrights " +
			"where databasename='" + tableName.split("\\.")[0] + 
			"' and username='" + userName + 
			"' and tablename='" + tableName.split("\\.")[1] + "'";
	}
	
	// SQL statement to grant select of table to user
	//public static String sqlGrantSelect = "GRANT RETRIEVE/SELECT ON '" +
	public static String sqlGrantSelect(String tableName, String userName){ 
		return "GRANT SELECT ON '" + tableName + "' to '" + userName + "';";
	}
	
	// SQL statement to revoke select of table to user
	//public static String sqlRevokeSelect = "REVOKE RETRIEVE/SELECT ON '" +
	public static String sqlRevokeSelect(String tableName, String userName) { 
		return "REVOKE SELECT ON '" + tableName + "' to '" + userName + "';";
	}
	
	// SQL statement to grant insert of table to user
	public static String sqlGrantInsert(String tableName, String userName) { 
		return "GRANT INSERT ON '" + tableName + "' to '" + userName + "';";
	}
	
	//SQL statement to revoke insert of table to user
	public static String sqlRevokeInsert(String tableName, String userName) { 
		return "REVOKE INSERT ON '" + tableName + "' to '" + userName + "';";
	}
	
	// SQL statement to export metaDBs
	public static String sqlQueryMetaDB(TSETInfoTables table){ 
		return "select * from " + table.getName() + ";";
	}
	
	// SQL statement to import metaDBs
	public static String sqlInsertMetaDB(TSETInfoTables table, String cols){ 
		return "insert into " + table.getName() + " values(" + cols + ")";
	}
	
	// SQL statement to export Physical Config
	public static String sqlQueryMonitorPhysicalConfig() { 
		return "SELECT t2.* " + "FROM TABLE (MonitorPhysicalConfig()) AS t2;";
	}
	
	// SQL statement to export virtual Config
	public static String sqlQueryVirtualPhysicalConfig() { 
		return "SELECT t2.* " + "FROM TABLE (MonitorVirtualConfig()) AS t2;";
	}
	
}
