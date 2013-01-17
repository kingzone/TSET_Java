package com.teradata.qaf.tset.exporter;

import com.teradata.qaf.tset.common.ISQLStatement;

public class ExpSQLStatement implements ISQLStatement {

	private String sql;
	private String databaseName;
	
	public void generateSQLStatement() {

	}

	public void executeSQLStatement() {

	}
	
	public String expDDL() {
		sql = "select tablename, tablekind from dbc.tables where databasename='" + databaseName + "' order by createtimestamp";
		return sql;
	}
	
	public void expTSETInfoTables() {
		
	}

}
