package com.teradata.tset2.pgsql.dao;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.pojo.Column;
import com.teradata.qaf.tset.pojo.MetaDB;
import com.teradata.qaf.tset.pojo.Table;
import com.teradata.qaf.tset.utils.TSETCSVReader;

public class MetaDBDAO extends BaseDAO {
	
	private final Logger logger = Logger.getLogger(MetaDBDAO.class);
	private int System_id;
	
	public String generateSQL(Table table) {
		String sql = "";
		String cols = "?";
		for(int i=0; i<table.getColumnList().size(); i++) {
			cols += ", ?";
		}
		sql = CommonConfig.sqlInsertMetaDB(
				table.getName().split("\\.")[1], cols);
					
		return sql;
	}
	
	/**
	 * Insert metaDBs' tables into Postgres by CSV files
	 * @param metaDB
	 * @throws SQLException
	 */
	public void insert(MetaDB metaDB) throws SQLException {
		logger.info("insert into Postgres via CSV files -- metaDB: " + 
				metaDB.getName());
		for(Table table : metaDB.getTableList()) {
			TSETCSVReader csvReader = new TSETCSVReader();
			List<String[]> recordList = csvReader.readCSV(CommonConfig.path() + 
					metaDB.getName() + "/" + table.getName() + ".csv");
			
			// generate SQL statements
			String sql = this.generateSQL(table);
			try {
				logger.info("Postgres -- table: " + table.getName());
				this.executeBatchRequest(super.conn, sql, table, recordList);
			} catch (SQLException e) {
				
				e.printStackTrace();
				logger.error(e.getMessage());
				logger.error("ERROR while exporting metaDBs to PostgreSQL, " +
						"ROLLBACK automatically " +
						"and handle the exception outside.");
				throw new SQLException();
				
			}
		}
	}
	
	/**
	 * Insert metaDBs' tables into Postgres by ResultSet
	 * @param metaDB
	 * @param tdConn : connection to TD
	 * @throws SQLException
	 */
	public void insertByRs(MetaDB metaDB, Connection tdConn) 
			throws SQLException {
		logger.info("insert into Postgres -- metaDB: " + metaDB.getName());
		for(Table table : metaDB.getTableList()) {
			String s = CommonConfig.sqlQueryMetaDB(table);
			PreparedStatement ps = tdConn.prepareStatement(s);
			ResultSet rs = ps.executeQuery();
			
			List<String[]> recordList = genMatrixFromRs(rs, table);
			
			if(rs!=null && !rs.isClosed()) rs.close();
			if(ps!=null && !ps.isClosed()) ps.close();
			
			// generate SQL statements
			String sql = this.generateSQL(table);
			try {
				logger.info("Postgres -- table: " + table.getName());
				this.executeBatchRequest(super.conn, sql, table, recordList);
			} catch (SQLException e) {
				
				e.printStackTrace();
				logger.error(e.getMessage());
				logger.error("ERROR while exporting metaDBs to PostgreSQL, " +
						"ROLLBACK automatically " +
						"and handle the exception outside.");
				throw new SQLException();
				
			}
		}
	}

	/**
	 * Generate String Arrays from ResultSet
	 * @param rs
	 * @param table : table schema
	 * @return 2-D Arrays
	 * @throws SQLException 
	 */
	private List<String[]> genMatrixFromRs(ResultSet rs, Table table) 
			throws SQLException {
		List<String[]> recordList = new ArrayList<String[]>();
		List<Column> cols = table.getColumnList();
		while(rs.next()) {
			String []ss = new String[cols.size()];
			for(int i=0; i<cols.size(); i++) {
				if(rs.getObject(i+1) != null)
					ss[i] = rs.getObject(i+1).toString();
			}
			recordList.add(ss);
		}
		return recordList;
	}

	private void executeBatchRequest(
			Connection con, 
			String sInsert, 
			Table table, 
			List<String[]> recordList) throws SQLException
	{
		PreparedStatement pstmt;
		logger.info(" Preparing this SQL statement for execution:\n " + sInsert);

        // Creating a prepared statement object from an active connection
        pstmt = con.prepareStatement(sInsert);
        logger.info("Prepared statement object created. ");

        try {
            // Set parameter values indicated by ? (dynamic update)
            for (int nRecordCnt = 0; 
            		nRecordCnt < recordList.size(); 
            		nRecordCnt++) {
            	// skip the table header row
            	if (nRecordCnt == 0) continue;
            	pstmt.setInt(1, this.getSystem_id());
                for (int nItemCnt = 0; 
                		nItemCnt < recordList.get(nRecordCnt).length && 
                		nItemCnt < table.getColumnList().size(); 
                		nItemCnt++) {
                	
                	if (recordList.get(nRecordCnt)[nItemCnt] == null) {
                		pstmt.setObject(nItemCnt + 2, null);
                		continue;
                	}
                	
                    // setXXX according to the column type
                	Column column = table.getColumnList().get(nItemCnt);
//                	logger.info(column.getType() + " : " + 
//                			recordList.get(nRecordCnt)[nItemCnt]);
                	if(column.getType().equalsIgnoreCase("INTEGER") || 
                			column.getType().equalsIgnoreCase("SMALLINT") || 
                			column.getType().equalsIgnoreCase("BYTEINT")) {
                		pstmt.setInt(nItemCnt + 2, Integer.parseInt(
                				recordList.get(nRecordCnt)[nItemCnt]));
                	} else if (column.getType().equalsIgnoreCase("FLOAT")) {
                		// empty string
                		if (recordList.get(nRecordCnt)[nItemCnt].equals("")) {
                			pstmt.setFloat(nItemCnt + 2, 0);
                		} else {
                			pstmt.setFloat(nItemCnt + 2, Float.parseFloat(
                					recordList.get(nRecordCnt)[nItemCnt]));
                		}
                	} else {
                		pstmt.setString(nItemCnt + 2, 
                				recordList.get(nRecordCnt)[nItemCnt]);
                	}
                    //logger.info(column.getName());
                }
                pstmt.addBatch();
            }

            try {
                // The following code will perform an INSERT on the table.
                // The batch is empty
                if(recordList.size() <= 1 ) {
                	logger.info("Empty table. No Record in table: " + table.getName());
                	return;
                }
                // Submit a batch request, returning update counts
                int[] updateCount = pstmt.executeBatch();
                logger.info("updateCount: " + updateCount.length + 
                		" on Table: " + table.getName());
            }
            catch (BatchUpdateException ex) {
                logger.info(" Exception thrown " + 
                		ex.getErrorCode() + ":" + ex.getMessage() + "\n");
                logger.warn(ex.getMessage());
                ex.printStackTrace();
            }
        }
        finally {
            // Close the statement
            pstmt.close();
            logger.info("PreparedStatement object closed.");
        }
    }

	public int getSystem_id() {
		return System_id;
	}

	public void setSystem_id(int system_id) {
		System_id = system_id;
	}
}
