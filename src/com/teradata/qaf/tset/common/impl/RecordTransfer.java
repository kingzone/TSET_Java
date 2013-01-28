package com.teradata.qaf.tset.common.impl;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.common.Transferable;
import com.teradata.qaf.tset.pojo.Column;
import com.teradata.qaf.tset.pojo.MetaDB;
import com.teradata.qaf.tset.pojo.Table;
import com.teradata.qaf.tset.utils.TSETCSVReader;
import com.teradata.qaf.tset.utils.TSETCSVWriter;

public class RecordTransfer implements Transferable {
	private static Logger logger = Logger.getLogger(RecordTransfer.class.getName());
	private MetaDB metaDB;
	private Connection conn;
	
	public RecordTransfer() {
		
	}
	
	public RecordTransfer(MetaDB metaDB, Connection conn) {
		this.metaDB = metaDB;
		this.conn = conn;
	}
	
	@Override
	public String getGeneratedSQL() {
		// generate SQL of tables of the specified metaDB
		return null;
	}
	
	public String generateSQL(Table table, int flag) {
		String sql = "";
		if(flag == EXPORT) {
//			sql = "select * from " + table.getName() + ";";
			sql = CommonConfig.sqlQueryMetaDB(table);
		} else if(flag == IMPORT) {
			String cols = "?";
			for(int i=1; i<table.getColumnList().size(); i++) {
				cols += ", ?";
			}
//			sql = "insert into " + table.getName() + " values(" + cols + ")";
			sql = CommonConfig.sqlInsertMetaDB(table, cols);
					
		} else {
			logger.error("Unknown flag!");
		}
		return sql;
	}

	@Override
	public void doExport() {
		PreparedStatement ps = null;
		ResultSet rs = null;
		// 1.generate SQL; 2.execute SQL and write the results into csv files
		try {
			for(Table table : metaDB.getTableList()) {
//				String sql = "select * from " + table.getName() + ";";
				String sql = this.generateSQL(table, EXPORT);
						
				logger.info(sql);
				ps = conn.prepareStatement(sql);
				rs = ps.executeQuery();
				//TSETCSVWriter csvWriter = new TSETCSVWriter("TSETInfoTables/" + metaDB.getName() + "/" + table.getName() + ".csv");
				TSETCSVWriter csvWriter = new TSETCSVWriter(CommonConfig.path() + 
						metaDB.getName() + "/" + table.getName() + ".csv");
				csvWriter.writeCSV(rs);
				logger.info("execute sql : " + sql);
				
			}
			
			// zip the csv files to metaDB
			
			
		} catch (Exception e) {
			logger.error(e.getMessage());
			//logger.error(e.getStackTrace().toString());
			e.printStackTrace();
			try {
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		} finally {
			try {
				rs.close();
				ps.close();
				//conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
			
		}
	}

	@Override
	public void doImport() {
		logger.info("Now import metaDB: " + this.metaDB.getName());
		for(Table table : metaDB.getTableList()) {
			//if (!table.getName().equals("DBC.CostProfiles")) continue;
			// read the exported files
			TSETCSVReader csvReader = new TSETCSVReader();
			//List<String[]> recordList = csvReader.readCSV("TSETInfoTables/" + metaDB.getName() + "/" + table.getName() + ".csv");
			List<String[]> recordList = csvReader.readCSV(CommonConfig.path() + 
					metaDB.getName() + "/" + table.getName() + ".csv");
			
			// generate SQL statements
			String sql = this.generateSQL(table, IMPORT);
			try {
				logger.info("Now importing table: " + table.getName());
				this.executeBatchRequest(this.conn, sql, table, recordList);
			} catch (SQLException e) {
				
				e.printStackTrace();
				logger.error(e.getMessage());
				try {
					//conn.rollback();
					conn.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
					logger.error(e1.getMessage());
				}
				System.exit(1);
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
//	private static void executeBatchRequest(Connection con, String sInsert, String[][] aasEmps) throws SQLException
	private void executeBatchRequest(Connection con, String sInsert, Table table, List<String[]> recordList) throws SQLException
    {
        ArrayList failedParameterSets = new ArrayList();
        PreparedStatement pstmt;
        System.out.println(" Preparing this SQL statement for execution:\n " + sInsert);

        // Creating a prepared statement object from an active connection
        pstmt = con.prepareStatement(sInsert);
        System.out.println(" Prepared statement object created. \n");

        try
        {
            // Set parameter values indicated by ? (dynamic update)
            //System.out.println(" Using setString() to bind value to the parameter marker. \n" );
            for (int nRecordCnt = 0; nRecordCnt < recordList.size(); nRecordCnt++)
            {
            	// except the table header row
            	if (nRecordCnt == 0) continue;
                for (int nItemCnt = 0; nItemCnt < recordList.get(nRecordCnt).length && nItemCnt < table.getColumnList().size(); nItemCnt++) {
                    // setXXX according to the column type
                	Column column = table.getColumnList().get(nItemCnt);
                	logger.info(column.getType() + " : " + recordList.get(nRecordCnt)[nItemCnt]);
                	if(column.getType().equalsIgnoreCase("INTEGER") || column.getType().equalsIgnoreCase("SMALLINT") || column.getType().equalsIgnoreCase("BYTEINT")) {
                		pstmt.setInt(nItemCnt + 1, Integer.parseInt(recordList.get(nRecordCnt)[nItemCnt]));
                	} else if (column.getType().equalsIgnoreCase("FLOAT")) {
                		if (recordList.get(nRecordCnt)[nItemCnt].equals("")) {// empty string
                			pstmt.setFloat(nItemCnt + 1, 0);
                		} else {
                			pstmt.setFloat(nItemCnt + 1, Float.parseFloat(recordList.get(nRecordCnt)[nItemCnt]));
                		}
                	} else {
                		pstmt.setString(nItemCnt + 1, recordList.get(nRecordCnt)[nItemCnt]);
                	}
                    //logger.info((nItemCnt+1) + ": " + recordList.get(nRecordCnt)[nItemCnt]);
                }
                pstmt.addBatch();
            }

            try
            {
                // The following code will perform an INSERT on the table.
                System.out.println(" Submitting the batch request to be executed. \n");
                // The batch is empty
                if(recordList.size() <=1 ) {
                	logger.info("Empty table. No Record in table: " + table.getName());
                	return;
                }
                // Submit a batch request, returning update counts
                int[] updateCount = pstmt.executeBatch();
                logger.info("updateCount: " + updateCount.length + "on Table: " + table.getName());
            }
            catch (BatchUpdateException ex)
            {
                System.out.println(" Exception thrown "+ex.getErrorCode()+":"+ex.getMessage()+"\n");
                logger.warn(ex.getMessage());
                // If the error code is 1338, then this indicates that the new
                // functionality is available.

                // 1338: A failure occurred while executing a PreparedStatement
                // batch request. Details of the failure can be found in the
                // exception chain that is accessible with getNextException.
                if (ex.getErrorCode() == 1338)
                {
                    ArrayList resubmitParameterSets = new ArrayList();
                    // Get the array of update counts to check which parameter sets failed
                    int[] anUpdateCounts = ex.getUpdateCounts();

                    // Details of the failures are found in the exception chain
                    // Get the first SQL exception
                    SQLException se = ex.getNextException();

                    // BatchUpdateException error code 1138 indicates that each
                    // non-successful update count corresponds to a chained
                    // SQLException, in order.
                    for (int i = 0; i < anUpdateCounts.length; i++)
                    {
                        if (anUpdateCounts[i] == Statement.EXECUTE_FAILED)
                        {
                            // If the error code is negative, then we know this
                            // parameter set failed
                            if (se.getErrorCode() < 0)
                            {
                                // Since this parameter set failed lets add it
                                // as well as the error to a list of failed
                                // parameter sets
                                ArrayList failedParameterSet = new ArrayList(recordList.get(i).length + 2);
                                failedParameterSet.add(new Integer(se.getErrorCode()));
                                failedParameterSet.add(se.getMessage());

                                for (int nParam = 0; nParam < recordList.get(i).length; nParam++)
                                    failedParameterSet.add(recordList.get(i)[nParam]);
                                failedParameterSets.add(failedParameterSet);
                            }

                            // If the error code is a non negative number, then
                            // the parameter set must be resubmitted individually
                            // using PreparedStatement executeUpdate method.
                            else
                            {
                                // Add the paramter set to a list of sets that need to be resubmitted
                                Object[] resubmitParameterSet = new Object[recordList.get(i).length];
                                System.arraycopy(recordList.get(i), 0, resubmitParameterSet, 0, recordList.get(i).length);
                                resubmitParameterSets.add(resubmitParameterSet);
                            }
                            se = se.getNextException();
                        }
                    }

                    // We need to resubmit individual requests that were not executed
                    if (resubmitParameterSets.size() > 0)
                        failedParameterSets = resubmitIndividualParamSets(con, sInsert, resubmitParameterSets, failedParameterSets);

                    // Print the failed parameter sets and the exceptions
                    System.out.println(" FAILED PARAMETER SETS: \n");
                    logger.error(" FAILED PARAMETER SETS: \n");
                    Iterator sets = failedParameterSets.iterator();

                    while (sets.hasNext())
                    {
                        Object[] params = ((ArrayList) sets.next()).toArray();
                        for (int iCnt = 2; iCnt < params.length; iCnt++) {
                            System.out.print(params[iCnt]+"  ");
                            //logger.error(params[iCnt]+"  ");
                        }
                        System.out.println("\n Error "+params[0]+": "+params[1]+"\n");
                        logger.error("\n Error "+params[0]+": "+params[1]+"\n");
                    }
                }
                else
                {
                    System.out.println(" All parameter sets failed \n"
                                      +" Error "+ex.getErrorCode()+": "
                                      +ex.getMessage()+"\n");
                }
            }
        }
        finally
        {
            // Close the statement
            pstmt.close();
            System.out.println("\n PreparedStatement object closed.\n");
        }
    } // End method executeBatchRequest

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private ArrayList resubmitIndividualParamSets(Connection con,
            String sInsertStmt,
            ArrayList resubmitParameterSets,
            ArrayList failedParameterSets)
	throws SQLException
	{
		PreparedStatement pstmt;
		
		System.out.println(" Processing Individual Queries. \n");
		System.out.println(" Preparing this SQL statement for execution:\n " + sInsertStmt);
		
		// Creating a prepared statement object from an active connection
		pstmt = con.prepareStatement(sInsertStmt);
		System.out.println(" Prepared statement object created. \n");
		
		try
		{
			// Set parameter values indicated by ? (dynamic update)
			System.out.println(" Using setString() to bind value to the parameter marker. \n" );
			// Loop through each parameter set and resubmit them individually
			for (int nParSetCnt = 0; nParSetCnt < resubmitParameterSets.size(); nParSetCnt++)
			{
				Object[] paramSet = (Object[]) resubmitParameterSets.get(nParSetCnt);
				for (int nParam = 0; nParam < paramSet.length; nParam++)
				pstmt.setString(nParam + 1, (String) paramSet[nParam]);
				try
				{
					pstmt.executeUpdate();
				}
				catch (SQLException se)
				{
					// Add any failures to the list of failed parameter sets
					ArrayList failedParameterSet = new ArrayList(paramSet.length + 2);
					failedParameterSet.add(new Integer(se.getErrorCode()));
					failedParameterSet.add(se.getMessage());
					
					for (int nParam = 0; nParam < paramSet.length; nParam++)
						failedParameterSet.add(paramSet[nParam]);
					failedParameterSets.add(failedParameterSet);
				}
			}
		}
		finally
		{
			// Close the statement
			pstmt.close();
			System.out.println("\n PreparedStatement object closed.\n");
		}
		return failedParameterSets;
	}
}

