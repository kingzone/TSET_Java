package com.teradata.tset2.pgsql;

import java.util.Date;

import com.teradata.qaf.tset.pojo.TSET2_History;

public interface TSET2_HistoryDAO {

	/**
	 * Get LastAlterTimestamp from table TSET2_History
	 * @param systemID
	 * @param databaseID
	 * @param tableID
	 * @return 
	 */
	public Date getLastAlterTimestampByCriteria(
			String systemID, 
			String databaseID, 
			String tableID);
	/**
	 * 
	 * @param th
	 * @return
	 */
	public Date getLastAlterTimestampByPOJO(TSET2_History th);
	
	/**
	 * Check if the LastAlterTimestamp in the PRODUCTION DB is larger than 
	 * the corresponding one in the PostgreSQL DB (namely, Emulation DB)
	 * 
	 * @param th : mapping to the PostgreSQL record
	 * @param lastAlterTimestamp : Timestamp from the PRODUCTION DB
	 * @return true if changed
	 */
	public boolean isChanged(TSET2_History th, Date lastAlterTimestamp);
	
	/**
	 * 
	 * @param systemID : systemID of PostgreSQL DB (Emulation DB)
	 * @param databaseID : databaseID of PostgreSQL DB
	 * @param tableID : tableID of PostgreSQL DB
	 * @param lastAlterTimestamp : timestamp of PRODUCTION DB
	 * @return
	 */
	public boolean isChanged(
			String systemID, 
			String databaseID, 
			String tableID, 
			Date lastAlterTimestamp);
	
}
