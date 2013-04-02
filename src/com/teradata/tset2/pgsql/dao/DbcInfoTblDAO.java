package com.teradata.tset2.pgsql.dao;

import org.apache.log4j.Logger;

import com.teradata.tset2.pgsql.pojo.DbcInfoTbl;

public class DbcInfoTblDAO extends BaseDAO {
private final Logger logger = Logger.getLogger(DDL_KVDAO.class);
	
	public DbcInfoTblDAO() {
		super();
	}
	
	public void insert(DbcInfoTbl dbcInfoTbl) {
		String sql = "insert into dbcinfotbl values('" + 
				dbcInfoTbl.getInfoKey() + "', '" +
				dbcInfoTbl.getInfoData() + "')";
		logger.info(sql);
		super.execSQL(sql);
	}
	
}
