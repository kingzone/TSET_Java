package com.teradata.qaf.tset.exporter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.common.DBConn;
import com.teradata.qaf.tset.common.Transferable;
import com.teradata.qaf.tset.common.impl.DDLTransfer;
import com.teradata.qaf.tset.common.impl.MonitorConfigTransfer;
import com.teradata.qaf.tset.common.impl.RecordTransfer;
import com.teradata.qaf.tset.pojo.DBConfig;
import com.teradata.qaf.tset.pojo.MetaDB;
import com.teradata.qaf.tset.pojo.TSETInfoTables;
import com.teradata.qaf.tset.utils.DBConfigReader;
import com.teradata.qaf.tset.utils.XMLReader;
import com.teradata.tset2.pgsql.dao.MetaDBDAO;

public class Exporter {

	private final Logger logger = Logger.getLogger(Exporter.class.getName());
	
	private TSETInfoTables tsetInfoTables;
	
	// true: export Physical and Virtual Config
	private boolean exportPVConfig = true;
	
	// F: insert into Postgres via CSV files
	private boolean isF = false;
	
	//private int System_id;
	
	// initialize 
	public void initialize(String ConfigFileName) {
		
		XMLReader xmlReader = new XMLReader(ConfigFileName);
		tsetInfoTables = xmlReader.parseXml();
	}
	
	// 
	public List<Transferable> getExportTransferable() {
		return null;
	}
	
	// export all
	public void doTDExportAll() {
		List<DBConfig> dbConfigList = DBConfigReader.initDBConfig(CommonConfig.DBConfig());
		for (DBConfig dbConfig : dbConfigList) {
			this.doTDExport(dbConfig);
		}
	}
	
	// export DDL, CostProfiles, CostParameters, RAS and physical/virtual config
	//public void doTDExport() {
	public void doTDExport(DBConfig dbConfig) {
		
		Connection conn = DBConn.getConnection(dbConfig);
		// return if conn is null
		if(conn == null) {
			logger.error("Get connection Failed, skip this database.");
			return;
		}
		
		// check Authority
		ExpAuthorityImpl expAu = new ExpAuthorityImpl(conn, tsetInfoTables, 
				DBConn.getDatabase(), DBConn.getUsername());
		expAu.check(this.isExportPVConfig());
		expAu.grant();
		
		// export DDL
		DDLTransfer ddlTransfer = new DDLTransfer(conn);
		ddlTransfer.setSystem_id(dbConfig.getSystem_id());
		try {
			ddlTransfer.doExport();
		} catch (Exception e) {
			try {
				if(conn != null && !conn.isClosed()) conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			logger.error("ERROR while exporting DDLs, ROLLBACK automatically " +
					"and EXIT the Application.");
			ExpRollBackImpl expRollBack = new ExpRollBackImpl(expAu);
			expRollBack.doRollBack();
			System.exit(-1);
		}
		
		// export CostProfiles, CostParameters, RAS
		try {
			for(MetaDB metaDB : tsetInfoTables.getMetaDBList()) {
				// tackle tables of each metaDB
				
				RecordTransfer recordTransfer = new RecordTransfer(metaDB, conn);
				recordTransfer.doExport();
				
				MetaDBDAO mdd = new MetaDBDAO();
				mdd.setSystem_id(dbConfig.getSystem_id());
				if(this.isF()) mdd.insert(metaDB);
				else mdd.insertByRs(metaDB, conn);
				mdd.closeConn();
			}
		} catch (Exception e) {
			try {
				if(conn != null && !conn.isClosed()) conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			logger.error("ERROR while exporting metaDBs, ROLLBACK automatically " +
					"and EXIT the Application.");
			ExpRollBackImpl expRollBack = new ExpRollBackImpl(expAu);
			expRollBack.doRollBack();
			System.exit(-1);
		}
		
		//export physical/virtual config
		if(this.isExportPVConfig()) {
			MonitorConfigTransfer monitorConfigTransfer = 
					new MonitorConfigTransfer(conn); 
			monitorConfigTransfer.setSystem_id(dbConfig.getSystem_id());
			try {
				monitorConfigTransfer.doExport();
			} catch (Exception e) {
				
				try {
					if(conn != null && !conn.isClosed()) conn.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
				logger.error("ERROR while exporting MonitorXXXConfig, " +
						"ROLLBACK automatically and EXIT the Application.");
				ExpRollBackImpl expRollBack = new ExpRollBackImpl(expAu);
				expRollBack.doRollBack();
				System.exit(-1);
			}
		}
		
		expAu.revoke();
		
		DBConn.closeConnection(conn);
		
		// zip the TSETInfoTables directory
		
		
	}

	public boolean isExportPVConfig() {
		return exportPVConfig;
	}

	public void setExportPVConfig(boolean exportPVConfig) {
		this.exportPVConfig = exportPVConfig;
	}

	public boolean isF() {
		return isF;
	}

	public void setF(boolean isF) {
		this.isF = isF;
	}

}
