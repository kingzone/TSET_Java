package com.teradata.qaf.tset.exporter;

import java.sql.Connection;
import java.util.List;

import com.teradata.qaf.tset.common.DBConn;
import com.teradata.qaf.tset.common.ITransferable;
import com.teradata.qaf.tset.common.impl.DDLTransfer;
import com.teradata.qaf.tset.common.impl.MonitorConfigTransfer;
import com.teradata.qaf.tset.common.impl.RecordTransfer;
import com.teradata.qaf.tset.pojo.DBConfig;
import com.teradata.qaf.tset.pojo.MetaDB;
import com.teradata.qaf.tset.pojo.TSETInfoTables;
import com.teradata.qaf.tset.utils.DBConfigReader;
import com.teradata.qaf.tset.utils.XMLReader;

public class Exporter {

	//private List<ITransferable> transferableList;
	private TSETInfoTables tsetInfoTables;
	//private List<TSETInfoTables> tsetInfoTablesList;
	
	// initialize 
	public void initialize(String ConfigFileName, String outputFileName) {
		
		XMLReader xmlReader = new XMLReader(ConfigFileName);
		tsetInfoTables = xmlReader.parseXml();
		//tsetInfoTablesList = xmlReader.parseXml();
	}
	
	// 
	public List<ITransferable> getExportTransferable() {
		return null;
	}
	
	// export all
	public void doTDExportAll() {
		List<DBConfig> dbConfigList = DBConfigReader.initDBConfig("DBConfig.xml");
		for (DBConfig dbConfig : dbConfigList) {
			this.doTDExport(dbConfig);
		}
	}
	
	// export DDL, CostProfiles, CostParameters, RAS and physical/virtual config, in metaDB level
	//public void doTDExport() {
	public void doTDExport(DBConfig dbConfig) {
		
		//Connection conn = DBConn.getConnection();
		Connection conn = DBConn.getConnection(dbConfig);
		
		// check Authority
		ExpAuthority expAu = new ExpAuthority(conn, tsetInfoTables, DBConn.getDatabase(), DBConn.getUsername());
		expAu.check();
		expAu.grant();
		
		// export DDL
		DDLTransfer ddlTransfer = new DDLTransfer(conn);
		ddlTransfer.doExport();
		
		// export CostProfiles, CostParameters, RAS
		for(MetaDB metaDB : tsetInfoTables.getMetaDBList()) {
			// tackle tables of each metaDB
			
			RecordTransfer recordTransfer = new RecordTransfer(metaDB, conn);
			recordTransfer.doExport();
			
		}
		
		//export physical/virtual config
		MonitorConfigTransfer monitorConfigTransfer = new MonitorConfigTransfer(conn); 
		monitorConfigTransfer.doExport();
		
		expAu.revoke();
		
		DBConn.closeConnection(conn);
		
		// zip the TSETInfoTables directory
		
		
	}
	
	public void exportDDL() {
		
	}
	
	public void exportCostProfiles() {
		
	}
	
	public void exportCostParameters() {
		
	}
	
	public void exportRAS() {
		
	}
	
}
