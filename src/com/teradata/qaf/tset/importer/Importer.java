package com.teradata.qaf.tset.importer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.DBConn;
import com.teradata.qaf.tset.common.ITransferable;
import com.teradata.qaf.tset.common.impl.DDLTransfer;
import com.teradata.qaf.tset.common.impl.RecordTransfer;
import com.teradata.qaf.tset.pojo.MetaDB;
import com.teradata.qaf.tset.pojo.TSETInfoTables;
import com.teradata.qaf.tset.utils.XMLReader;

public class Importer {
private TSETInfoTables tsetInfoTables;
	
	private static final Logger logger = Logger.getLogger(Importer.class.getName());

	// initialize 
	public void initialize(String ConfigFileName, String inputFileName) {
		
		XMLReader xmlReader = new XMLReader(ConfigFileName);
		tsetInfoTables = xmlReader.parseXml();
		
	}
	
	// 
	public List<ITransferable> getImportTransferable() {
		return null;
	}
	
	// export DDL, CostProfiles, CostParameters, RAS and physical/virtual config, in metaDB level
	public void doTDImport() {
		Connection conn = DBConn.getConnection();
		
		// UnZip the TSETInfoTables directory
		
		// check Authority
		ImpAuthority impAu = new ImpAuthority(conn, tsetInfoTables, DBConn.getDatabase(), DBConn.getUsername());
		impAu.check();
		impAu.grant();
		
		// BT
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// import DDL
		DDLTransfer ddlTransfer = new DDLTransfer(conn);
		ddlTransfer.doImport();
		
		// import CostProfiles, CostParameters, RAS
		for(MetaDB metaDB : tsetInfoTables.getMetaDBList()) {
			// tackle tables of each metaDB
			logger.info("Now tackling MetaDB: " + metaDB.getName());
			//if(!metaDB.getName().equalsIgnoreCase("CostProfiles")) continue;
			RecordTransfer recordTransfer = new RecordTransfer(metaDB, conn);
			recordTransfer.doImport();
			
		}
		
		// import physical/virtual config
		
		// ET
		try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		impAu.revoke();
		
		DBConn.closeConnection(conn);
		
	}
}
