package com.teradata.qaf.tset.importer;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.common.Authority;
import com.teradata.qaf.tset.common.RollBack;

public class ImpRollBackImpl implements RollBack {

	private final Logger logger = Logger.getLogger(this.getClass());
	private Authority au;
	
	public ImpRollBackImpl(Authority au) {
		this.au = au;
	}
	
	@Override
	public void doRollBack() {
		// drop tables, delete records, revoke if necessary
		
		logger.info("RollBack: Drop tables success.");
		
		logger.info("RollBack: Delete metaDBs' records success.");
		
		this.au.revoke();
		logger.info("RollBack: Revoke success.");
		
		System.exit(-1);
	}

}
