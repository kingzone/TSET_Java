package com.teradata.qaf.tset.importer;

import com.teradata.qaf.tset.common.RollBack;

public class ImpRollBackImpl implements RollBack {

	@Override
	public void doRollBack() {
		// drop tables, delete records, revoke if necessary

	}

}
