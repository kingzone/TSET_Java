package com.teradata.qaf.tset.exporter;

import com.teradata.qaf.tset.common.RollBack;

public class ExpRollBackImpl implements RollBack {

	@Override
	public void doRollBack() {
		// delete the local files, revoke authority if necessary

	}

}
