package com.teradata.qaf.tset.exporter;

import java.io.File;

import com.teradata.qaf.tset.common.Authority;
import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.common.RollBack;

public class ExpRollBackImpl implements RollBack {

	private Authority au;
	
	public ExpRollBackImpl(Authority au) {
		this.au = au;
	}
	
	@Override
	public void doRollBack() {
		// delete the local files, revoke authority if necessary
		File dir = new File(CommonConfig.path());
		if(dir.exists()) {
			this.delDir(dir);
		}
		
		this.au.revoke();
	}

	// delete Dir recursively
	public boolean delDir(File dir) {
		if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = delDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
	}
	
}
