package com.teradata.qaf.tset.common;

public interface Authority {

	void check();
	void grant();
	void revoke();
	
}
