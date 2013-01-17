package com.teradata.qaf.tset.common;

public interface IAuthority {

	void check();
	void grant();
	void revoke();
	
}
