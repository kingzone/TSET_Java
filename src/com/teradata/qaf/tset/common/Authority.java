package com.teradata.qaf.tset.common;

public interface Authority {

	/**
	 * Check if the user have proper authority.
	 * 
	 * 1. EF(EXECUTE FUNCTION) on SYSLIB.MonitorPhysicalConfig
	 * 2. OTHERS : R(RETRIEVE/SELECT) for Export, I(INSERT) for Import
	 * 
	 */
	void check();
	/**
	 * Try to grant needed authority to user
	 */
	void grant();
	/**
	 * Revoke the granted authority.
	 */
	void revoke();
	
}
