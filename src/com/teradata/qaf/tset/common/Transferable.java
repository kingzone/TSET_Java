package com.teradata.qaf.tset.common;

public interface Transferable {

	public static final int EXPORT = 0;
	public static final int IMPORT = 1;
	
	public String getGeneratedSQL();
	public void doExport() throws Exception;
	public void doImport();
	
}
