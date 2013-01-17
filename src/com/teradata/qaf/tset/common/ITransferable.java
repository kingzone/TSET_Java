package com.teradata.qaf.tset.common;

public interface ITransferable {

	public static final int EXPORT = 0;
	public static final int IMPORT = 1;
	
	public String getGeneratedSQL();
	public void doExport();
	public void doImport();
	
}
