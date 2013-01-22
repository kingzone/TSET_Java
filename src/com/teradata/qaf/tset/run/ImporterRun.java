package com.teradata.qaf.tset.run;

import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.importer.Importer;

public class ImporterRun {

	private static final Logger logger = Logger.getLogger(ImporterRun.class.getName());
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		long starttime = System.currentTimeMillis();
		Importer importer = new Importer();
		//importer.initialize("ConfFile_schema.xml", "input.csv");
		importer.initialize("ConfFile_schema_13.10.xml", "input.csv");
		importer.doTDImport();
		
		logger.info("Import success.");
		long endtime = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		logger.info("Start Time: " + sdf.format(starttime));
		logger.info("End Time: " + sdf.format(endtime));
		logger.info("Time Elapsed: " + (endtime-starttime) + " ms.");
	}

}
