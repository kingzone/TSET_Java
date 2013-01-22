package com.teradata.qaf.tset.run;

import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import com.teradata.qaf.tset.exporter.Exporter;

public class ExporterRun {

	private static final Logger logger = Logger.getLogger(ExporterRun.class.getName());
	
	public static void main(String []args) {
		long starttime = System.currentTimeMillis();
		Exporter exporter = new Exporter();
		exporter.initialize("ConfFile_schema.xml", "output.csv");
		//exporter.doTDExport();
		exporter.doTDExportAll();
		
		logger.info("Export success.");
		long endtime = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		logger.info("Start Time: " + sdf.format(starttime));
		logger.info("End Time: " + sdf.format(endtime));
		logger.info("Time Elapsed: " + (endtime-starttime) + " ms.");
	}
	
}
