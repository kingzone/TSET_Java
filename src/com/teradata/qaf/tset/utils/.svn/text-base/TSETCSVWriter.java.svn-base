package com.teradata.qaf.tset.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;

public class TSETCSVWriter extends BaseWriter {

	private static Logger logger = Logger.getLogger(TSETCSVWriter.class.getName());
	//private String fileName = "TSETInfoTables/TSETInfoTables.csv";
	private String fileName;
	
	public TSETCSVWriter() {
		
	}
	
	public TSETCSVWriter(String fileName) {
		this.fileName = fileName;
	}
	
	// should not be static, considering the support of multi-thread write file
	public void writeCSV(ResultSet rs) throws Exception{
		File f = super.openFile(fileName);
		CSVWriter writer = null;
		try {
			//writer = new CSVWriter(new FileWriter(fileName), '\t');
			writer = new CSVWriter(new FileWriter(f));
			writer.writeAll(rs, true);//include headers
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			logger.error("ERROR while writing exported metaDB file(s), " +
					"ROLLBACK automatically and handle the exception outside.");
			throw new IOException();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
}
