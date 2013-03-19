package com.teradata.qaf.tset.utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

public class TSETCSVReader {

	private static final Logger logger = Logger.getLogger(TSETCSVReader.class.getName());
	
	public List<String[]> readCSV(String fileName) {
		CSVReader reader = null;
		List<String[]> myEntries = null;
	    try {
	    	reader = new CSVReader(new FileReader(fileName));
			myEntries = reader.readAll();
			
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} finally {
			
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
	    return myEntries;
	}
	
}
