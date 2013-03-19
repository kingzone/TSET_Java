package com.teradata.qaf.tset.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

public abstract class BaseReader {

	protected String fileName;
	private BufferedReader br;
	//private static Logger logger = Logger.getLogger(BaseReader.class.getName());
	protected static Logger logger = Logger.getLogger(BaseReader.class.getName());
	
	public BaseReader() {
		this.fileName = "";
	}
	
	public BaseReader(String fileName) {
		this.fileName = fileName;
	}
	
	public BufferedReader getReader() {
		File file = new File(fileName);
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			logger.error("BufferedReader br initialize error." + e.getMessage());
		}
		
		return br;
	}
	
	public void closeReader() {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("BufferedReader br close error." + e.getMessage());
		}
	}
	
}
