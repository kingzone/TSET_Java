package com.teradata.qaf.tset.utils;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

public class BaseWriter {

	private static Logger logger = Logger.getLogger(BaseWriter.class.getName());
	
	// settle down the null directory
	public File openFile(String fileName) {
		File f = new File(fileName);
		File parent = f.getParentFile();
		if(parent!=null && !parent.exists()) {
			parent.mkdirs();
		}
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		return f;
	}
	
}
