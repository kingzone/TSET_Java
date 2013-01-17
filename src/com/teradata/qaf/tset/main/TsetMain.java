package com.teradata.qaf.tset.main;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TsetMain {

	private static Logger logger = Logger.getLogger(TsetMain.class.getName());
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		//System.out.println(System.getProperty("user.dir"));
		PropertyConfigurator.configure(System.getProperty("user.dir") + "\\src\\log4j.properties");
		logger.info("log4j ok.");
		
	}

}
