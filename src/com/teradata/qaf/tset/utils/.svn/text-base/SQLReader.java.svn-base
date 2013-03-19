package com.teradata.qaf.tset.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class SQLReader {

	private static final Logger logger = Logger.getLogger(SQLReader.class.getName());
	
	public static List<String> readSQL(String fileName) {
		File f = new File(fileName);
		List<String> sqlList = new ArrayList<String>();
		BufferedReader br = null;
		String temp = "";
		String aSQL = "";
		try {
			
			br = new BufferedReader(new FileReader(f));
			while((temp=br.readLine()) != null) {
				// insert a space in the end of each line
				if(temp.trim().endsWith(";")) {
					aSQL += (temp + " ");
					sqlList.add(aSQL);
					aSQL = "";
				} else {
					aSQL += (temp + " ");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		return sqlList;
	}
	
	/**
	public static String readSQL(String fileName) {
		File f = new File(fileName);
		String sql = "";
		BufferedReader br = null;
		String temp = "";
		try {
			br = new BufferedReader(new FileReader(f));
			while((temp=br.readLine()) != null) {
				// insert a space in the end of each line
				sql += (temp + " ");
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		return sql;
	}
	*/
}
