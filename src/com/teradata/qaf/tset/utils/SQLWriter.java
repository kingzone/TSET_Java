package com.teradata.qaf.tset.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

public class SQLWriter extends BaseWriter{

	private static Logger logger = Logger.getLogger(SQLWriter.class.getName());
	private static String ddlFileName = "TSETInfoTables/ddl.sql";
	
	public static void writeSQL(List<String> sqlList) {
		//File f = new File(ddlFileName);
		File f = (new SQLWriter()).openFile(ddlFileName);
		FileWriter fw = null;
		try {
			fw = new FileWriter(f);
			Iterator<String> it = sqlList.iterator();
			while(it.hasNext()) {
				
				//fw.append(it.next() + ";\r\n");
				
				String tempSql = it.next();
				if (tempSql.endsWith(";")) {
					fw.append(it.next() + "\r\n");
				} else {
					fw.append(it.next() + ";\r\n");
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} finally {
			try {
				fw.flush();
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
			
		}
	}
	
	public static void setFileName(String fileName) {
		ddlFileName = fileName;
	}
	
}
