package com.teradata.qaf.tset.run;

import java.util.Iterator;

import com.teradata.qaf.tset.common.CommonConfig;
import com.teradata.qaf.tset.pojo.Column;
import com.teradata.qaf.tset.pojo.MetaDB;
import com.teradata.qaf.tset.pojo.TSETInfoTables;
import com.teradata.qaf.tset.pojo.Table;
import com.teradata.qaf.tset.utils.XMLReader;

public class XmlReaderRun {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		XMLReader xmlReader = new XMLReader("ConfFile_schema.xml");
		XMLReader xmlReader = new XMLReader(CommonConfig.ConfFile_schema("14.00"));
		xmlReader.parseXml();
		
		genMetaDBDDL();
		genFormatMetaDBDDL();
	}
	
	public static void genMetaDBDDL() {
		XMLReader xmlReader = new XMLReader(CommonConfig.ConfFile_schema("13.10"));
		TSETInfoTables tit = xmlReader.parseXml();
		Iterator<MetaDB> it1 = tit.getMetaDBList().iterator();
		while(it1.hasNext()) {
			MetaDB metaDB = it1.next();
			Iterator<Table> it2 = metaDB.getTableList().iterator();
			while(it2.hasNext()) {
				Table table = it2.next();
				String tableName = table.getName();
				String[] s = tableName.split("\\.");
				//String sql = "CREATE TABLE " + tableName + "(";
				String sql = "CREATE TABLE " + s[1] + "(";
				sql += "System_id int,";
				Iterator<Column> it3 = table.getColumnList().iterator();
				while(it3.hasNext()) {
					Column col = it3.next();
					sql += col.getName() + " " + col.getType() + ",";
				}
				sql = sql.substring(0, sql.length() - 1);
				sql += ");";
				System.out.println(sql);
			}
			
		}
	}
	
	public static void genFormatMetaDBDDL() {
		XMLReader xmlReader = new XMLReader(CommonConfig.ConfFile_schema("13.10"));
		TSETInfoTables tit = xmlReader.parseXml();
		Iterator<MetaDB> it1 = tit.getMetaDBList().iterator();
		while(it1.hasNext()) {
			MetaDB metaDB = it1.next();
			Iterator<Table> it2 = metaDB.getTableList().iterator();
			while(it2.hasNext()) {
				Table table = it2.next();
				String tableName = table.getName();
				String[] s = tableName.split("\\.");
				String comment = "/*" + s[1] + "*/";
				System.out.println(comment);
				//String sql = "CREATE TABLE " + tableName + "(";
				String sql = "\"CREATE TABLE " + s[1] + "(\" +\n";
				sql += "\"	System_id int,\" + \n";
				Iterator<Column> it3 = table.getColumnList().iterator();
				while(it3.hasNext()) {
					Column col = it3.next();
					sql += "\"	" + col.getName() + " " + col.getType() + ",\" + \n";
				}
				sql = sql.substring(0, sql.length() - 6);
				sql += ");\",";
				//System.out.println(sql);
				//System.out.println();
				
				sql = sql.replace("BYTEINT", "SMALLINT");
				sql = sql.replace("VARBYTE", "VARCHAR");
				System.out.println(sql);
				System.out.println();
			}
			
		}
	}

}
