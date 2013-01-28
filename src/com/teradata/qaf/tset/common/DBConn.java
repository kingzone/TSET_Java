/**
 * @author zj255003
 * 
 */

package com.teradata.qaf.tset.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.teradata.qaf.tset.pojo.DBConfig;

public class DBConn {

	static Logger logger = Logger.getLogger(DBConn.class.getName());
	
	private static Connection conn = null;
//	private static String url = "jdbc:teradata://153.64.80.9/database=hadoop,charset=ASCII,TMODE=TERA";
//	private static String url = "jdbc:teradata://153.64.28.192/database=hadoop,charset=ASCII,TMODE=TERA";
	private static String url = "jdbc:teradata://153.64.80.9/";
	private static String database = "";// if unspecified, export all database
	private static String charset = "ASCII";
	private static String tmode = "TERA";
	private static String username = "dbc";
	private static String password = "dbc";
	
	// EXP
	public static void initDBConfig(DBConfig dbConfig) {
		url = dbConfig.getUrl();
		database = dbConfig.getDatabase();
		charset = dbConfig.getCharset();
		tmode = dbConfig.getTmode();
		username = dbConfig.getUsername();
		password = dbConfig.getPassword();
	}
	
	// IMP
	public static void initDBConfig(String fileName) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance(); 
			DocumentBuilder db = dbf.newDocumentBuilder(); 
			Document document = db.parse(fileName); 
			NodeList DBConfigNodeList = document.getChildNodes(); 
			Node DBConfigNode = DBConfigNodeList.item(0);
			NodeList DBNodeList = DBConfigNode.getChildNodes();
			for(int i=0; i<DBNodeList.getLength(); i++)
			{
				Node DBNode = DBNodeList.item(i);
				NodeList DBElementNodeList = DBNode.getChildNodes();
				for(int m = 0; m < DBElementNodeList.getLength(); m++) {
					//Node columnContent = columnContentList.item(m);
					Node DBElementNode = DBElementNodeList.item(m);
					// The CRLF character will be treated as a node, so using IF.
					if (DBElementNode.hasChildNodes()) {
						
						System.out.println(DBElementNode.getNodeName() + ":" + DBElementNode.getTextContent());
						//System.out.println(m);
						if (DBElementNode.getNodeName().equals("URL")) {
							url = DBElementNode.getTextContent();
						} else if (DBElementNode.getNodeName().equalsIgnoreCase("DATABASE")) {
							database = DBElementNode.getTextContent();
						} else if (DBElementNode.getNodeName().equalsIgnoreCase("CHARSET")) {
							charset = DBElementNode.getTextContent();
						} else if (DBElementNode.getNodeName().equalsIgnoreCase("TMODE")) {
							tmode = DBElementNode.getTextContent();
						} else if (DBElementNode.getNodeName().equalsIgnoreCase("USERNAME")) {
							username = DBElementNode.getTextContent();
						} else if (DBElementNode.getNodeName().equalsIgnoreCase("PASSWORD")) {
							password = DBElementNode.getTextContent();
						} else {
							logger.error("unknown DB config info.");
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}
	
	
	//public static Connection getConnection() {
	// EXP
	public static Connection getConnection(DBConfig dbConfig) {
		//Connection conn = null;
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		//
		//initDBConfig("DBConfig.xml");
		initDBConfig(dbConfig);
		
//		String connStr = url + "database=\"" + database + "\",charset=" + charset + ",tmode=" + tmode;
		String connStr = CommonConfig.connectionString(url, database, charset, tmode);
		logger.info(connStr);
		try {
			conn = DriverManager.getConnection(connStr, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		if (conn == null) {
//			System.out.println("ERROR: Get connection to " + url + " FAILED!");
			logger.info("ERROR: Get connection to " + url + " FAILED!");
		} else {
			//System.out.println("INFO: Get connection to " + url + " SUCCESS!");
			logger.info("INFO: Get connection to " + url + " SUCCESS!");
		}
		return conn;
	}
	
	// IMP
	public static Connection getConnection() {
		//Connection conn = null;
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		//
		initDBConfig("DBConfig.xml");
		
//		String connStr = url + "database=\"" + database + "\",charset=" + charset + ",tmode=" + tmode;
		String connStr = CommonConfig.connectionString(url, database, charset, tmode);
		logger.info(connStr);
		try {
			conn = DriverManager.getConnection(connStr, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		if (conn == null) {
//				System.out.println("ERROR: Get connection to " + url + " FAILED!");
			logger.info("ERROR: Get connection to " + url + " FAILED!");
		} else {
			//System.out.println("INFO: Get connection to " + url + " SUCCESS!");
			logger.info("INFO: Get connection to " + url + " SUCCESS!");
		}
		return conn;
	}
	
	public static void closeConnection() {
		if (conn != null) {
			try {
				conn.close();
				logger.info("DB connection close success.");
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
	}
	
	public static void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
				logger.info("DB connection close success.");
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
	}

	public static String getDatabase() {
		return database;
	}

	public static void setDatabase(String database) {
		DBConn.database = database;
	}

	public static String getUsername() {
		return username;
	}

	public static void setUsername(String username) {
		DBConn.username = username;
	}

	public static String getUrl() {
		return url;
	}

	public static void setUrl(String url) {
		DBConn.url = url;
	}
	
}
