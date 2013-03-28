package com.teradata.qaf.tset.utils;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.teradata.qaf.tset.pojo.DBConfig;

public class DBConfigReader {
	
	static Logger logger = Logger.getLogger(DBConfigReader.class.getName());
	
	//private static List<DBConfig> dbConfigList = new ArrayList<DBConfig>();
	
	public static List<DBConfig> initDBConfig(String fileName) {
		List<DBConfig> dbConfigList = new ArrayList<DBConfig>();
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance(); 
			DocumentBuilder db = dbf.newDocumentBuilder(); 
			Document document = db.parse(fileName); 
			NodeList DBConfigNodeList = document.getChildNodes(); 
			Node DBConfigNode = DBConfigNodeList.item(0);
			NodeList DBNodeList = DBConfigNode.getChildNodes();
			for(int i=0; i<DBNodeList.getLength(); i++)
			{
				DBConfig dbConfig = new DBConfig();
				Node DBNode = DBNodeList.item(i);
				if (!DBNode.hasChildNodes()) continue;
				NodeList DBElementNodeList = DBNode.getChildNodes();
				for(int m = 0; m < DBElementNodeList.getLength(); m++) {
					//Node columnContent = columnContentList.item(m);
					Node DBElementNode = DBElementNodeList.item(m);
					// The CRLF character will be treated as a node, so using IF.
					if (DBElementNode.hasChildNodes()) {
						
						if (DBElementNode.getNodeName().equals("URL")) {
							//url = DBElementNode.getTextContent();
							dbConfig.setUrl(DBElementNode.getTextContent());
						} else if (DBElementNode.getNodeName().
								equalsIgnoreCase("DATABASE")) {
							//database = DBElementNode.getTextContent();
							dbConfig.setDatabase(DBElementNode.getTextContent());
						} else if (DBElementNode.getNodeName().
								equalsIgnoreCase("CHARSET")) {
							//charset = DBElementNode.getTextContent();
							dbConfig.setCharset(DBElementNode.getTextContent());
						} else if (DBElementNode.getNodeName().
								equalsIgnoreCase("TMODE")) {
							//tmode = DBElementNode.getTextContent();
							dbConfig.setTmode(DBElementNode.getTextContent());
						} else if (DBElementNode.getNodeName().
								equalsIgnoreCase("USERNAME")) {
							//username = DBElementNode.getTextContent();
							dbConfig.setUsername(DBElementNode.getTextContent());
						} else if (DBElementNode.getNodeName().
								equalsIgnoreCase("PASSWORD")) {
							//password = DBElementNode.getTextContent();
							dbConfig.setPassword(DBElementNode.getTextContent());
						} else if (DBElementNode.getNodeName().
								equalsIgnoreCase("System_id")) {
							dbConfig.setSystem_id(Integer.parseInt(
									DBElementNode.getTextContent()));
						} else {
							logger.error("unknown DB config info.");
						}
					}
				}
				dbConfigList.add(dbConfig);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		return dbConfigList;
	}
}
