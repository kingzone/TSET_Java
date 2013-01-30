package com.teradata.qaf.tset.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.teradata.qaf.tset.pojo.Column;
import com.teradata.qaf.tset.pojo.MetaDB;
import com.teradata.qaf.tset.pojo.TSETInfoTables;
import com.teradata.qaf.tset.pojo.Table;

public class XMLReader extends BaseReader {

	private static Logger logger = Logger.getLogger(XMLReader.class.getName());
	private Document document; 
	
	public XMLReader() {
		super();
	}
	
	public XMLReader(String fileName) {
		super(fileName);
	}
	
	// initialize the document object
	public void init() { 
		try { 
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance(); 
			DocumentBuilder builder = factory.newDocumentBuilder(); 
			this.document = builder.newDocument(); 
			logger.info("initilize the document success.");
		} catch (ParserConfigurationException e) { 
			//System.out.println(e.getMessage()); 
			logger.error(e.getMessage());
		} 
	} 
	
	// create XML file fileName
	public void createXml(String fileName) { 
		Element root = this.document.createElement("TSETInfoTables"); 
		this.document.appendChild(root); 
		Element metaDB = this.document.createElement("metaDB"); 
		Element table = this.document.createElement("table"); 
		
		Element column = this.document.createElement("column");
		Element columnName = this.document.createElement("columnName");
		columnName.appendChild(this.document.createTextNode("test")); 
		column.appendChild(columnName); 
		Element columnType = this.document.createElement("columnType"); 
		columnType.appendChild(this.document.createTextNode("INTEGER")); 
		column.appendChild(columnType); 
		
		table.appendChild(column);
		metaDB.appendChild(table);
		root.appendChild(metaDB); 
		
		TransformerFactory tf = TransformerFactory.newInstance(); 
		try { 
			Transformer transformer = tf.newTransformer(); 
			DOMSource source = new DOMSource(document); 
			transformer.setOutputProperty(OutputKeys.ENCODING, "gb2312"); 
			transformer.setOutputProperty(OutputKeys.INDENT, "yes"); 
			PrintWriter pw = new PrintWriter(new FileOutputStream(fileName)); 
			StreamResult result = new StreamResult(pw); 
			transformer.transform(source, result); 
			//System.out.println("generate XML file success!"); 
			logger.info("Generate XML file success!");
		} catch (TransformerConfigurationException e) { 
			//System.out.println(e.getMessage()); 
			logger.error(e.getMessage());
		} catch (IllegalArgumentException e) { 
			//System.out.println(e.getMessage()); 
			logger.error(e.getMessage());
		} catch (FileNotFoundException e) { 
			//System.out.println(e.getMessage());
			logger.error(e.getMessage());
		} catch (TransformerException e) { 
			//System.out.println(e.getMessage());
			logger.error(e.getMessage());
		} 
	} 
	
	public TSETInfoTables parseXml() {
		return this.parseXml(fileName);
	}
	
	// parse the XML file fileName(Description file of TD)
	public TSETInfoTables parseXml(String fileName) { 
		try { 
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance(); 
			DocumentBuilder db = dbf.newDocumentBuilder(); 
			Document document = db.parse(fileName); 
			NodeList TSETInfoTablesNodeList = document.getChildNodes(); 
			Node TSETInfoTablesNode = TSETInfoTablesNodeList.item(0); 
			//if (!columnNode.hasChildNodes()) continue;
			
			// pojo
			TSETInfoTables infoTables = new TSETInfoTables();
			List<MetaDB> metaDBList = new ArrayList<MetaDB>();
			//infoTables.setMetaDBList(metaDBList);
			
			NodeList metaDBNodeList = TSETInfoTablesNode.getChildNodes();
			for (int i = 0; i < metaDBNodeList.getLength(); i++) { 
				Node metaDBNode = metaDBNodeList.item(i);
				// tackle the CRLF character, otherwise it will be treated as a empty node.
				if (!metaDBNode.hasChildNodes()) continue;
				MetaDB metaDB = new MetaDB();
				//metaDBList.add(metaDB);
				this.parseMetaDBAttribute(metaDBNode, metaDB);
				List<Table> tableList = new ArrayList<Table>();
				//metaDB.setTableList(tableList);
				
				NodeList tableNodeList = metaDBNode.getChildNodes(); 
				for (int j = 0; j < tableNodeList.getLength(); j++) { 
					Node tableNode = tableNodeList.item(j); 
					if (!tableNode.hasChildNodes()) continue;
					Table table = new Table();
					//tableList.add(table);
					this.parseTableAttribute(tableNode, table);
					List<Column> columnList = new ArrayList<Column>();
					//table.setColumnList(columnList);
					
					NodeList columnNodeList = tableNode.getChildNodes(); 
					for (int k = 0; k < columnNodeList.getLength(); k++) { 
						Node columnNode = columnNodeList.item(k);
						if (!columnNode.hasChildNodes()) continue;
						//this.parseAttribute(columnNode);
						Column column = new Column();
						//columnList.add(column);
						this.parseColumnAttribute(columnNode, column);
						
						NodeList columnContentNodeList = columnNode.getChildNodes();
						
						for(int m = 0; m < columnContentNodeList.getLength(); m++) {
							//Node columnContent = columnContentList.item(m);
							Node columnContentNode = columnContentNodeList.item(m);
							// The CRLF character will be treated as a node, so using IF.
							if (columnContentNode.hasChildNodes()) {
								
								//System.out.println(columnContentNode.getNodeName() + ":" + columnContentNode.getTextContent());
								//System.out.println(m);
								if (columnContentNode.getNodeName().equals("columnName")) {
									column.setName(columnContentNode.getTextContent());
								} else if (columnContentNode.getNodeName().equals("columnType")) {
									column.setType(columnContentNode.getTextContent());
								} else {
									logger.error("unknown column content.");
								}
							}
						}
						columnList.add(column);
					} 
					table.setColumnList(columnList);
					tableList.add(table);
					logger.info(j + " table tackled.");
				} 
				metaDB.setTableList(tableList);
				metaDBList.add(metaDB);
				logger.info(i + " metaDB tackled.");
			} 
			infoTables.setMetaDBList(metaDBList);
			//System.out.println("parse XML success."); 
			logger.info("Parse XML success.");
			return infoTables;
		} catch (FileNotFoundException e) { 
			//System.out.println(e.getMessage()); 
			logger.error(e.getMessage());
		} catch (ParserConfigurationException e) { 
			//System.out.println(e.getMessage());
			logger.error(e.getMessage());
		} catch (SAXException e) { 
			//System.out.println(e.getMessage());
			logger.error(e.getMessage());
		} catch (IOException e) { 
			//System.out.println(e.getMessage());
			logger.error(e.getMessage());
		} 
		return null;
	} 

	
	private void parseColumnAttribute(Node columnNode, Column column) {
		
		NamedNodeMap attList = columnNode.getAttributes();
		if(attList != null) {
			for(int i=0; i<attList.getLength(); i++) {
				Node att = attList.item(i);
				
				//System.out.println(att.getNodeName() + ":" + att.getNodeValue());
				//logger.info(att.getNodeName() + ":" + att.getNodeValue());
				if (att.getNodeName().equals("id")) {
					column.setId(att.getNodeValue());
				} else {
					logger.error("unknown Column attribute");
				}
			}
		}
	}

	private void parseTableAttribute(Node tableNode, Table table) {
		
		NamedNodeMap attList = tableNode.getAttributes();
		if(attList != null) {
			for(int i=0; i<attList.getLength(); i++) {
				Node att = attList.item(i);
				
				//System.out.println(att.getNodeName() + ":" + att.getNodeValue());
				logger.info(att.getNodeName() + " : " + att.getNodeValue());
				if (att.getNodeName().equals("id")) {
					table.setId(att.getNodeValue());
				} else if (att.getNodeName().equals("name")) {
					table.setName(att.getNodeValue());
				} else {
					logger.error("unknown Table attribute");
				}
			}
		}
	}

	
	
	// Parse the attributes in the node 
	public void parseAttribute(Node node) {
		NamedNodeMap attList = node.getAttributes();
		if(attList != null) {
			for(int n=0; n<attList.getLength(); n++) {
				Node att = attList.item(n);
				//System.out.println(att.getNodeName() + ":" + att.getNodeValue());
				logger.info(att.getNodeName() + ":" + att.getNodeValue());
			}
		}
	}
	
	// parse the attributes of metaDB and load them to a MetaDB object
	public void parseMetaDBAttribute(Node node, MetaDB metaDB) {
		NamedNodeMap attList = node.getAttributes();
		if(attList != null) {
			for(int i=0; i<attList.getLength(); i++) {
				Node att = attList.item(i);
				
				//System.out.println(att.getNodeName() + ":" + att.getNodeValue());
				logger.info(att.getNodeName() + " : " + att.getNodeValue());
				if (att.getNodeName().equals("id")) {
					metaDB.setId(att.getNodeValue());
				} else if (att.getNodeName().equals("name")) {
					metaDB.setName(att.getNodeValue());
				} else {
					logger.error("unknown MetaDB attribute");
				}
			}
		}
	}
	
}
