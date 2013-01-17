package com.teradata.qaf.tset.common;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.teradata.qaf.tset.pojo.TSETInfoTables;

/**
 * 
 * @author zj255003
 * @description load config file to the pojo TSETInfoTables
 *
 */
public class ConfigLoader {

	private TSETInfoTables tables;
	
	public ConfigLoader() {
		tables = new TSETInfoTables();
	}
	
	public void loadTSETInfoTables(Node node) {
		NamedNodeMap attList = node.getAttributes();
		if(attList != null) {
			for(int n=0; n<attList.getLength(); n++) {
				Node att = attList.item(n);
				System.out.println(att.getNodeName() + ":" + att.getNodeValue());
			}
		}
	}
	
	// parse attributes of metaDB
	public void loadMetaDB(Node node) {
		NamedNodeMap attList = node.getAttributes();
		if(attList != null) {
			for(int i=0; i<attList.getLength(); i++) {
				Node att = attList.item(i);
				
				System.out.println(att.getNodeName() + ":" + att.getNodeValue());
				if (att.getNodeName().equals("id")) {
				} else if (att.getNodeName().equals("name")) {
				}
			}
		}
	}
	
	public void loadTable(Node node) {
		
	}
	
	public void loadColumn(Node node) {
		
	}
	
	public TSETInfoTables loadConfig() {
		tables = new TSETInfoTables();
		
		return tables;
	}
	
}
