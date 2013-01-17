package com.teradata.qaf.tset.pojo;

import java.util.List;

/**
 * 
 * @author zj255003
 * @version 1.0
 * @since 2012-12-18
 * @description Entity class of the configuration file to each teradata version.
 * The hierarchy of the file is: TSETInfoTables--MetaDB--Table--Column.
 *
 */
public class TSETInfoTables {

	private String id;
	private String name;
	private List<MetaDB> metaDBList;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<MetaDB> getMetaDBList() {
		return metaDBList;
	}
	public void setMetaDBList(List<MetaDB> metaDBList) {
		this.metaDBList = metaDBList;
	}
	
}
