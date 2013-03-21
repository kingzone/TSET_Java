package com.teradata.tset2.pgsql.pojo;

import java.util.Date;

public class DDL_KV {

	private String key;
	private Date ddl_createTimestamp;
	private String ddl_txt;
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Date getDdl_createTimestamp() {
		return ddl_createTimestamp;
	}
	public void setDdl_createTimestamp(Date ddl_createTimestamp) {
		this.ddl_createTimestamp = ddl_createTimestamp;
	}
	public String getDdl_txt() {
		return ddl_txt;
	}
	public void setDdl_txt(String ddl_txt) {
		this.ddl_txt = ddl_txt;
	}
	
}
