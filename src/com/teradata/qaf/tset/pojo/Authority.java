package com.teradata.qaf.tset.pojo;

import java.util.List;

public class Authority {

	private List<String> expAuthorityList;
	private List<String> impAuthorityList;
	public List<String> getExpAuthorityList() {
		return expAuthorityList;
	}
	public void setExpAuthorityList(List<String> expAuthorityList) {
		this.expAuthorityList = expAuthorityList;
	}
	public List<String> getImpAuthorityList() {
		return impAuthorityList;
	}
	public void setImpAuthorityList(List<String> impAuthorityList) {
		this.impAuthorityList = impAuthorityList;
	}
	
}
