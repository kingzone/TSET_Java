package com.teradata.tset2.pgsql.pojo;

public class PhysicalConfiguration {

	private int system_id;
	private int procid;
	private String status;
	private String CPUType;
	private int CPUCount;
	private String systemType;
	private int cliqueNo;
	private String netAUP;
	private String netBUP;
	public int getSystem_id() {
		return system_id;
	}
	public void setSystem_id(int system_id) {
		this.system_id = system_id;
	}
	public int getProcid() {
		return procid;
	}
	public void setProcid(int procid) {
		this.procid = procid;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getCPUType() {
		return CPUType;
	}
	public void setCPUType(String cPUType) {
		CPUType = cPUType;
	}
	public int getCPUCount() {
		return CPUCount;
	}
	public void setCPUCount(int cPUCount) {
		CPUCount = cPUCount;
	}
	public String getSystemType() {
		return systemType;
	}
	public void setSystemType(String systemType) {
		this.systemType = systemType;
	}
	public int getCliqueNo() {
		return cliqueNo;
	}
	public void setCliqueNo(int cliqueNo) {
		this.cliqueNo = cliqueNo;
	}
	public String getNetAUP() {
		return netAUP;
	}
	public void setNetAUP(String netAUP) {
		this.netAUP = netAUP;
	}
	public String getNetBUP() {
		return netBUP;
	}
	public void setNetBUP(String netBUP) {
		this.netBUP = netBUP;
	}
	
}
