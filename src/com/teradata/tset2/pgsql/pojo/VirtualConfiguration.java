package com.teradata.tset2.pgsql.pojo;

public class VirtualConfiguration {

	private int system_id;
	private int procid;
	private int vprocNo;
	private String vprocType;
	private int hostid;
	private String status;
	private int diskSlice;
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
	public int getVprocNo() {
		return vprocNo;
	}
	public void setVprocNo(int vprocNo) {
		this.vprocNo = vprocNo;
	}
	public String getVprocType() {
		return vprocType;
	}
	public void setVprocType(String vprocType) {
		this.vprocType = vprocType;
	}
	public int getHostid() {
		return hostid;
	}
	public void setHostid(int hostid) {
		this.hostid = hostid;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public int getDiskSlice() {
		return diskSlice;
	}
	public void setDiskSlice(int diskSlice) {
		this.diskSlice = diskSlice;
	}
	
}
