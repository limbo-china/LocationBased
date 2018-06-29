package cn.ac.iie.hy.centralserver.data;

import java.io.Serializable;
import java.util.List;

public class TraceQueryResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2645667382326939695L;
	private int status;
	private String imsi = "";
	private String imei = "";
	private String msisdn = "";
	private String tracelist = "";
	
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getImsi() {
		return imsi;
	}
	public void setImsi(String imsi) {
		this.imsi = imsi;
	}
	public String getImei() {
		return imei;
	}
	public void setImei(String imei) {
		this.imei = imei;
	}
	public String getMsisdn() {
		return msisdn;
	}
	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}
	public String getTracelist() {
		return tracelist;
	}
	public void setTracelist(String tracelist) {
		this.tracelist = tracelist;
	}
	
}
