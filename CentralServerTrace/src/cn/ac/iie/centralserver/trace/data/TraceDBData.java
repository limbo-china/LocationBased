package cn.ac.iie.centralserver.trace.data;

import java.io.Serializable;

public class TraceDBData implements Serializable,Comparable<TraceDBData>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 8843150323040275335L;
	
	private String c_imsi = "";
	private String c_imei = "";
	private String c_msisdn = "";
	private String c_uli = "";
	private String c_areacode = "";
	private Long c_timestamp = 0L;
	
	public String getC_imsi() {
		return c_imsi;
	}
	public void setC_imsi(String c_imsi) {
		this.c_imsi = c_imsi;
	}
	public String getC_imei() {
		return c_imei;
	}
	public void setC_imei(String c_imei) {
		this.c_imei = c_imei;
	}
	public String getC_msisdn() {
		return c_msisdn;
	}
	public void setC_msisdn(String c_msisdn) {
		this.c_msisdn = c_msisdn;
	}
	public String getC_uli() {
		return c_uli;
	}
	public void setC_uli(String c_uli) {
		this.c_uli = c_uli;
	}
	public String getC_areacode() {
		return c_areacode;
	}
	public void setC_areacode(String c_areacode) {
		this.c_areacode = c_areacode;
	}
	public Long getC_timestamp() {
		return c_timestamp;
	}
	public void setC_timestamp(Long c_timestamp) {
		this.c_timestamp = c_timestamp;
	}
	@Override
	public int compareTo(TraceDBData o) {
		return (this.c_timestamp<o.c_timestamp)?-1:1;
	}
}
