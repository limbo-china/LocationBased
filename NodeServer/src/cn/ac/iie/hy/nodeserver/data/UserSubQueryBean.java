package cn.ac.iie.hy.nodeserver.data;

public class UserSubQueryBean {

	int status;
	String imsi;
	String imei;
	String msisdn;
	String regionCode;
	String lac;
	String ci;
	String uli;
	String homeCode;
	double lngi;
	double lati;
	String time;
	
	public UserSubQueryBean() {
		super();
	}
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
	public String getRegionCode() {
		return regionCode;
	}
	public void setRegionCode(String regionCode) {
		this.regionCode = regionCode;
	}
	public String getLac() {
		return lac;
	}
	public void setLac(String lac) {
		this.lac = lac;
	}
	public String getCi() {
		return ci;
	}
	public void setCi(String ci) {
		this.ci = ci;
	}
	public String getUli() {
		return uli;
	}
	public void setUli(String uli) {
		this.uli = uli;
	}
	public String getHomeCode() {
		return homeCode;
	}
	public void setHomeCode(String homeCode) {
		this.homeCode = homeCode;
	}
	public double getLngi() {
		return lngi;
	}
	public void setLngi(double lngi) {
		this.lngi = lngi;
	}
	public double getLati() {
		return lati;
	}
	public void setLati(double lati) {
		this.lati = lati;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String timestamp) {
		this.time = timestamp;
	}
	
	
}
