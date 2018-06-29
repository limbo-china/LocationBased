package cn.ac.iie.hy.nodeserver.data;
import java.io.Serializable;


public class SMetaData implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5311306462835188881L;
	public SMetaData(String imsi, String imei, String msisdn, String relateNum, String regionCode, String lac,
			String ci, String uli, String homeCode, double lngi, double lati, long timestamp, int cdrType) {
		super();
		this.imsi = imsi;
		this.imei = imei;
		this.msisdn = msisdn;
		this.relateNum = relateNum;
		this.regionCode = regionCode;
		this.lac = lac;
		this.ci = ci;
		this.uli = uli;
		this.homeCode = homeCode;
		this.lngi = lngi;
		this.lati = lati;
		this.timestamp = timestamp;
		this.cdrType = cdrType;
	}
	
	String imsi;
	String imei;
	String msisdn;
	String relateNum;
	String regionCode;
	String lac;
	String ci;
	String uli;
	String homeCode;
	String cdrContent;
	double lngi;
	double lati;
	long timestamp;
	
	int cdrType;

	
	public String getCdrContent() {
		return cdrContent;
	}

	public void setCdrContent(String cdrContent) {
		this.cdrContent = cdrContent;
	}

		
	public String getRelateNum() {
		return relateNum;
	}

	public void setRelateNum(String relateNum) {
		this.relateNum = relateNum;
	}

	public int getCdrType() {
		return cdrType;
	}

	public void setCdrType(int cdrType) {
		this.cdrType = cdrType;
	}

	public SMetaData(){
		
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
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	
	
}
