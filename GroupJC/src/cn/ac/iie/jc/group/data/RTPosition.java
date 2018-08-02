package cn.ac.iie.jc.group.data;

public class RTPosition {

	private String imsi;
	private String imei;
	private String msisdn;
	private String regionCode;
	private String uli;
	private double lngi = 0;
	private double lati = 0;
	private String time;
	private String province;
	private String city;
	private String district;
	private String baseinfo;

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

	public String getUli() {
		return uli;
	}

	public void setUli(String uli) {
		this.uli = uli;
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

	public void setTime(String time) {
		this.time = time;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getDistrict() {
		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	public String getBaseinfo() {
		return baseinfo;
	}

	public void setBaseinfo(String baseinfo) {
		this.baseinfo = baseinfo;
	}

	@Override
	public String toString() {
		return imsi + "," + imei + "," + msisdn + "," + regionCode + "," + uli + "," + lngi + "," + lati + "," + time
				+ "," + province + "," + city + "," + district + "," + baseinfo;
	}

}
