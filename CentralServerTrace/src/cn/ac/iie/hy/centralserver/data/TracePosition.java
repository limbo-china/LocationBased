package cn.ac.iie.hy.centralserver.data;

import java.io.Serializable;

public class TracePosition implements Serializable {

	private static final long serialVersionUID = -2509845860519483925L;

	private String imsi;
	private String imei;
	private String msisdn;
	private String regionCode;
	private String uli;
	private float longi;
	private float lati;
	private String province;
	private String city;
	private String district;
	private String baseInfo;
	private String time;

	public String getUli() {
		return uli;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public void setImei(String imei) {
		this.imei = imei;
	}

	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}

	public void setRegionCode(String regionCode) {
		this.regionCode = regionCode;
	}

	public void setUli(String uli) {
		this.uli = uli;
	}

	public void setLongi(float longi) {
		this.longi = longi;
	}

	public void setLati(float lati) {
		this.lati = lati;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	public void setBaseInfo(String baseInfo) {
		this.baseInfo = baseInfo;
	}

	public void setTime(String time) {
		this.time = time;
	}

}
