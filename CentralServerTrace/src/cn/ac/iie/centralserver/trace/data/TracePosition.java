package cn.ac.iie.centralserver.trace.data;

import java.io.Serializable;

public class TracePosition implements Serializable {

	private static final long serialVersionUID = -2509845860519483925L;

	private String imsi = "";
	private String imei = "";
	private String msisdn = "";
	private String regionCode = "";
	private String uli = "";
	private float longi = 0;
	private float lati = 0;
	private String province = "";
	private String city = "";
	private String district = "";
	private String baseInfo = "";
	private String time = "";

	public String getUli() {
		return uli;
	}

	public void setImsi(String imsi) {
		if (imsi == null)
			imsi = "";
		this.imsi = imsi;
	}

	public void setImei(String imei) {
		if (imei == null)
			imei = "";
		this.imei = imei;
	}

	public void setMsisdn(String msisdn) {
		if (msisdn == null)
			msisdn = "";
		this.msisdn = msisdn;
	}

	public void setRegionCode(String regionCode) {
		if (regionCode == null)
			regionCode = "";
		this.regionCode = regionCode;
	}

	public void setUli(String uli) {
		if (uli == null)
			uli = "";
		this.uli = uli;
	}

	public void setLongi(float longi) {
		this.longi = longi;
	}

	public void setLati(float lati) {
		this.lati = lati;
	}

	public void setProvince(String province) {
		if (province == null)
			province = "";
		this.province = province;
	}

	public void setCity(String city) {
		if (city == null)
			city = "";
		this.city = city;
	}

	public void setDistrict(String district) {
		if (district == null)
			district = "";
		this.district = district;
	}

	public void setBaseInfo(String baseInfo) {
		if (baseInfo == null)
			baseInfo = "";
		this.baseInfo = baseInfo;
	}

	public void setTime(String time) {
		if (time == null)
			time = "";
		this.time = time;
	}

	@Override
	public String toString() {
		return "TracePosition [imsi=" + imsi + ", imei=" + imei + ", msisdn=" + msisdn + ", regionCode=" + regionCode
				+ ", uli=" + uli + ", longi=" + longi + ", lati=" + lati + ", province=" + province + ", city=" + city
				+ ", district=" + district + ", baseInfo=" + baseInfo + ", time=" + time + "]";
	}

}
