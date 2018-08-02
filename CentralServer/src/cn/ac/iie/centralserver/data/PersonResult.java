package cn.ac.iie.centralserver.data;

public class PersonResult {

	int status = -1;
	String imsi = "";
	String imei = "";
	String msisdn = "";
	String regionCode = "";
	String lac = "";
	String ci = "";
	String uli = "";
	String homeCode = "";
	double lngi = 0.0;
	double lati = 0.0;
	String time = "";
	String province = "";
	String city = "";
	String district = "";
	String baseinfo = "";
	String reason = null;

	public PersonResult() {
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
		setReason(mapReason(status));
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

	public String getReason() {
		return reason;
	}

	private void setReason(String reason) {
		this.reason = reason;
	}

	private String mapReason(int ret) {
		switch (ret) {
		case 0:
			return null;
		case 1:
			return "服务器错误";
		case 2:
			return "请求参数非法";
		case 3:
			return "权限校验失败";
		case 4:
			return "配额不足";
		case 5:
			return "token不存在或非法";
		case 6:
			return "手机号映射缺失";
		case 7:
			return "查询结果为空";
		default:
			return "未知错误";
		}
	}
}
