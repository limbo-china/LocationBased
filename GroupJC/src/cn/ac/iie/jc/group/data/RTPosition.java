package cn.ac.iie.jc.group.data;

import com.google.gson.Gson;

public class RTPosition {

	private String c_source = "";
	private String c_groupid = "";
	private String c_groupname = "";
	private int c_status = -1;
	private String c_imsi = "";
	private String c_imei = "";
	private String c_msisdn = "";
	private String c_regioncode = "";
	private String c_lac = "";
	private String c_ci = "";
	private String c_homecode = "";
	private String c_uli = "";
	private double c_lngi = 0;
	private double c_lati = 0;
	private String c_time = "";
	private String c_province = "";
	private String c_city = "";
	private String c_district = "";
	private String c_baseinfo = "";
	private String c_reason = "";

	public String getSource() {
		return c_source;
	}

	public void setSource(String c_source) {
		this.c_source = c_source;
	}

	public String getGroupid() {
		return c_groupid;
	}

	public void setGroupid(String c_groupid) {
		this.c_groupid = c_groupid;
	}

	public String getGroupname() {
		return c_groupname;
	}

	public void setGroupname(String c_groupname) {
		this.c_groupname = c_groupname;
	}

	public int getStatus() {
		return c_status;
	}

	public void setStatus(int c_status) {
		this.c_status = c_status;
		setReason(mapReason(c_status));
	}

	public String getImsi() {
		return c_imsi;
	}

	public void setImsi(String c_imsi) {
		this.c_imsi = c_imsi;
	}

	public String getImei() {
		return c_imei;
	}

	public void setImei(String c_imei) {
		this.c_imei = c_imei;
	}

	public String getMsisdn() {
		return c_msisdn;
	}

	public void setMsisdn(String c_msisdn) {
		this.c_msisdn = c_msisdn;
	}

	public String getRegionCode() {
		return c_regioncode;
	}

	public void setRegionCode(String c_regionCode) {
		this.c_regioncode = c_regionCode;
	}

	public String getLac() {
		return c_lac;
	}

	public void setLac(String c_lac) {
		this.c_lac = c_lac;
	}

	public String getCi() {
		return c_ci;
	}

	public void setCi(String c_ci) {
		this.c_ci = c_ci;
	}

	public String getHomecode() {
		return c_homecode;
	}

	public void setHomecode(String c_homecode) {
		this.c_homecode = c_homecode;
	}

	public String getUli() {
		return c_uli;
	}

	public void setUli(String c_uli) {
		this.c_uli = c_uli;
	}

	public double getLngi() {
		return c_lngi;
	}

	public void setLngi(double c_lngi) {
		this.c_lngi = c_lngi;
	}

	public double getLati() {
		return c_lati;
	}

	public void setLati(double c_lati) {
		this.c_lati = c_lati;
	}

	public String getTime() {
		return c_time;
	}

	public void setTime(String c_time) {
		this.c_time = c_time;
	}

	public String getProvince() {
		return c_province;
	}

	public void setProvince(String c_province) {
		this.c_province = c_province;
	}

	public String getCity() {
		return c_city;
	}

	public void setCity(String c_city) {
		this.c_city = c_city;
	}

	public String getDistrict() {
		return c_district;
	}

	public void setDistrict(String c_district) {
		this.c_district = c_district;
	}

	public String getBaseinfo() {
		return c_baseinfo;
	}

	public void setBaseinfo(String c_baseinfo) {
		this.c_baseinfo = c_baseinfo;
	}

	public String getReason() {
		return c_reason;
	}

	private void setReason(String c_reason) {
		this.c_reason = c_reason;
	}

	private String mapReason(int ret) {
		switch (ret) {
		case 0:
			return "正常";
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
		case 8:
			return "位置缺失";
		case 9:
			return "uli位置未上报";
		default:
			return "未知错误";
		}
	}

	@Override
	public String toString() {
		return "RTPosition [c_source=" + c_source + ", c_groupid=" + c_groupid + ", c_groupname=" + c_groupname
				+ ", c_status=" + c_status + ", c_imsi=" + c_imsi + ", c_imei=" + c_imei + ", c_msisdn=" + c_msisdn
				+ ", c_regioncode=" + c_regioncode + ", c_lac=" + c_lac + ", c_ci=" + c_ci + ", c_homecode="
				+ c_homecode + ", c_uli=" + c_uli + ", c_lngi=" + c_lngi + ", c_lati=" + c_lati + ", c_time=" + c_time
				+ ", c_province=" + c_province + ", c_city=" + c_city + ", c_district=" + c_district + ", c_baseinfo="
				+ c_baseinfo + ", c_reason=" + c_reason + "]";
	}
	
	public String toDBString(){
		return  "'"+c_source +"'"+ ","+
				"'"+c_groupid+"'"+"," +
				"'"+ c_groupname+"'"+","+
				c_status + ","+
				"'" +c_imsi+"'" +","+
				"'" +c_imei+"'" +","+
				"'" +c_msisdn+"'" +","+
				"'" +c_regioncode+"'" +","+
				"'" +c_lac+"'" +","+
				"'" +c_ci+"'" +","+
				"'" +c_homecode+"'" +","+
				"'" +c_uli+"'" +","+
				c_lngi+","+
				c_lati+","+
				"'" +c_time+"'" +","+
				"'" +c_province+"'" +","+
				"'" +c_city+"'" +","+
				"'" +c_district+"'" +","+
				"'" +c_baseinfo+"'" +","+
				"'" +c_reason+"'";
	}

	public String toFileString() {
		return c_imsi + "," + c_imei + "," + c_msisdn + "," + c_regioncode + "," + c_uli + "," + c_lngi + "," + c_lati
				+ "," + c_time + "," + c_province + "," + c_city + "," + c_district + "," + c_baseinfo;
	}

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

}
