package cn.ac.iie.hy.nodeserver.task;

public class Data {
	String c_msisdn;
	String c_areacode;
	String c_uli;
	String c_province;
	String c_date;

	public Data(String c_msisdn, String c_areacode, String c_uli, String c_province, String c_date) {
		super();
		this.c_msisdn = c_msisdn;
		this.c_areacode = c_areacode;
		this.c_uli = c_uli;
		this.c_province = c_province;
		this.c_date = c_date;
	}

	public String getC_msisdn() {
		return c_msisdn;
	}

	public void setC_msisdn(String c_msisdn) {
		this.c_msisdn = c_msisdn;
	}

	public String getC_areacode() {
		return c_areacode;
	}

	public void setC_areacode(String c_areacode) {
		this.c_areacode = c_areacode;
	}

	public String getC_uli() {
		return c_uli;
	}

	public void setC_uli(String c_uli) {
		this.c_uli = c_uli;
	}

	public String getC_province() {
		return c_province;
	}

	public void setC_province(String c_province) {
		this.c_province = c_province;
	}

	public String getC_date() {
		return c_date;
	}

	public void setC_date(String c_date) {
		this.c_date = c_date;
	}

}
