package cn.ac.iie.hy.datadispatch.metadata;

public class RTPGJData {

	String c_groupid;
	String c_groupname;
	int c_source;
	String c_provinceid;
	String c_provincename;
	String c_phone;
	Long c_updatetime;
	int c_action;
	

	
	public RTPGJData() {
		super();
		// TODO Auto-generated constructor stub
	}



	public RTPGJData(String c_groupid, String c_groupname, int c_source, String c_provinceid, String c_provincename,
			String c_phone, Long c_updatetime, int c_action) {
		super();
		this.c_groupid = c_groupid;
		this.c_groupname = c_groupname;
		this.c_source = c_source;
		this.c_provinceid = c_provinceid;
		this.c_provincename = c_provincename;
		this.c_phone = c_phone;
		this.c_updatetime = c_updatetime;
		this.c_action = c_action;
	}



	public String getC_groupid() {
		return c_groupid;
	}



	public void setC_groupid(String c_groupid) {
		this.c_groupid = c_groupid;
	}



	public String getC_groupname() {
		return c_groupname;
	}



	public void setC_groupname(String c_groupname) {
		this.c_groupname = c_groupname;
	}



	public int getC_source() {
		return c_source;
	}



	public void setC_source(int c_source) {
		this.c_source = c_source;
	}



	public String getC_provinceid() {
		return c_provinceid;
	}



	public void setC_provinceid(String c_provinceid) {
		this.c_provinceid = c_provinceid;
	}



	public String getC_provincename() {
		return c_provincename;
	}



	public void setC_provincename(String c_provincename) {
		this.c_provincename = c_provincename;
	}



	public String getC_phone() {
		return c_phone;
	}



	public void setC_phone(String c_phone) {
		this.c_phone = c_phone;
	}



	public Long getC_updatetime() {
		return c_updatetime;
	}



	public void setC_updatetime(Long c_updatetime) {
		this.c_updatetime = c_updatetime;
	}



	public int getC_action() {
		return c_action;
	}



	public void setC_action(int c_action) {
		this.c_action = c_action;
	}






	
	
}
