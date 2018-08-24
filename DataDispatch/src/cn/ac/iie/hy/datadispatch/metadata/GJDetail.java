package cn.ac.iie.hy.datadispatch.metadata;

public class GJDetail {
	String groupid;
	String groupname;
	String provincename;
	int source;
	//用于存储key<province gjdetail>
	public String getGroupid() {
		return groupid;
	}
	public void setGroupid(String groupid) {
		this.groupid = groupid;
	}
	public String getGroupname() {
		return groupname;
	}
	public void setGroupname(String groupname) {
		this.groupname = groupname;
	}
	public String getProvinceName() {
		return provincename;
	}
	public void setProvinceName(String provinceName) {
		this.provincename = provinceName;
	}
	public int getSource() {
		return source;
	}
	public void setSource(int source) {
		this.source = source;
	}

	
}
