package cn.ac.iie.jc.query.data;

import com.google.gson.Gson;

public class JCPerson {

	private String phone;
	private String createBy;
	private String createTime;
	private String updateBy;
	private String updateTime;

	private JCPerson() {
	}

	public String getPhone() {
		return phone;
	}

	public String getCreateBy() {
		return createBy;
	}

	public String getCreateTime() {
		return createTime;
	}

	public String getUpdateBy() {
		return updateBy;
	}

	public String getUpdateTime() {
		return updateTime;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public void setCreateBy(String createBy) {
		this.createBy = createBy;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public void setUpdateBy(String updateBy) {
		this.updateBy = updateBy;
	}

	public void setUpdateTime(String updateTime) {
		this.updateTime = updateTime;
	}

	@Override
	public String toString() {
		return "JCPerson [phone=" + phone + ", createBy=" + createBy + ", createTime=" + createTime + ", updateBy="
				+ updateBy + ", updateTime=" + updateTime + "]";
	}

	public static JCPerson newInstanceFromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, JCPerson.class);
	}
}
