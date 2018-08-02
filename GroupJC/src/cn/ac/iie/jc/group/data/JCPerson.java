package cn.ac.iie.jc.group.data;

import com.google.gson.Gson;

public class JCPerson {

	private String phone;
	private String createby;
	private String createtime;
	private String updateby;
	private String updatetime;

	private JCPerson() {
	}

	public String getPhone() {
		return phone;
	}

	public String getCreateBy() {
		return createby;
	}

	public String getCreateTime() {
		return createtime;
	}

	public String getUpdateBy() {
		return updateby;
	}

	public String getUpdateTime() {
		return updatetime;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public void setCreateBy(String createBy) {
		this.createby = createBy;
	}

	public void setCreateTime(String createTime) {
		this.createtime = createTime;
	}

	public void setUpdateBy(String updateBy) {
		this.updateby = updateBy;
	}

	public void setUpdateTime(String updateTime) {
		this.updatetime = updateTime;
	}

	@Override
	public String toString() {
		return "JCPerson [phone=" + phone + ", createBy=" + createby + ", createTime=" + createtime + ", updateBy="
				+ updateby + ", updateTime=" + updatetime + "]";
	}

	public static JCPerson newFromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, JCPerson.class);
	}
}
