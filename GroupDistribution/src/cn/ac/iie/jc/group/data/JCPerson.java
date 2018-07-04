package cn.ac.iie.jc.group.data;

import com.google.gson.Gson;

public class JCPerson {

	private String phone;
	private String provinceId;
	private String provinceName;
	private String createBy;
	private String createTime;
	private String updateBy;
	private String updateTime;

	private JCPerson() {
	}

	public String getPhone() {
		return phone;
	}

	public String getProvinceId() {
		return provinceId;
	}

	public String getProvinceName() {
		return provinceName;
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

	public void setProvinceId(String provinceId) {
		this.provinceId = provinceId;
	}

	public void setProvinceName(String provinceName) {
		this.provinceName = provinceName;
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
		return "JCPerson [phone=" + phone + ", provinceId=" + provinceId
				+ ", provinceName=" + provinceName + ", createBy=" + createBy
				+ ", createTime=" + createTime + ", updateBy=" + updateBy
				+ ", updateTime=" + updateTime + "]";
	}

	public static JCPerson getFromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, JCPerson.class);
	}
}
