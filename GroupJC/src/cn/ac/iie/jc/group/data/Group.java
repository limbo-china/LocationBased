package cn.ac.iie.jc.group.data;

import com.google.gson.Gson;

public class Group {

	private String groupId;
	private String groupName;
	private String source;
	private int is_use;
	private String starttime;
	private String endtime;
	private String createby;
	private String createtime;
	private String updateby;
	private String updatetime;

	public Group(String groupId) {
		this.groupId = groupId;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public int isUse() {
		return is_use;
	}

	public void setUse(int isUse) {
		this.is_use = isUse;
	}

	public String getStartTime() {
		return starttime;
	}

	public void setStartTime(String startTime) {
		this.starttime = startTime;
	}

	public String getEndTime() {
		return endtime;
	}

	public void setEndTime(String endTime) {
		this.endtime = endTime;
	}

	public String getCreateBy() {
		return createby;
	}

	public void setCreateBy(String createBy) {
		this.createby = createBy;
	}

	public String getCreateTime() {
		return createtime;
	}

	public void setCreateTime(String createTime) {
		this.createtime = createTime;
	}

	public String getUpdateBy() {
		return updateby;
	}

	public void setUpdateBy(String updateBy) {
		this.updateby = updateBy;
	}

	public String getUpdateTime() {
		return updatetime;
	}

	public void setUpdateTime(String updateTime) {
		this.updatetime = updateTime;
	}

	@Override
	public String toString() {
		return "Group [groupId=" + groupId + ", groupName=" + groupName + ", source=" + source + ", isUse=" + is_use
				+ ", startTime=" + starttime + ", endTime=" + endtime + ", createBy=" + createby + ", createTime="
				+ createtime + ", updateBy=" + updateby + ", updateTime=" + updatetime + "]";
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof Group))
			return false;
		Group Group = (Group) o;
		return Group.groupId.equals(this.groupId);
	}

	@Override
	public int hashCode() {
		return groupId.hashCode();
	}

	public static Group newFromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, Group.class);
	}

}
