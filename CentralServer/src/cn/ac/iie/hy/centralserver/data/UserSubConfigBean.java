package cn.ac.iie.hy.centralserver.data;

import java.util.List;

public class UserSubConfigBean {
	String time;
	String indexType;
	List<String> personInfos;
	String crowdId;
	String crowdName;

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getIndexType() {
		return indexType;
	}

	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}

	public List<String> getPersonInfos() {
		return personInfos;
	}

	public void setPersonInfos(List<String> personInfos) {
		this.personInfos = personInfos;
	}

	public String getCrowdId() {
		return crowdId;
	}

	public void setCrowdId(String crowdId) {
		this.crowdId = crowdId;
	}

	public String getCrowdName() {
		return crowdName;
	}

	public void setCrowdName(String crowdName) {
		this.crowdName = crowdName;
	}

}
