package cn.ac.iie.centralserver.trace.bean;

public class QueryRequest {

	private String remoteHost;
	private String url;
	private String token;
	private String queryType;
	private String index;
	private String[] indexList;
	private String startTime;
	private String endTime;

	public String getRemoteHost() {
		return remoteHost;
	}

	public void setRemoteHost(String remoteHost) {
		this.remoteHost = remoteHost;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getQueryType() {
		return queryType;
	}

	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public void generateIndexList() {
		indexList = index.split(",");
	}

	public String[] getIndexList() {
		return indexList;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String starttime) {
		this.startTime = starttime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endtime) {
		this.endTime = endtime;
	}

	@Override
	public String toString() {
		return "QueryRequest [remoteHost=" + remoteHost + ", url=" + url + ", token=" + token + ", queryType="
				+ queryType + ", indexCount=" + indexList.length + ", startTime="
				+ startTime + ", endTime=" + endTime + "]";
	}

	

}
