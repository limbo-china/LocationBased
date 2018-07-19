package cn.ac.iie.jc.query.data;

public class QueryRequest {

	private String remoteHost;
	private String url;
	private String token;
	private String groupId;
	private String queryType;
	private int pageSize;
	private int currentPage;

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

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getQueryType() {
		return queryType;
	}

	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getCurrentPage() {
		return currentPage;
	}

	public void setCurrentPage(int currentPage) {
		this.currentPage = currentPage;
	}

	@Override
	public String toString() {
		return "QueryRequest [remoteHost=" + remoteHost + ", url=" + url
				+ ", token=" + token + ", groupId=" + groupId + ", queryType="
				+ queryType + ", pageSize=" + pageSize + ", currentPage="
				+ currentPage + "]";
	}

}
