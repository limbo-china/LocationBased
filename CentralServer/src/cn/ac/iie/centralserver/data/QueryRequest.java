package cn.ac.iie.centralserver.data;

import java.util.Arrays;

public class QueryRequest {

	private String remoteHost;
	private String url;
	private String token;
	private String queryType;
	private String index;
	private String[] indexList;

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

	@Override
	public String toString() {
		return "QueryRequest [remoteHost=" + remoteHost + ", url=" + url + ", token=" + token + ", queryType="
				+ queryType + ", indexCount=" + indexList.length + ":" + Arrays.asList(indexList).toString() + "]";
	}

}
