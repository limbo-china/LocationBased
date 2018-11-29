package cn.ac.iie.upushcongfig.bean;

import java.util.Arrays;
import java.util.List;

public class QueryRequest {

	private String remoteHost;
	private String url;
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

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public void generateIndexList() {
		indexList = index.split(",");
	}

	public List<String> getIndexList() {
		return Arrays.asList(indexList);
	}

	@Override
	public String toString() {
		return "QueryRequest [remoteHost=" + remoteHost + ", url=" + url
				+ ", indexCount=" + indexList.length + "]";
	}

}
