package cn.ac.iie.jc.group.data;

import java.util.HashMap;

import com.google.gson.Gson;

public class Distribution {

	private String groupId;
	private int total;
	private int inner;
	private int outer;
	private HashMap<String, Integer> distribution = new HashMap<String, Integer>();

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public void setInner(int inner) {
		this.inner = inner;
	}

	public void setOuter(int outer) {
		this.outer = outer;
	}

	public void addCount(String province, int count) {
		distribution.put(province, count);
	}

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
}
