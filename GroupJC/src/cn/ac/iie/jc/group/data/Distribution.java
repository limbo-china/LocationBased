package cn.ac.iie.jc.group.data;

import java.util.HashMap;

import cn.ac.iie.jc.config.ConfigUtil;

import com.google.gson.Gson;

public class Distribution {

	private Group group;
	private int total = 0;
	private int inner = 0;
	private int outer = 0;
	private HashMap<String, Integer> distribution = new HashMap<String, Integer>();
	private String updateBy;
	private String updateTime;

	public Group getGroup() {
		return group;
	}

	public int getTotal() {
		return total;
	}

	public int getInner() {
		return inner;
	}

	public int getOuter() {
		return outer;
	}

	public HashMap<String, Integer> getDistribution() {
		return distribution;
	}

	public String getUpdateBy() {
		return updateBy;
	}

	public String getUpdateTime() {
		return updateTime;
	}

	public void setUpdateBy(String updateBy) {
		this.updateBy = updateBy;
	}

	public void setUpdateTime(String updateTime) {
		this.updateTime = updateTime;
	}

	public Distribution(Group group) {
		this.group = group;
	}

	public boolean hasProvince(String province) {
		return distribution.get(province) != null;
	}

	public void initProvince(String province) {
		distribution.put(province, 0);
	}

	public void populationAccumulate(String province) {
		int origin = distribution.get(province);
		distribution.put(province, ++origin);

		if (ConfigUtil.getString("provinceCode").equals(province))
			inner = inner + 1;
		else
			outer = outer + 1;
		total = total + 1;
	}

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(new DistributionJson(this));
	}
}
