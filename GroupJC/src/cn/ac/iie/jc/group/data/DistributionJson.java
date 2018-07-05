package cn.ac.iie.jc.group.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.ac.iie.jc.config.ProvinceMap;

public class DistributionJson {
	private String groupId;
	private int total = 0;
	private int inner = 0;
	private int outer = 0;
	private List<HashMap<String, String>> distribution = new ArrayList<HashMap<String, String>>();
	private String updateBy;
	private String updateTime;

	public DistributionJson(Distribution distrib) {
		this.groupId = distrib.getGroup().getGroupId();
		this.total = distrib.getTotal();
		this.inner = distrib.getInner();
		this.outer = distrib.getOuter();
		this.updateBy = distrib.getUpdateBy();
		this.updateTime = distrib.getUpdateTime();

		for (Map.Entry<String, Integer> entry : distrib.getDistribution()
				.entrySet()) {
			HashMap<String, String> map = new HashMap<String, String>();
			map.put("count", String.valueOf(entry.getValue()));
			map.put("provinceName", codeToName(entry.getKey()));
			map.put("provinceId", entry.getKey());
			distribution.add(map);
		}
	}

	private String codeToName(String provinceCode) {
		return ProvinceMap.getString(provinceCode);
	}

}
