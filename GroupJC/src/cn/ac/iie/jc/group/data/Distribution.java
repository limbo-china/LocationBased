package cn.ac.iie.jc.group.data;

import java.util.HashMap;
import java.util.Map;

public class Distribution {

	private Group group;
	private AggregateCount aggregate = new AggregateCount();
	private HashMap<String, ProvincePopulation> provinceDisribution = new HashMap<String, ProvincePopulation>();

	public Group getGroup() {
		return group;
	}

	public AggregateCount getAggregate() {
		return aggregate;
	}

	public synchronized void increTotal() {
		this.aggregate.increTotal();
	}

	public synchronized void increInner() {
		this.aggregate.increInner();
	}

	public synchronized void increOuter() {
		this.aggregate.increOuter();
	}

	public Distribution(Group group) {
		this.group = group;
	}

	public HashMap<String, ProvincePopulation> getProvinceDisribution() {
		return provinceDisribution;
	}

	private synchronized boolean containProvince(String prov) {
		return provinceDisribution.containsKey(prov);
	}

	public synchronized void addProvincePopulation(ProvincePopulation popu, boolean isWhole) {
		if (!isWhole)
			popu.setGroupId(group.getGroupId());
		if (!containProvince(popu.getProvinceId()))
			provinceDisribution.put(popu.getProvinceId(), popu);
		else {
			ProvincePopulation originPopu = provinceDisribution.get(popu.getProvinceId());
			originPopu.increCountByN(popu.getCount());

			for (Map.Entry<String, CityPopulation> entry : originPopu.getCityDistribution().entrySet())
				originPopu.increCityPopulationByCityPopulation(entry.getValue());
		}
	}

}
