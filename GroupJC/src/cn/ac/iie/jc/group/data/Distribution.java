package cn.ac.iie.jc.group.data;

import java.util.ArrayList;
import java.util.List;

public class Distribution {

	private Group group;
	private AggregateCount aggregate = new AggregateCount();
	private List<ProvincePopulation> provinceDisribution = new ArrayList<ProvincePopulation>();

	public Group getGroup() {
		return group;
	}

	public AggregateCount getAggregate() {
		return aggregate;
	}

	public void increTotal() {
		this.aggregate.increTotal();
	}

	public void increInner() {
		this.aggregate.increInner();
	}

	public void increOuter() {
		this.aggregate.increOuter();
	}

	public Distribution(Group group) {
		this.group = group;
	}

	public void addProvincePopulation(ProvincePopulation popu) {
		provinceDisribution.add(popu);
	}

}
