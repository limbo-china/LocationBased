package cn.ac.iie.jc.group.data;

import com.google.gson.Gson;

public class AggregateCount {

	private int total = 0;
	private int inner = 0;
	private int outer = 0;

	public int getTotal() {
		return total;
	}

	public void increTotal() {
		total++;
	}

	public int getInner() {
		return inner;
	}

	public void increInner() {
		inner++;
	}

	public int getOuter() {
		return outer;
	}

	public void increOuter() {
		outer++;
	}

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
}
