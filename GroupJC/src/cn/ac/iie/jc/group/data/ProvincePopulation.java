package cn.ac.iie.jc.group.data;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class ProvincePopulation {

	private String provinceId;
	private String provinceName;
	private int count = 0;
	private String dayId;
	private int version = 0;
	private long updateTime;
	private HashMap<String, CityPopulation> cityDistribution = new HashMap<String, CityPopulation>();

	public String getProvinceId() {
		return provinceId;
	}

	public void setProvinceId(String provinceId) {
		this.provinceId = provinceId;
	}

	public String getProvinceName() {
		return provinceName;
	}

	public void setProvinceName(String provinceName) {
		this.provinceName = provinceName;
	}

	public int getCount() {
		return count;
	}

	public void increCount() {
		this.count++;
	}

	public void increCountByN(int n) {
		this.count = this.count + n;
	}

	public String getDayId() {
		return dayId;
	}

	public void updateDayId() {
		this.dayId = stampToDate(System.currentTimeMillis());
	}

	public int getVersion() {
		return version;
	}

	public void updateVersion() {
		this.version++;
	}

	public long getUpdateTime() {
		return updateTime;
	}

	public void updateUpdateTime() {
		this.updateTime = System.currentTimeMillis() / 1000;
	}

	public HashMap<String, CityPopulation> getCityDistribution() {
		return cityDistribution;
	}

	public void increCityPopulationByCity(City city) {
		if (city.getCityName() == null)
			return;
		String cityId = city.getCityId();
		if (cityDistribution.get(cityId) == null) {
			CityPopulation popu = new CityPopulation();
			popu.setCityId(cityId);
			popu.setCityName(city.getCityName());
			popu.updateDayId();
			popu.updateUpdateTime();
			popu.updateVersion();
			popu.increCount();
			cityDistribution.put(cityId, popu);
		} else
			cityDistribution.get(cityId).increCount();
	}

	public void increCityPopulationByCityPopulation(CityPopulation popu) {

		String cityId = popu.getCityId();
		if (cityDistribution.get(cityId) == null)
			cityDistribution.put(cityId, popu);
		else
			cityDistribution.get(cityId).increCountByN(popu.getCount());
	}

	private static String stampToDate(long stamp) {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}
}
