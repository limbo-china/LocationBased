package cn.ac.iie.jc.group.data;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import cn.ac.iie.jc.config.VersionConfig;

public class ProvincePopulation {

	private String c_groupid;
	private String c_provinceid;
	private String c_provincename;
	private int c_count = 0;
	private String c_dayid;
	private int c_version = 0;
	private long c_updatetime;
	private static long update = System.currentTimeMillis() / 1000;
	private HashMap<String, CityPopulation> cityDistribution = new HashMap<String, CityPopulation>();

	public String getGroupId() {
		return c_groupid;
	}

	public void setGroupId(String groupId) {
		this.c_groupid = groupId;
	}

	public String getProvinceId() {
		return c_provinceid;
	}

	public void setProvinceId(String provinceId) {
		this.c_provinceid = provinceId;
	}

	public String getProvinceName() {
		return c_provincename;
	}

	public void setProvinceName(String provinceName) {
		this.c_provincename = provinceName;
	}

	public int getCount() {
		return c_count;
	}

	public void increCount() {
		this.c_count++;
	}

	public void increCountByN(int n) {
		this.c_count = this.c_count + n;
	}

	public String getDayId() {
		return c_dayid;
	}

	public void updateDayId() {
		this.c_dayid = stampToDate(System.currentTimeMillis());
	}

	public int getVersion() {
		return c_version;
	}

	public void updateVersion() {
		this.c_version = VersionConfig.getInt("provinceVersion");
	}

	public long getUpdateTime() {
		return c_updatetime;
	}

	public void updateUpdateTime() {
		this.c_updatetime = update;
	}

	public HashMap<String, CityPopulation> getCityDistribution() {
		return cityDistribution;
	}

	public void increCityPopulationByCity(City city) {
		String cityId = city.getCityId();
		if (cityId.length() != 6) {
			if (cityDistribution.get("000000") == null) {
				CityPopulation popu = new CityPopulation();
				popu.setGroupId(c_groupid);
				popu.setProvinceId(c_provinceid);
				popu.setProvinceName(c_provincename);
				popu.setCityId("000000");
				popu.setCityName("未知");
				popu.updateDayId();
				popu.updateUpdateTime();
				popu.updateVersion();
				popu.increCount();
				cityDistribution.put("000000", popu);
			} else
				cityDistribution.get("000000").increCount();
		} else if (cityDistribution.get(cityId) == null) {
			CityPopulation popu = new CityPopulation();
			popu.setGroupId(c_groupid);
			popu.setProvinceId(c_provinceid);
			popu.setProvinceName(c_provincename);
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

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}

	public String toDBJson() {
		Gson gson = new GsonBuilder().addSerializationExclusionStrategy(new ExclusionStrategy() {
			@Override
			public boolean shouldSkipField(FieldAttributes arg0) {
				if ("cityDistribution".equals(arg0.getName()))
					return true;
				return false;
			}

			@Override
			public boolean shouldSkipClass(Class<?> arg0) {
				return false;
			}
		}).create();
		return gson.toJson(this);
	}
}
