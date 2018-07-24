package cn.ac.iie.jc.group.data;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CityPopulation {

	private String cityId;
	private String cityName;
	private int count = 0;
	private String dayId;
	private int version = 0;
	private long updateTime;

	public String getCityId() {
		return cityId;
	}

	public void setCityId(String cityId) {
		this.cityId = cityId;
	}

	public String getCityName() {
		return cityName;
	}

	public void setCityName(String cityName) {
		this.cityName = cityName;
	}

	public int getCount() {
		return count;
	}

	public void increCount() {
		this.count++;
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

	private static String stampToDate(long stamp) {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}
}
