package cn.ac.iie.jc.group.data;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.gson.Gson;

import cn.ac.iie.jc.config.VersionConfig;

public class CityPopulation {

	private String c_groupid;
	private String c_provinceid;
	private String c_provincename;
	private String c_cityid;
	private String c_cityname;
	private int c_count = 0;
	private String c_dayid;
	private int c_version = 0;
	private long c_updatetime;
	private static long update = System.currentTimeMillis() / 1000;

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

	public String getCityId() {
		return c_cityid;
	}

	public void setCityId(String cityId) {
		this.c_cityid = cityId;
	}

	public String getCityName() {
		return c_cityname;
	}

	public void setCityName(String cityName) {
		this.c_cityname = cityName;
	}

	public int getCount() {
		return c_count;
	}

	public void setCount(int count) {
		this.c_count = count;
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
		this.c_version = VersionConfig.getInt("cityVersion");
	}

	public long getUpdateTime() {
		return c_updatetime;
	}

	public void updateUpdateTime() {
		this.c_updatetime = update;
	}

	public CityPopulation clone() {
		CityPopulation popu = new CityPopulation();
		popu.setGroupId(this.getGroupId());
		popu.setProvinceId(this.getProvinceId());
		popu.setProvinceName(this.getProvinceName());
		popu.setCityId(this.getCityId());
		popu.setCityName(this.getCityName());
		popu.setCount(this.getCount());
		popu.updateDayId();
		popu.updateUpdateTime();
		popu.updateVersion();
		return popu;
	}

	private static String stampToDate(long stamp) {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
}
