package cn.ac.iie.jc.group.task;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.jc.config.ConfigUtil;
import cn.ac.iie.jc.config.ProvinceCityMap;
import cn.ac.iie.jc.config.ProvinceRedisMap;
import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.group.data.City;
import cn.ac.iie.jc.group.data.Distribution;
import cn.ac.iie.jc.group.data.Group;
import cn.ac.iie.jc.group.data.ProvincePopulation;
import cn.ac.iie.jc.log.LogUtil;

import com.google.gson.Gson;

public class DistributionExecutor implements Runnable {

	private Group group;
	private Distribution distrib;
	private List<String> imsis;

	public DistributionExecutor(Group group, List<String> imsis) {

		this.group = group;
		this.distrib = new Distribution(group);
		this.imsis = imsis;
	}

	@Override
	public void run() {
		LogUtil.info("start calculating group " + group.getGroupId()
				+ " group count: " + imsis.size());
		fetchRedisTable();
		writeToDB();
		LogUtil.info("finish calculating group " + group.getGroupId());
	}

	private void fetchRedisTable() {
		try {
			String filename = group.getGroupId() + "_"
					+ stampToDate(System.currentTimeMillis()) + ".txt";
			FileOutputStream output = new FileOutputStream(filename);
			OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");

			HashMap<String, List<String>> provinceMap = new HashMap<String, List<String>>();

			String para = "aTableRedis";
			ShardedJedis jedis = RedisUtil.getJedis(para);

			ShardedJedisPipeline pipeline = jedis.pipelined();
			for (String imsi : imsis)
				pipeline.get(imsi);

			List<Object> resp = pipeline.syncAndReturnAll();
			RedisUtil.returnJedis(jedis, para);

			for (Object rs : resp) {
				if (rs == null)
					continue;
				String province = ((String) rs).split(",")[3].substring(0, 2);
				if (provinceMap.get(province) == null) {
					List<String> imsiList = new ArrayList<String>();
					imsiList.add(((String) rs).split(",")[0]);
					provinceMap.put(province, imsiList);
				} else
					provinceMap.get(province).add(((String) rs).split(",")[0]);
			}

			for (Map.Entry<String, List<String>> entry : provinceMap.entrySet()) {
				String ipList = ProvinceRedisMap.getProRedisIP(entry.getKey());
				if (ipList == null)
					continue;
				ShardedJedis provJedis = RedisUtil.getJedisByIpList(ipList);
				ShardedJedisPipeline provPipeline = provJedis.pipelined();
				for (String imsi : entry.getValue())
					provPipeline.get(imsi);

				List<Object> provResp = provPipeline.syncAndReturnAll();
				RedisUtil.returnJedis(provJedis, ipList);

				updateDistribution(entry.getKey(), provResp);
				RTPositionFileWriter rt = new RTPositionFileWriter(provResp);
				rt.write(writer);
			}

			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void updateDistribution(String prov, List<Object> resp) {

		ProvincePopulation provPopulation = new ProvincePopulation();
		String provId = prov + "0000";
		provPopulation.setProvinceId(provId);
		provPopulation.setProvinceName(ProvinceCityMap.getProvCity(provId));
		provPopulation.updateDayId();
		provPopulation.updateUpdateTime();
		provPopulation.updateVersion();

		for (Object rs : resp) {
			if (rs == null)
				continue;
			provPopulation.increCount();
			distrib.increTotal();
			if (prov.equals(ConfigUtil.getString("provinceCode")))
				distrib.increInner();
			else
				distrib.increOuter();

			String value = (String) rs;
			String regionCode = value.split(";")[3];
			City city = getCity(regionCode);
			provPopulation.increCityPopulationByCity(city);
		}
		distrib.addProvincePopulation(provPopulation);
	}

	private City getCity(String regionCode) {
		City city = new City();
		if (regionCode.length() != 6)
			return city;
		if (ProvinceCityMap.getProvCity(regionCode) == null) {
			String cityId = regionCode.substring(0, 4) + "00";
			city.setCityId(cityId);
			city.setCityName(ProvinceCityMap.getProvCity(cityId));
		}
		city.setCityId(regionCode);
		city.setCityName(ProvinceCityMap.getProvCity(regionCode));
		return city;
	}

	private void writeToDB() {

		Gson gson = new Gson();
		System.out.println(gson.toJson(distrib));
		// String para = "groupRedis";
		// ShardedJedis jedis = RedisUtil.getJedis(para);
		// jedis.hset("ProvinceDistribution", group.getGroupId(),
		// distrib.toJson());
		//
		// RedisUtil.returnJedis(jedis, para);
	}

	private static String stampToDate(long stamp) {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd-HH");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}
}
