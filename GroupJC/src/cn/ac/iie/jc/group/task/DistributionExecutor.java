package cn.ac.iie.jc.group.task;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import cn.ac.iie.jc.config.ConfigUtil;
import cn.ac.iie.jc.config.ProvinceCityMap;
import cn.ac.iie.jc.config.ProvinceRedisMap;
import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.group.data.City;
import cn.ac.iie.jc.group.data.Distribution;
import cn.ac.iie.jc.group.data.Group;
import cn.ac.iie.jc.group.data.ProvincePopulation;
import cn.ac.iie.jc.log.LogUtil;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

public class DistributionExecutor implements Runnable {

	private static Group group0 = new Group(ConfigUtil.getString("provinceName") + "_0000000000");
	private static Distribution distrib0 = new Distribution(group0);

	private static OutputStreamWriter wholeWriter;
	static {
		String filename = ConfigUtil.getString("filePath") + ConfigUtil.getString("provinceName") + "/"
				+ group0.getGroupId() + "_" + stampToDate(System.currentTimeMillis()) + ".csv";
		FileOutputStream output;
		try {
			output = new FileOutputStream(filename);
			wholeWriter = new OutputStreamWriter(output, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

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
		LogUtil.info("start calculating group " + group.getGroupId() + " group count: " + imsis.size());
		fetchRedisTable();
		writeToDB();
		LogUtil.info("finish group " + group.getGroupId());
	}

	private void fetchRedisTable() {
		try {
			String filename = ConfigUtil.getString("filePath") + ConfigUtil.getString("provinceName") + "/"
					+ group.getGroupId() + "_" + stampToDate(System.currentTimeMillis()) + ".csv";
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
				rt.write(writer, wholeWriter);
			}

			// Gson gson = new Gson();
			// writer.write(gson.toJson(distrib) + "\n");
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
			distrib0.increTotal();
			distrib.increTotal();
			if (prov.equals(ConfigUtil.getString("provinceCode"))) {
				distrib.increInner();
				distrib0.increInner();
			} else {
				distrib.increOuter();
				distrib0.increOuter();
			}

			String value = (String) rs;
			String regionCode = value.split(";")[3];
			City city = getCity(regionCode);
			provPopulation.increCityPopulationByCity(city);
		}
		distrib.addProvincePopulation(provPopulation);
		distrib0.addProvincePopulation(provPopulation);
	}

	private City getCity(String regionCode) {
		City city = new City();
		String cityId = regionCode;
		if (regionCode.length() != 6)
			return city;
		if (ProvinceCityMap.getProvCity(cityId) == null)
			cityId = cityId.substring(0, 4) + "00";
		city.setCityId(cityId);
		city.setCityName(ProvinceCityMap.getProvCity(cityId));
		return city;
	}

	private void writeToDB() {

		LogUtil.info("writing group " + group.getGroupId() + " to db");

		ShardedJedis groupJedis = RedisUtil.getJedis("groupRedis");

		groupJedis.hset("PopulationStatics", group.getGroupId(), distrib.getAggregate().toJson());

		RedisUtil.returnJedis(groupJedis, "groupRedis");
	}

	public static void writeWholeToDB() {
		Gson gson = new Gson();
		LogUtil.info(gson.toJson(distrib0));
		LogUtil.info("writing group " + group0.getGroupId() + " to db");

		ShardedJedis groupJedis = RedisUtil.getJedis("groupRedis");

		groupJedis.hset("PopulationStatics", group0.getGroupId(), distrib0.getAggregate().toJson());

		RedisUtil.returnJedis(groupJedis, "groupRedis");
	}

	private static String stampToDate(long stamp) {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}
}
