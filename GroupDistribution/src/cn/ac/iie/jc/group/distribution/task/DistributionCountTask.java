package cn.ac.iie.jc.group.distribution.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.jc.config.ConfigUtil;
import cn.ac.iie.jc.config.ProvinceMap;
import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.group.data.Distribution;
import cn.ac.iie.jc.group.data.JCPerson;

public class DistributionCountTask implements Runnable {

	private HashMap<String, HashMap<String, Integer>> groupCountMap = new HashMap<String, HashMap<String, Integer>>();
	private HashMap<String, List<String>> personListMap = new HashMap<String, List<String>>();
	private HashMap<String, List<Integer>> populationMap = new HashMap<String, List<Integer>>();

	@Override
	public void run() {
		fetchJCPerson();
		groupDistributionCount();
		populationCalculate();
		writeToRedis();
	}

	private void fetchJCPerson() {
		RedisUtil groupRedis = new RedisUtil("groupRedis");
		ShardedJedis groupJedis = groupRedis.getJedis();
		Set<String> groups = groupJedis.hkeys("JCGroup");

		RedisUtil msisdnRedis = new RedisUtil("msisdnRedis");
		ShardedJedis msisdnJedis = msisdnRedis.getJedis();

		for (String group : groups) {
			List<String> personJsons = groupJedis.lrange(group, 0, -1);
			List<String> imsis = new ArrayList<String>();
			ShardedJedisPipeline msisdnPipeline = msisdnJedis.pipelined();
			for (String personJson : personJsons) {
				JCPerson person = JCPerson.getFromJson(personJson);
				msisdnPipeline.get(person.getPhone());
			}
			List<Object> imsiResp = msisdnPipeline.syncAndReturnAll();

			for (Object imsiRs : imsiResp)
				imsis.add((String) imsiRs);
			personListMap.put(group, imsis);
		}
		groupRedis.returnJedis(groupJedis);
		msisdnRedis.returnJedis(msisdnJedis);
	}

	private void groupDistributionCount() {
		RedisUtil redis = new RedisUtil("aTableRedis");
		ShardedJedis jedis = redis.getJedis();

		for (Map.Entry<String, List<String>> entry : personListMap.entrySet()) {
			String group = entry.getKey();
			if (groupCountMap.get(group) == null) {
				HashMap<String, Integer> countMap = new HashMap<String, Integer>();
				groupCountMap.put(group, countMap);
			}

			ShardedJedisPipeline pipeline = jedis.pipelined();
			HashMap<String, Integer> countMap = groupCountMap.get(group);
			List<String> imsis = personListMap.get(group);
			for (String imsi : imsis)
				pipeline.get(imsi);

			List<Object> resp = pipeline.syncAndReturnAll();
			for (Object rs : resp) {
				String province = ((String) rs).split(",")[7].substring(0, 2);
				Integer originCount = countMap.get(province);
				if (originCount == null) {
					countMap.put(province, 1);
				} else {
					countMap.put(province, originCount + 1);
				}
			}
		}
		redis.returnJedis(jedis);
	}

	private void populationCalculate() {
		for (Map.Entry<String, HashMap<String, Integer>> entry : groupCountMap
				.entrySet()) {
			String group = entry.getKey();
			String province = ConfigUtil.getString("provinceCode");
			int total = 0;
			int inner = 0;
			int outer = 0;
			for (Map.Entry<String, Integer> e : entry.getValue().entrySet()) {
				if (province.equals(e.getKey()))
					inner = inner + e.getValue();
				else
					outer = outer + e.getValue();
				total = total + e.getValue();
			}
			List<Integer> countList = new ArrayList<Integer>();
			countList.add(total);
			countList.add(inner);
			countList.add(outer);
			populationMap.put(group, countList);
		}
	}

	private void writeToRedis() {
		RedisUtil redis = new RedisUtil("groupRedis");
		ShardedJedis jedis = redis.getJedis();

		for (Map.Entry<String, List<Integer>> entry : populationMap.entrySet()) {
			String group = entry.getKey();
			Distribution distrib = new Distribution();
			distrib.setGroupId(group);
			distrib.setTotal(entry.getValue().get(0));
			distrib.setInner(entry.getValue().get(1));
			distrib.setOuter(entry.getValue().get(2));

			for (Map.Entry<String, Integer> e : groupCountMap.get(group)
					.entrySet())
				distrib.addCount(ProvinceMap.getString(e.getKey()),
						e.getValue());

			jedis.hset("ProvinceDistribution", group, distrib.toJson());
		}
	}

	public static void main(String[] args) {
		DistributionCountTask task = new DistributionCountTask();
		task.run();
	}
}
