package cn.ac.iie.jc.group.task;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.ac.iie.jc.config.ConfigUtil;
import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.group.crypt.DataCrypt;
import cn.ac.iie.jc.group.data.Group;
import cn.ac.iie.jc.group.data.IndexToQuery;
import cn.ac.iie.jc.group.data.JCPerson;
import cn.ac.iie.jc.group.data.RTPosition;
import cn.ac.iie.jc.thread.ThreadPoolManager;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

public class GroupDistributionTask {

	private static ThreadPoolManager threadPool = ThreadPoolManager.getInstance();
	private HashMap<Group, HashMap<IndexToQuery, RTPosition>> groupPositionMap = new HashMap<Group, HashMap<IndexToQuery, RTPosition>>();

	public void exec() {
		initGroupPositionMap();
		groupDistribution();
	}

	private void initGroupPositionMap() {

		ShardedJedis groupJedis = RedisUtil.getJedis("groupRedis");

		Set<String> groupIds = groupJedis.hkeys(ConfigUtil.getString("provinceName") + "_JCGroup");

		for (String groupId : groupIds) {
			String rule = groupJedis.hget(ConfigUtil.getString("provinceName") + "_JCGroup", groupId);
			Group group = Group.newFromJson(rule);
			group.setGroupId(groupId);

			HashMap<IndexToQuery, RTPosition> positionMap = generatePositionMap(groupJedis, group);
			groupPositionMap.put(group, positionMap);
		}

		RedisUtil.returnJedis(groupJedis, "groupRedis");
	}

	private HashMap<IndexToQuery, RTPosition> generatePositionMap(ShardedJedis groupJedis, Group group) {

		ShardedJedis msisdnJedis = RedisUtil.getJedis("msisdnRedis");
		ShardedJedisPipeline msisdnPipeline = msisdnJedis.pipelined();

		List<String> personJsons = groupJedis.lrange(group.getGroupId(), 0, -1);

		HashMap<IndexToQuery, RTPosition> positionMap = new HashMap<IndexToQuery, RTPosition>();

		for (String personJson : personJsons) {
			JCPerson person = JCPerson.newFromJson(personJson);
			msisdnPipeline.get(person.getPhone());
		}

		List<Object> imsiResp = msisdnPipeline.syncAndReturnAll();

		Iterator<Object> iter = imsiResp.iterator();
		int count = 0;
		while (iter.hasNext()) {
			String personJson = personJsons.get(count++);
			JCPerson person = JCPerson.newFromJson(personJson);
			String msisdn = person.getPhone();
			RTPosition position = new RTPosition();
			position.setSource(group.getSource());
			position.setGroupid(group.getGroupId());
			position.setGroupname(group.getGroupName());
			position.setMsisdn(msisdn);
			
			String value = (String) iter.next();
			if (value == null) {
				position.setStatus(6);
				positionMap.put(new IndexToQuery(msisdn), position);
				continue;
			}
			position.setImsi(value);
			position.setStatus(0);
			positionMap.put(new IndexToQuery(msisdn, value), position);
		}

		RedisUtil.returnJedis(msisdnJedis, "msisdnRedis");

		return positionMap;
	}

	private void groupDistribution() {

		for (Map.Entry<Group, HashMap<IndexToQuery, RTPosition>> entry : groupPositionMap.entrySet()) {
			Group group = entry.getKey();
			HashMap<IndexToQuery, RTPosition> resultMap = entry.getValue();
			threadPool.addExecuteTask(new DistributionExecutor(group, resultMap));
		}
		threadPool.shutdown();
	}

	public static void main(String[] args) {
		try {
			DataCrypt.auth("jm.conf");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		GroupDistributionTask task = new GroupDistributionTask();
		task.exec();

		while (!threadPool.isTaskEnd()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		DistributionExecutor.writeWholeToDB();
		System.exit(0);
	}
}
