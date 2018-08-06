package cn.ac.iie.jc.group.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.ac.iie.jc.config.ConfigUtil;
import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.group.data.Group;
import cn.ac.iie.jc.group.data.JCPerson;
import cn.ac.iie.jc.thread.ThreadPoolManager;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

public class GroupDistributionTask {

	private static ThreadPoolManager threadPool = ThreadPoolManager.getInstance();
	private HashMap<Group, List<String>> personListMap = new HashMap<Group, List<String>>();

	public void exec() {
		fetchJCPerson();
		groupDistributionCount();
	}

	private void fetchJCPerson() {
		String para1 = "groupRedis";
		String para2 = "msisdnRedis";
		ShardedJedis groupJedis = RedisUtil.getJedis(para1);
		Set<String> groupIds = groupJedis.hkeys(ConfigUtil.getString("provinceName") + "_JCGroup");

		ShardedJedis msisdnJedis = RedisUtil.getJedis(para2);

		for (String groupId : groupIds) {
			String rule = groupJedis.hget(ConfigUtil.getString("provinceName") + "_JCGroup", groupId);
			Group group = Group.newFromJson(rule);
			group.setGroupId(groupId);

			List<String> personJsons = groupJedis.lrange(groupId, 0, -1);
			List<String> imsis = new ArrayList<String>();
			ShardedJedisPipeline msisdnPipeline = msisdnJedis.pipelined();
			for (String personJson : personJsons) {
				JCPerson person = JCPerson.newFromJson(personJson);
				msisdnPipeline.get(person.getPhone());
			}
			List<Object> imsiResp = msisdnPipeline.syncAndReturnAll();

			for (Object imsiRs : imsiResp) {
				if (imsiRs == null)
					continue;
				imsis.add((String) imsiRs);
			}
			personListMap.put(group, imsis);
		}
		RedisUtil.returnJedis(groupJedis, para1);
		RedisUtil.returnJedis(msisdnJedis, para2);
	}

	private void groupDistributionCount() {

		for (Map.Entry<Group, List<String>> entry : personListMap.entrySet()) {
			Group group = entry.getKey();
			List<String> imsis = entry.getValue();
			threadPool.addExecuteTask(new DistributionExecutor(group, imsis));
		}
		threadPool.shutdown();
	}

	public static void main(String[] args) {
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
