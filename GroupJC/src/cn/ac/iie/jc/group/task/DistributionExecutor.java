package cn.ac.iie.jc.group.task;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.group.data.Distribution;
import cn.ac.iie.jc.group.data.Group;
import cn.ac.iie.jc.thread.ThreadPoolManager;

public class DistributionExecutor implements Runnable {

	private static ThreadPoolManager threadPool = ThreadPoolManager
			.getInstance();
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

		calculate();
		writeToRedis();
	}

	private void calculate() {
		String para = "aTableRedis";
		ShardedJedis jedis = RedisUtil.getJedis(para);

		ShardedJedisPipeline pipeline = jedis.pipelined();
		for (String imsi : imsis)
			pipeline.get(imsi);

		List<Object> resp = pipeline.syncAndReturnAll();
		RedisUtil.returnJedis(jedis, para);

		threadPool.addExecuteTask(new RTPositionFileEexcutor(group, resp));

		for (Object rs : resp) {
			String province = ((String) rs).split(",")[7].substring(0, 2);
			if (!distrib.hasProvince(province))
				distrib.initProvince(province);
			else
				distrib.populationAccumulate(province);
		}
		distrib.setUpdateBy("testUpdateBy");
		distrib.setUpdateTime(stampToDate(System.currentTimeMillis()));

	}

	private void writeToRedis() {
		String para = "groupRedis";
		ShardedJedis jedis = RedisUtil.getJedis(para);
		jedis.hset("ProvinceDistribution", group.getGroupId(), distrib.toJson());

		RedisUtil.returnJedis(jedis, para);
	}

	private static String stampToDate(long stamp) {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}
}
