package cn.ac.iie.jc.group.task;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.group.data.Group;
import cn.ac.iie.jc.group.data.RTPosition;

public class RTPositionFileEexcutor implements Runnable {

	private Group group;
	private List<Object> aTableContents;
	private HashMap<String, String> uliMap = new HashMap<String, String>();

	public RTPositionFileEexcutor(Group group, List<Object> aTableContents) {
		this.group = group;
		this.aTableContents = aTableContents;
	}

	@Override
	public void run() {
		try {
			String filename = group.getGroupId()
					+ stampToDate(System.currentTimeMillis()) + ".txt";
			FileOutputStream output = new FileOutputStream(filename);
			OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");

			String para = "uliRedis";
			ShardedJedis jedis = RedisUtil.getJedis(para);
			ShardedJedisPipeline pipeline = jedis.pipelined();

			fetchUliAddress(aTableContents, pipeline);
			RedisUtil.returnJedis(jedis, para);

			for (Object aTableContent : aTableContents) {
				RTPosition position = getRTPosition((String) aTableContent);
				writer.write(position.toString() + "\n");
			}
			writer.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private RTPosition getRTPosition(String aTableContent) {
		RTPosition position = new RTPosition();
		position.setImsi(((String) aTableContent).split(",")[0]);
		position.setImei(((String) aTableContent).split(",")[1]);
		position.setMsisdn(((String) aTableContent).split(",")[2]);
		position.setTime(stringStampToDate(((String) aTableContent).split(",")[10]));

		String uli = ((String) aTableContent).split(",")[6];
		position.setProvinceId(uliMap.get(uli).split(",")[3]);
		position.setProvinceName("testProvinceName");
		position.setCityId(uliMap.get(uli).split(",")[4]);
		position.setCityName("testCityName");
		position.setAreaId(uliMap.get(uli).split(",")[5]);
		position.setAreaName("testAreaName");
		position.setAddress(uliMap.get(uli).split(",")[7]);

		return position;

	}

	private void fetchUliAddress(List<Object> aTableContents,
			ShardedJedisPipeline pipeline) {

		for (Object aTableContent : aTableContents)
			pipeline.get(((String) aTableContent).split(",")[6]);
		List<Object> resp = pipeline.syncAndReturnAll();

		for (Object rs : resp) {
			String uli = ((String) rs).split(",")[0];
			uliMap.put(uli, (String) rs);
		}
	}

	private static String stampToDate(long stamp) {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd-HH-mm-ss");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}

	private static String stringStampToDate(String str) {

		long stamp = Long.parseLong(str) * 1000;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}

}
