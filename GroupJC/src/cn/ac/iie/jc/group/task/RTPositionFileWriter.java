package cn.ac.iie.jc.group.task;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.group.data.RTPosition;

public class RTPositionFileWriter {

	private List<Object> cTableContents;
	private HashMap<String, String> uliMap = new HashMap<String, String>();

	public RTPositionFileWriter(List<Object> cTableContents) {
		this.cTableContents = cTableContents;
	}

	public void write(OutputStreamWriter writer) throws IOException {

		String para = "uliRedis";
		ShardedJedis jedis = RedisUtil.getJedis(para);
		ShardedJedisPipeline pipeline = jedis.pipelined();

		fetchUliAddress(cTableContents, pipeline);
		RedisUtil.returnJedis(jedis, para);

		for (Object cTableContent : cTableContents) {
			if (cTableContent == null)
				continue;
			RTPosition position = getRTPosition((String) cTableContent);
			writer.write(position.toString() + "\n");
		}

	}

	private RTPosition getRTPosition(String cTableContent) {
		RTPosition position = new RTPosition();
		position.setImsi(((String) cTableContent).split(";")[0]);
		position.setImei(((String) cTableContent).split(";")[1]);
		position.setMsisdn(((String) cTableContent).split(";")[2]);
		position.setTime(stringStampToDate(((String) cTableContent).split(";")[10]));

		String uli = ((String) cTableContent).split(";")[6];
		position.setProvinceId(uliMap.get(uli).split(",")[3]);
		position.setProvinceName("testProvinceName");
		position.setCityId(uliMap.get(uli).split(",")[4]);
		position.setCityName("testCityName");
		position.setAreaId(uliMap.get(uli).split(",")[5]);
		position.setAreaName("testAreaName");
		position.setAddress(uliMap.get(uli).split(",")[7]);

		return position;

	}

	private void fetchUliAddress(List<Object> cTableContents,
			ShardedJedisPipeline pipeline) {

		for (Object cTableContent : cTableContents) {
			if (cTableContent == null)
				continue;
			pipeline.get(((String) cTableContent).split(";")[6]);
		}
		List<Object> resp = pipeline.syncAndReturnAll();

		for (Object rs : resp) {
			String uli = ((String) rs).split(",")[0];
			uliMap.put(uli, (String) rs);
		}
	}

	private static String stringStampToDate(String str) {

		long stamp = Long.parseLong(str) * 1000;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}

}
