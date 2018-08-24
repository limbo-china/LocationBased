package cn.ac.iie.jc.group.task;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.group.data.RTPosition;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

public class RTPositionFileWriter {

	private List<Object> cTableContents;
	private HashMap<String, String> uliMap = new HashMap<String, String>();

	public RTPositionFileWriter(List<Object> cTableContents) {
		this.cTableContents = cTableContents;
	}

	public void write(
			OutputStreamWriter writer/* , OutputStreamWriter wholeWriter */) throws IOException {

		String para = "uliRedis";
		ShardedJedis jedis = RedisUtil.getJedis(para);
		ShardedJedisPipeline pipeline = jedis.pipelined();

		fetchUliAddress(cTableContents, pipeline);
		RedisUtil.returnJedis(jedis, para);

		for (Object cTableContent : cTableContents) {
			if (cTableContent == null)
				continue;
			RTPosition position = getRTPosition((String) cTableContent);
			writer.write(position.toFileString() + "\n");
			// synchronized (wholeWriter) {
			// wholeWriter.write(position.toString() + "\n");
			// }
		}

	}

	private RTPosition getRTPosition(String cTableContent) {
		RTPosition position = new RTPosition();
		position.setImsi(((String) cTableContent).split(";")[0]);
		position.setImei(((String) cTableContent).split(";")[1]);
		position.setMsisdn(((String) cTableContent).split(";")[2]);
		position.setRegionCode(((String) cTableContent).split(";")[3]);

		if (((String) cTableContent).split(";").length > 10)
			position.setTime(stringStampToDate(((String) cTableContent).split(";")[10]));

		String uli = ((String) cTableContent).split(";")[6];
		position.setUli(uli);
		String gis = uliMap.get(uli);
		if (gis != null && gis.split(",").length > 7) {
			position.setLngi(Double.parseDouble((gis.split(",")[1])));
			position.setLati(Double.parseDouble((gis.split(",")[2])));
			position.setProvince(gis.split(",")[3]);
			position.setCity(gis.split(",")[4]);
			position.setDistrict(gis.split(",")[5]);
			position.setBaseinfo(gis.split(",")[7]);
			if (gis.split(",").length > 8)
				position.setRegionCode(gis.split(",")[8]);
		}

		return position;

	}

	private void fetchUliAddress(List<Object> cTableContents, ShardedJedisPipeline pipeline) {

		for (Object cTableContent : cTableContents) {
			if (cTableContent == null)
				continue;
			pipeline.get(((String) cTableContent).split(";")[6]);
		}
		List<Object> resp = pipeline.syncAndReturnAll();

		for (Object rs : resp) {
			if (rs == null)
				continue;
			String uli = ((String) rs).split(",")[0];
			uliMap.put(uli, (String) rs);
		}
	}

	private static String stringStampToDate(String str) {

		long stamp = Long.parseLong(str) * 1000;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}

}
