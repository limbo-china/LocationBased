package cn.ac.iie.jc.group.task;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import com.scistor.softcrypto.SoftCrypto;

import cn.ac.iie.jc.group.crypt.CryptData;
import cn.ac.iie.jc.group.crypt.DataCrypt;
import cn.ac.iie.jc.avro.Serializing;
import cn.ac.iie.jc.config.ConfigUtil;
import cn.ac.iie.jc.config.ProvinceCityMap;
import cn.ac.iie.jc.config.ProvinceRedisMap;
import cn.ac.iie.jc.db.RedisUtil;
import cn.ac.iie.jc.db.XClusterDataFetch;
import cn.ac.iie.jc.group.data.City;
import cn.ac.iie.jc.group.data.CityPopulation;
import cn.ac.iie.jc.group.data.Distribution;
import cn.ac.iie.jc.group.data.Group;
import cn.ac.iie.jc.group.data.IndexToQuery;
import cn.ac.iie.jc.group.data.ProvincePopulation;
import cn.ac.iie.jc.group.data.RTPosition;
import cn.ac.iie.jc.log.LogUtil;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

public class DistributionExecutor implements Runnable {

	private static SoftCrypto crypt = new SoftCrypto();

	static {
		crypt.Initialize("abc");
	}

	private static Group group0 = new Group(ConfigUtil.getString("provinceName") + "_0000000000");
	private static Distribution distrib0 = new Distribution(group0);

	// private static OutputStreamWriter wholeWriter;
	// static {
	// String filename = ConfigUtil.getString("filePath") +
	// ConfigUtil.getString("provinceName") + "/"
	// + group0.getGroupId() + "_" + stampToDate(System.currentTimeMillis()) +
	// ".csv";
	// FileOutputStream output;
	// try {
	// output = new FileOutputStream(filename);
	// wholeWriter = new OutputStreamWriter(output, "UTF-8");
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }

	private Group group;
	private Distribution distrib;
	private HashMap<IndexToQuery, RTPosition> positionMap;

	public DistributionExecutor(Group group, HashMap<IndexToQuery, RTPosition> positionMap) {

		this.group = group;
		this.distrib = new Distribution(group);
		this.positionMap = positionMap;
	}

	@Override
	public void run() {
		LogUtil.info("start calculating group " + group.getGroupId() + " group count: " + positionMap.size());
		fetchRedisTable();
		
		new XClusterDataFetch();
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

			for (Map.Entry<IndexToQuery, RTPosition> entry : positionMap.entrySet()) {
				IndexToQuery index = entry.getKey();
				if (!index.getImsi().equals("")) {
					String value = jedis.get(index.getImsi());
					if (value == null) {
						positionMap.get(new IndexToQuery("", index.getImsi())).setStatus(7);
						continue;
					}
					String province = value.split(",")[3].substring(0, 2);
					if (provinceMap.get(province) == null) {
						List<String> imsiList = new ArrayList<String>();
						imsiList.add(value.split(",")[0]);
						provinceMap.put(province, imsiList);
					} else
						provinceMap.get(province).add(value.split(",")[0]);
				}
			}

			RedisUtil.returnJedis(jedis, para);

			for (Map.Entry<String, List<String>> entry : provinceMap.entrySet()) {
				String ipList = ProvinceRedisMap.getProRedisIP(entry.getKey());
				if (ipList == null)
					continue;
				ShardedJedis provJedis = RedisUtil.getJedisByIpList(ipList);
				ShardedJedisPipeline provPipeline = provJedis.pipelined();

				for (String imsi : entry.getValue()) {
					if (isJMProvince(ipList)) {
						if (imsi != null && imsi.length() == 15) {
							byte[] datain = imsi.substring(3, 15).getBytes();
							byte[] dataout = new byte[datain.length];
							crypt.crypto_encrypt(datain, dataout, datain.length, 1, 0);
							String t = new String(dataout);
							imsi = imsi.substring(0, 3) + t;
						}
					}
					provPipeline.get(imsi);
				}

				List<Object> provResp = provPipeline.syncAndReturnAll();
				RedisUtil.returnJedis(provJedis, ipList);

				fillPositionMap(entry.getValue(), provResp, ipList);
				updateDistribution(entry.getKey(), provResp);
//				 RTPositionFileWriter rt = new RTPositionFileWriter(provResp);
//				 rt.write(writer/* , wholeWriter */);
			}

			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private boolean isJMProvince(String ipList){
		return ipList.startsWith("10.245") || ipList.startsWith("10.233");
	}
	private void fillPositionMap(List<String> imsiList, List<Object> provResp, String ipList) {

		HashMap<String, String> uliMap = new HashMap<String, String>();
		ShardedJedis jedis = RedisUtil.getJedis("uliRedis");
		ShardedJedisPipeline pipeline = jedis.pipelined();
		fetchUliAddress(provResp, pipeline, uliMap);
		RedisUtil.returnJedis(jedis, "uliRedis");

		int count = 0;
		Iterator<Object> iter = provResp.iterator();
		while (iter.hasNext()) {
			String imsi = imsiList.get(count++);
			RTPosition position = positionMap.get(new IndexToQuery("", imsi));

			String value = (String) iter.next();
			if (value == null) {
				position.setStatus(7);
				continue;
			} else
				setRTPosition(value, position, uliMap, ipList);
		}
	}

	private void setRTPosition(String cTableContent, RTPosition position, HashMap<String, String> uliMap, String ipList) {

		CryptData cd = new CryptData();
		cd.setImsi(cTableContent.split(";")[0]);
		cd.setImei(cTableContent.split(";")[1]);
		cd.setMsisdn(cTableContent.split(";")[2]);

		if (isJMProvince(ipList)) {
			int dedataout_result = 0;

			dedataout_result = cd.decryptData();
			while (-1 == dedataout_result) {
				LogUtil.info("Decrypt ticket time out!");
				try {
					DataCrypt.auth("jm.conf");
				} catch (IOException e) {
					e.printStackTrace();
				}
				dedataout_result = cd.decryptData();
			}
		}
		position.setImei(cd.getImei());
		position.setRegionCode(cTableContent.split(";")[3]);

		if (cTableContent.split(";").length > 10)
			position.setTime(stringStampToDate(cTableContent.split(";")[10]));

		String uli = cTableContent.split(";")[6];
		if (uli == null || uli.equals("0") || uli.equals("")) {
			position.setStatus(8);
		} else {
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
			} else
				position.setStatus(9);
		}
	}

	private void fetchUliAddress(List<Object> cTableContents, ShardedJedisPipeline pipeline,
			HashMap<String, String> uliMap) {

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

	private void updateDistribution(String prov, List<Object> resp) {

		ShardedJedis jedis = RedisUtil.getJedis("uliRedis");

		ProvincePopulation provPopulation = new ProvincePopulation();
		String provId = prov + "0000";
		provPopulation.setProvinceId(provId);
		provPopulation.setProvinceName(ProvinceCityMap.getProvCity(provId));
		provPopulation.updateDayId();
		provPopulation.updateUpdateTime();
		provPopulation.updateVersion();

		distrib.addProvincePopulation(provPopulation, false);

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
			String uli = value.split(";")[6];
			City city = getCity(regionCode, uli, jedis);
			provPopulation.increCityPopulationByCity(city);
		}

		distrib0.addProvincePopulation(provPopulation, true);

		RedisUtil.returnJedis(jedis, "uliRedis");
	}

	private City getCity(String regionCode, String uli, ShardedJedis jedis) {
		City city = new City();
		if (uli == null || uli.equals(""))
			city.setCityId(regionCode);
		else {
			String value = (String) jedis.get(uli);
			if (value == null || value.split(",").length <= 8)
				city.setCityId(regionCode);
			else
				city.setCityId(value.split(",")[8]);
		}

		if (city.getCityId().length() != 6)
			return city;

		String cityId = city.getCityId();
		if (ProvinceCityMap.getProvCity(cityId) == null)
			cityId = cityId.substring(0, 4) + "00";
		city.setCityId(cityId);
		String cityName = ProvinceCityMap.getProvCity(cityId);
		if (cityName == null)
			city.setCityId("0");
		else
			city.setCityName(cityName);
		return city;
	}

	private void writeToDB() {

		LogUtil.info("writing group " + group.getGroupId() + " to redis db");

		ShardedJedis groupJedis = RedisUtil.getJedis("groupRedis");

		groupJedis.hset("PopulationStatics", group.getGroupId(), distrib.getAggregate().toJson());

		RedisUtil.returnJedis(groupJedis, "groupRedis");

		LogUtil.info("writing group " + group.getGroupId() + " to st db");
		for (Map.Entry<String, ProvincePopulation> entry : distrib.getProvinceDisribution().entrySet()) {
			ProvincePopulation provincePopulation = entry.getValue();
			sendProvData(provincePopulation);
			for (Map.Entry<String, CityPopulation> cityentry : provincePopulation.getCityDistribution().entrySet())
				sendCityData(cityentry.getValue());
		}

		for (Map.Entry<IndexToQuery, RTPosition> entry : positionMap.entrySet())	
			sendRTPosition(entry.getValue());
		LogUtil.info("write position finished.");
	}

	public static void writeWholeToDB() {
		LogUtil.info("writing group " + group0.getGroupId() + " to db");

		ShardedJedis groupJedis = RedisUtil.getJedis("groupRedis");

		groupJedis.hset("PopulationStatics", group0.getGroupId(), distrib0.getAggregate().toJson());

		RedisUtil.returnJedis(groupJedis, "groupRedis");

		LogUtil.info("writing group " + group0.getGroupId() + " to st db");
		for (Map.Entry<String, ProvincePopulation> entry : distrib0.getProvinceDisribution().entrySet()) {
			ProvincePopulation provincePopulation = entry.getValue();
			provincePopulation.setGroupId(group0.getGroupId());
			sendProvData(provincePopulation);
			for (Map.Entry<String, CityPopulation> cityentry : provincePopulation.getCityDistribution().entrySet()) {
				cityentry.getValue().setGroupId(group0.getGroupId());
				sendCityData(cityentry.getValue());
			}
		}
	}

	private static void sendProvData(ProvincePopulation popu) {
		HttpClient httpClient = new DefaultHttpClient();
		Serializing serialize = Serializing.getInstance();
		try {
			byte[] data = serialize.serializeProvPopuToBytes(popu);
			if (data != null) {
				HttpPost httppost = new HttpPost(ConfigUtil.getString("loadUrl"));
				httppost.addHeader("content-type", "utf-8");
				httppost.addHeader("User", "iie");
				httppost.addHeader("Password", "123456");
				httppost.addHeader("Topic", ConfigUtil.getString("provinceTopicName"));
				httppost.addHeader("Format", "avro");

				InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), data.length);
				reqEntity.setContentType("binary/octet-stream");
				httppost.setEntity(reqEntity);

				HttpResponse response = httpClient.execute(httppost);
				LogUtil.info("Prov" + "send " + popu.getGroupId() + " " + popu.getProvinceName() + " "
						+ response.getStatusLine());
				httppost.releaseConnection();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}
	}

	private static void sendCityData(CityPopulation popu) {
		HttpClient httpClient = new DefaultHttpClient();
		Serializing serialize = Serializing.getInstance();
		try {
			byte[] data = serialize.serializeCityPopuToBytes(popu);
			if (data != null) {
				HttpPost httppost = new HttpPost(ConfigUtil.getString("loadUrl"));
				httppost.addHeader("content-type", "utf-8");
				httppost.addHeader("User", "iie");
				httppost.addHeader("Password", "123456");
				httppost.addHeader("Topic", ConfigUtil.getString("cityTopicName"));
				httppost.addHeader("Format", "avro");
				InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), data.length);
				reqEntity.setContentType("binary/octet-stream");
				httppost.setEntity(reqEntity);

				HttpResponse response = httpClient.execute(httppost);
				LogUtil.info("City" + "send " + popu.getGroupId() + " " + popu.getProvinceName() + " "
						+ popu.getCityName() + " " + response.getStatusLine());
				httppost.releaseConnection();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}
	}

	private void sendRTPosition(RTPosition position) {
		HttpClient httpClient = new DefaultHttpClient();
		Serializing serialize = Serializing.getInstance();
		try {
			byte[] data = serialize.serializeRTPositionToBytes(position);
			if (data != null) {
				HttpPost httppost = new HttpPost(ConfigUtil.getString("loadUrl"));
				httppost.addHeader("content-type", "utf-8");
				httppost.addHeader("User", "iie");
				httppost.addHeader("Password", "123456");
				httppost.addHeader("Topic", ConfigUtil.getString("positionTopicName"));
				httppost.addHeader("Format", "avro");
				InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), data.length);
				reqEntity.setContentType("binary/octet-stream");
				httppost.setEntity(reqEntity);

				HttpResponse response = httpClient.execute(httppost);
				response.getStatusLine();
				httppost.releaseConnection();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}
	}

	private static String stampToDate(long stamp) {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
		Date date = new Date(stamp);
		return simpleDateFormat.format(date);
	}
}
