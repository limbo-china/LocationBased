package cn.ac.iie.jc.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import cn.ac.iie.jc.config.ConfigUtil;
import cn.ac.iie.jc.group.data.CityPopulation;
import cn.ac.iie.jc.group.data.ProvincePopulation;
import cn.ac.iie.jc.group.data.RTPosition;
import cn.ac.iie.jc.log.LogUtil;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class Serializing {

	private static Schema provinceSchema = null;
	private static Schema citySchema = null;
	private static Schema positionSchema = null;
	private static DatumWriter<GenericRecord> provinceWriter = null;
	private static DatumWriter<GenericRecord> cityWriter = null;
	private static DatumWriter<GenericRecord> positionWriter = null;
	private static final Serializing INSTANCE = new Serializing();

	private JsonAvroConverter converter = new JsonAvroConverter();

	static {
		try {
			CachedSchemaRegistryClient client;
			client = new CachedSchemaRegistryClient(ConfigUtil.getString("schemaUrl"), 100);
			SchemaMetadata prov = client.getLatestSchemaMetadata(ConfigUtil.getString("provinceTopicName"));
			provinceSchema = new Schema.Parser().parse(prov.getSchema());
			SchemaMetadata city = client.getLatestSchemaMetadata(ConfigUtil.getString("cityTopicName"));
			citySchema = new Schema.Parser().parse(city.getSchema());
			SchemaMetadata position = client.getLatestSchemaMetadata(ConfigUtil.getString("positionTopicName"));
			positionSchema = new Schema.Parser().parse(position.getSchema());
			LogUtil.info(positionSchema.toString());

			provinceWriter = new GenericDatumWriter<GenericRecord>(provinceSchema);
			cityWriter = new GenericDatumWriter<GenericRecord>(citySchema);
			positionWriter = new GenericDatumWriter<GenericRecord>(positionSchema);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Serializing() {
	}

	public static Serializing getInstance() {
		return INSTANCE;
	}

	public byte[] serializeProvPopuToBytes(ProvincePopulation popu) throws IOException {
		ByteArrayOutputStream contentOutput = new ByteArrayOutputStream();
		BinaryEncoder contentEncoder = new EncoderFactory().binaryEncoder(contentOutput, null);
		provinceWriter.write(provToGenericRecord(popu), contentEncoder);
		contentEncoder.flush();

		return contentOutput.toByteArray();
	}

	public GenericRecord provToGenericRecord(ProvincePopulation popu) {
		return converter.convertToGenericDataRecord(popu.toDBJson().getBytes(), provinceSchema);
	}

	public byte[] serializeCityPopuToBytes(CityPopulation popu) throws IOException {
		ByteArrayOutputStream contentOutput = new ByteArrayOutputStream();
		BinaryEncoder contentEncoder = new EncoderFactory().binaryEncoder(contentOutput, null);
		cityWriter.write(cityToGenericRecord(popu), contentEncoder);
		contentEncoder.flush();

		return contentOutput.toByteArray();
	}

	private GenericRecord cityToGenericRecord(CityPopulation popu) {
		return converter.convertToGenericDataRecord(popu.toJson().getBytes(), citySchema);
	}

	public byte[] serializeRTPositionToBytes(RTPosition position) throws IOException {
		ByteArrayOutputStream contentOutput = new ByteArrayOutputStream();
		BinaryEncoder contentEncoder = new EncoderFactory().binaryEncoder(contentOutput, null);
		positionWriter.write(positionToGenericRecord(position), contentEncoder);
		contentEncoder.flush();

		return contentOutput.toByteArray();
	}

	private GenericRecord positionToGenericRecord(RTPosition position) {
		return converter.convertToGenericDataRecord(position.toJson().getBytes(), positionSchema);
	}
}
