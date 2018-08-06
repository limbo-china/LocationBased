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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class Serializing {

	private static Schema provinceSchema = null;
	private static Schema citySchema = null;
	private static DatumWriter<GenericRecord> provinceWriter = null;
	private static DatumWriter<GenericRecord> cityWriter = null;
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

			provinceWriter = new GenericDatumWriter<GenericRecord>(provinceSchema);
			cityWriter = new GenericDatumWriter<GenericRecord>(citySchema);
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
}
