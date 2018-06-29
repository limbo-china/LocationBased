package cn.ac.iie.hy.datatrains.handler;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerWithAvro {
//	public static void main(String[] args) {
//
//		Properties props = new Properties();
//		props.put("bootstrap.servers", "10.250.82.32:9092");
////		props.put("group.id", "test_0329_1507");
//		props.put("group.id", "distKa-2");
//		props.put("enable.auto.commit", "true");
//		props.put("auto.commit.interval.ms", "1000");
//		props.put("session.timeout.ms", "30000");
//		props.put("auto.offset.reset", "earliest");
//		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
//		consumer.subscribe(Arrays.asList("t_cdr"));
//		try {  
//			Schema avroschema = new Schema.Parser().parse(new File("t_cdr.json"));
//			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(avroschema);
//			BinaryDecoder defaultDecoder = null;
//			GenericRecord gRecord = new GenericData.Record(avroschema);
//
//			while (true) {
//				ConsumerRecords<String, byte[]> records = consumer.poll(100);
//				for (ConsumerRecord<String, byte[]> record : records) {
//					defaultDecoder = DecoderFactory.get().binaryDecoder(record.value(), defaultDecoder);
//					while (!defaultDecoder.isEnd()) {
//						datumReader.read(gRecord, defaultDecoder);
//						String imsi = gRecord.get("c_imsi")==null ? "Defalt" : gRecord.get("c_imsi").toString();
//						String imei = gRecord.get("c_imei")==null ? "Defalt" : gRecord.get("c_imei").toString();
//						System.out.println("c_imsi is " + imsi + ", c_imei is " + imei);
//					}
//					System.out.println("next iterm");
//				}
//			}
//		} catch (Exception e1) {
//			e1.printStackTrace();
//		}
//	}

}
