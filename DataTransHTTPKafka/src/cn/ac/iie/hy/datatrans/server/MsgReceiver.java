package cn.ac.iie.hy.datatrans.server;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ac.iie.hy.datatrains.handler.DataDispatcher;
import cn.ac.iie.hy.datatrains.metadata.SMetaData;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by siyu on 2017/5/22.
 */
public class MsgReceiver implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MsgReceiver.class);
    //private BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> commitQueue = new LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>>();
    public static Properties consumerConfig;
    private String topicName;
    
    private static File file = null;
    private static FileWriter fileWritter = null;
    private static BufferedWriter bufferWritter = null;
    
    
    static {	
    	try{
    		file = new File("msisdn" +".txt");
    		fileWritter = new FileWriter(file.getName(),true);
    		bufferWritter = new BufferedWriter(fileWritter);
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
    /**
     *
     * @param consumerConfig
     * @param topicName
     */
    public MsgReceiver(Properties consumerConfig, String topicName) {

        this.consumerConfig = consumerConfig;
        this.topicName = topicName;
    }

    public void run(){
        //kafka Consumer是非线程安全�?�?���?��每个线程建立�?��consumer
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(consumerConfig);
        //读取指定的partition
       /* List<TopicPartition> partitionsList = new ArrayList<TopicPartition>();
        // 消费编号为startPartitionNo-endPartitionNo的partition
        int startPartitionNo = Integer.parseInt(consumerConfig.getProperty("partition.start.no"));
        int endPartitionNo = Integer.parseInt(consumerConfig.getProperty("partition.end.no"));

        for(int i=startPartitionNo; i<=endPartitionNo; ++i){
            partitionsList.add(new TopicPartition(topicName,i));
        }
        consumer.assign(partitionsList); //读取指定分区
*/        consumer.subscribe(Arrays.asList(topicName));
        //�?��线程中断标志是否设置, 如果设置则表示外界想要停止该任务,终止该任�?
        //Producer<String, byte[]> producer = PerProducer.getInstance(producerConfig).producer;
        
        Schema schema;
		try {
			schema = new Schema.Parser().parse(new File(consumerConfig.getProperty("schema.cdr")));
			
			while (!Thread.currentThread().isInterrupted()) {
                try {
                    //查看该消费�?是否有需要提交的偏移信息, 使用非阻塞读�?
                    //�?��轮询100ms
                    ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                    
                    if(records.count()!=0){
                    	logger.info("==============poll records size: " + records.count()+"==============");

                    	//List<SMetaData> al = new ArrayList<SMetaData>();
                        for (final ConsumerRecord<String, byte[]> record : records) {
                       
                            DatumReader<GenericRecord> datumReader = new GenericDatumReader(schema);

                            ByteArrayInputStream bis = new ByteArrayInputStream(record.value());
                            BinaryDecoder defaultDecoder = new DecoderFactory().binaryDecoder(bis, null);
                            
                            List<SMetaData> al = new ArrayList<SMetaData>();
                            //每个record�?0000条数�?
                            //List<GenericRecord> gRecordsArr = new ArrayList<GenericRecord>();
                
                            try {
                                GenericRecord genericRecord = new GenericData.Record(schema);
                                while (!defaultDecoder.isEnd()) {
                                    //GenericRecord genericRecord = new GenericData.Record(schema);
                                	
                                    try {
                                    	  datumReader.read(genericRecord, defaultDecoder);
                                        
                                        //处理逻辑
                                        //count++;                                    
                                        //logger.info("INFO:==data info==: " + genericRecord.get("sdn").toString()+"==============");
                                        SMetaData metaLog = new SMetaData();
                						
                						metaLog.setTimestamp(genericRecord.get("c_timestamp")==null ? 0 : Long.parseLong(genericRecord.get("c_timestamp").toString()));
                						metaLog.setCdrType(genericRecord.get("c_cdrtype")==null ? 999 : Integer.parseInt(genericRecord.get("c_cdrtype").toString()));
                						metaLog.setMsisdn(genericRecord.get("c_usernum")==null ? "Defalt" : genericRecord.get("c_usernum").toString());
                						metaLog.setRelateNum(genericRecord.get("c_relatenum")==null ? "Defalt" : genericRecord.get("c_relatenum").toString());
                						metaLog.setImsi(genericRecord.get("c_imsi")==null ? "Defalt" : genericRecord.get("c_imsi").toString());
                						metaLog.setImei(genericRecord.get("c_imei")==null ? "Defalt" : genericRecord.get("c_imei").toString());
                						metaLog.setLac(genericRecord.get("c_lac")==null ? "Defalt" : genericRecord.get("c_lac").toString());
                						metaLog.setCi(genericRecord.get("c_ci")==null ? "Defalt" : genericRecord.get("c_ci").toString());
                						metaLog.setUli(genericRecord.get("c_uli")==null ? "Defalt" : genericRecord.get("c_uli").toString());
                						metaLog.setRegionCode(genericRecord.get("c_areacode")==null ? "Defalt" : genericRecord.get("c_areacode").toString());
                						metaLog.setHomeCode(genericRecord.get("c_homecode")==null ? "Defalt" : genericRecord.get("c_homecode").toString());
                						metaLog.setCdrContent(genericRecord.get("c_content")==null ? "Defalt" : genericRecord.get("c_content").toString());
                						metaLog.setLngi(0.0);
                						metaLog.setLati(0.0);
                						metaLog.setSourceData(genericRecord.toString());//add by zhangyu
                						al.add(metaLog);
                						if(metaLog.getMsisdn().equals("8618811770875") || metaLog.getMsisdn().equals("8613716201439")){
                							bufferWritter.write(metaLog.toString());
                							bufferWritter.flush();
                						}
                						
                                    } catch (IOException e1) {
                                        // TODO Auto-generated catch block
                                        e1.printStackTrace();
                                    }
                               
       							 
       						}
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                          
                            DataDispatcher.runTask(al);
                            //logger.info("INFO:==record count==: " +count+"==============");
                        }
                        
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.warn("MsgReceiver exception " + e + " ignore it");
                }
                
                
            }
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}finally {
            consumer.close();
        }
    }

}
