/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.datatrains.handler;


import cn.ac.iie.hy.datatrains.config.Configuration;
import cn.ac.iie.hy.datatrains.metadata.SMetaData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;




import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　　　　　┃
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　　　　　┃
 * 　　┃　　　┻　　　┃
 * 　　┃　　　　　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃神兽保佑, 永无BUG!
 * 　　　　┃　　　┃Code is far away from bug with the animal protecting
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * @author zhangyu
 */
public class DataDispatchReceiver {

    //docs shcema
    private static String docsSchemaContent = null;
    private static Schema docsSchema = null;
    private static Schema dataSchema = null;
    private static DatumReader<GenericRecord> docsReader = null;
    private static DatumReader<GenericRecord> contentReader = null;
    private static DataDispatchReceiver dataDispatchReceiver = null;
    static Logger logger = null;
    
    List<SMetaData> cache = new ArrayList<SMetaData>();
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataDispatchReceiver.class.getName());
    }

    private DataDispatchReceiver() {
    	
    }

    public static DataDispatchReceiver getDataDispatch() {
        if (dataDispatchReceiver != null) {
            return dataDispatchReceiver;
        }
        dataDispatchReceiver = new DataDispatchReceiver();
        Protocol protocol;
		try {
			protocol = Protocol.parse(new File("t_cdr.json"));
			//protocol = Protocol.parse(new File("xl-protocol.json"));

			docsSchema = protocol.getType("docs");
	        //GenericRecord docsRecord = new GenericData.Record(docsSchema);
	        //GenericArray docSet = new GenericData.Array<GenericRecord>(batchSize, docsSchema.getField("doc_set").schema());

	        //docsSchema = protocol.getType(schemaName);
            docsReader = new GenericDatumReader<GenericRecord>(docsSchema);
	        //数据包内每个record的具体格式
	        dataSchema = protocol.getType("t_cdr");
	        //Schema dataSchema = protocol.getType("xl");

	        contentReader = new GenericDatumReader<GenericRecord>(dataSchema);
	        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return dataDispatchReceiver;
    }


    public  void receive() throws Exception {
    	
    	DataDispatchReceiver.getDataDispatch();
    	
    	String configurationFileName = "kafka.properties";
        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }

        String servers = conf.getString("bootstrap.servers", "");
        String groupid = conf.getString("group.id", "");
        String autocommit = conf.getString("enable.auto.commit", "");
        String inteval = conf.getString("auto.commit.interval.ms", "");
        String timeout = conf.getString("session.timeout.ms", "");
        String reset = conf.getString("auto.offset.reset", "");
        String keydeser = conf.getString("key.deserializer", "");
        String valuedeser = conf.getString("value.deserializer", "");
        String topic = conf.getString("topic.name", "");
        if (servers.isEmpty()) {
            throw new Exception("definition bootstrap.servers is not found in " + configurationFileName);
        }
        if (groupid.isEmpty()) {
            throw new Exception("definition group.id is not found in " + configurationFileName);
        }
        if (autocommit.isEmpty()) {
            throw new Exception("definition enable.auto.commit is not found in " + configurationFileName);
        }
        if (inteval.isEmpty()) {
            throw new Exception("definition auto.commit.interval.ms is not found in " + configurationFileName);
        }
        if (timeout.isEmpty()) {
            throw new Exception("definition session.timeout.ms is not found in " + configurationFileName);
        }
        if (reset.isEmpty()) {
            throw new Exception("definition auto.offset.reset is not found in " + configurationFileName);
        }
        if (keydeser.isEmpty()) {
            throw new Exception("definition key.deserializer is not found in " + configurationFileName);
        }
        if (valuedeser.isEmpty()) {
            throw new Exception("definition value.deserializer is not found in " + configurationFileName);
        }
        if (topic.isEmpty()) {
            throw new Exception("definition topic.name is not found in " + configurationFileName);
        }

        
        Properties props = new Properties();
		props.put("bootstrap.servers", servers);
		props.put("group.id", groupid);
		props.put("enable.auto.commit", autocommit);
		props.put("auto.commit.interval.ms", inteval);
		props.put("session.timeout.ms", timeout);
		props.put("auto.offset.reset", reset);
		props.put("key.deserializer", keydeser);
		props.put("value.deserializer", valuedeser);
        
		String reqID = String.valueOf(System.nanoTime());;
        
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
		consumer.subscribe(Arrays.asList(topic));
		
		
		//try {  

			BinaryDecoder defaultDecoder = null;
			GenericRecord gRecord = new GenericData.Record(dataSchema);

			while (true) {
				System.out.println("1");
				ConsumerRecords<String, byte[]> records = consumer.poll(10);
				System.out.println("2");
				for (ConsumerRecord<String, byte[]> record : records) {
					System.out.println("3");
					defaultDecoder = DecoderFactory.get().binaryDecoder(record.value(), defaultDecoder);
					while (!defaultDecoder.isEnd()) {
						contentReader.read(gRecord, defaultDecoder);
						String imsi = gRecord.get("c_imsi")==null ? "Defalt" : gRecord.get("c_imsi").toString();
						String imei = gRecord.get("c_imei")==null ? "Defalt" : gRecord.get("c_imei").toString();
						System.out.println("c_imsi is " + imsi + ", c_imei is " + imei);
					}
					//System.out.println("next iterm");
				}
			}
		//} catch (Exception e1) {
		//	e1.printStackTrace();
		//}
		
        //retrive data
//        ServletInputStream servletInputStream = null;
//        byte[] req = null;
//        try {
//            logger.debug("req " + reqID + ":retriving bussisness data for request  ...");
//            baseRequest.setHandled(true);
//            servletInputStream = baseRequest.getInputStream();
//            ByteArrayOutputStream out = new ByteArrayOutputStream();
//            byte[] b = new byte[1024];
//            int i = 0;
//            while ((i = servletInputStream.read(b, 0, 1024)) > 0) {
//                out.write(b, 0, i);
//            }
//            req = out.toByteArray();
//            logger.debug("req " + reqID + ":length of bussisness data is " + req.length);
//        } catch (Exception ex) {
//            String errInfo = "req " + reqID + ":retrive bussiness data unsuccessfully for " + ex.getMessage();
//            logger.error(errInfo, ex);
//
//            return;
//        } finally {
//            try {
//                servletInputStream.close();
//            } catch (Exception ex) {
//            }
//        }
//
//        if (req == null || req.length == 0) {
//            String warnInfo = "req " + reqID + ":retrive bussiness data unsuccessfully for content is empty";
//            logger.error(warnInfo);
//            
//            return;
//        }
        
        //logger.info("req length " + req.length/1024 + "kb");

        //decode data
        //ByteArrayInputStream docsbis = null;
         //GenericRecord docsRecord = null;
       // try {
            //logger.debug("req " + reqID + ":decoding bussisness data ...");
            //docsbis = new ByteArrayInputStream(req);//tuning
//            BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
//
           // new GenericData.Record(docsSchema);
            //docsRecord = docsReader.read(docsRecord, docsbd);
//            //System.out.println(docsRecord);
//            logger.debug("req " + reqID + ":decode bussisness data sccessfully");
//        } catch (Exception ex) {
//            String errInfo = "req " + reqID + ":decode bussiness data unsuccessfully for " + ex.getMessage();
//            logger.error(errInfo, ex);
//
//            return;
//        } finally {
//            try {
//                docsbis = null;
//            } catch (Exception ex) {
//            }
//        }

        //check format of docs
//        String docSchemaName = null;
//        try {
//            docSchemaName = docsRecord.get("doc_schema_name").toString();
//            if (docSchemaName == null || docSchemaName.isEmpty()) {
//                String errInfo = "req " + reqID + ":docSchemaName is empty";
//                logger.error(errInfo);
//
//                return;
//            }
//        } catch (Exception ex) {
//            String errInfo = "req " + reqID + ":wrong format of bussiness data,can't get docSchemaName";
//            logger.error(errInfo, ex);
//
//            return;
//        }

        //String docSchemaFullName = docSchemaName + (region.isEmpty() ? "" : "@" + region);
        //logger.debug("req " + reqID + ":doc shcema full name is " + docSchemaFullName);

        
//        try {
//            Object bzSysSign = docsRecord.get("sign");
//            GenericArray<?> docsSet = (GenericArray<?>) docsRecord.get("doc_set");
//
//            if (docsSet == null) {
//                String errInfo = "req " + reqID + ":wrong format of bussiness data, doc_set is null";
//                logger.error(errInfo);
//
//            } else {
//                if (docsSet.size() <= 0) {
//                    String errInfo = "req " + reqID + ":doc_set is empty";
//                    logger.error(docsRecord.toString());
//
//                } else {
//                	GenericArray<?> docSet = (GenericArray<?>) docsRecord.get("doc_set");
//                	Iterator<?> itor = docSet.iterator();
//                	if(docSchemaName.equals("t_cdr")){
//                    	List<SMetaData> al = new ArrayList<SMetaData>();
//                    	
//                		while (itor.hasNext()) {
//    						ByteArrayInputStream databis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
//    						BinaryDecoder dataDecoder = new DecoderFactory().binaryDecoder(databis, null);
//    						GenericRecord record = (GenericRecord) contentReader.read(null, dataDecoder);
//    						//System.out.println(record);
//    						
//    						SMetaData metaLog = new SMetaData();
//    						
//    						metaLog.setTimestamp(record.get("c_timestamp")==null ? 0 : Long.parseLong(record.get("c_timestamp").toString()));
//    						metaLog.setCdrType(record.get("c_cdrtype")==null ? 999 : Integer.parseInt(record.get("c_cdrtype").toString()));
//    						metaLog.setMsisdn(record.get("c_usernum")==null ? "Defalt" : record.get("c_usernum").toString());
//    						metaLog.setRelateNum(record.get("c_relatenum")==null ? "Defalt" : record.get("c_relatenum").toString());
//    						metaLog.setImsi(record.get("c_imsi")==null ? "Defalt" : record.get("c_imsi").toString());
//    						metaLog.setImei(record.get("c_imei")==null ? "Defalt" : record.get("c_imei").toString());
//    						metaLog.setLac(record.get("c_lac")==null ? "Defalt" : record.get("c_lac").toString());
//    						metaLog.setCi(record.get("c_ci")==null ? "Defalt" : record.get("c_ci").toString());
//    						metaLog.setUli(record.get("c_uli")==null ? "Defalt" : record.get("c_uli").toString());
//    						metaLog.setRegionCode(record.get("c_areacode")==null ? "Defalt" : record.get("c_areacode").toString());
//    						metaLog.setHomeCode(record.get("c_homecode")==null ? "Defalt" : record.get("c_homecode").toString());
//    						metaLog.setCdrContent(record.get("c_content")==null ? "Defalt" : record.get("c_content").toString());
//    						metaLog.setLngi(0.0);
//    						metaLog.setLati(0.0);
//    						metaLog.setSourceData(record.toString());//add by zhangyu
//    						al.add(metaLog);
//    					}
//
//    					//DataDispatcher.runTask(al);
//                	}
//                	else if(docSchemaName.equals("t_cs_cdr")){
//                		List<String> al = new ArrayList<String>();
//                		while (itor.hasNext()) {
//    						ByteArrayInputStream databis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
//    						BinaryDecoder dataDecoder = new DecoderFactory().binaryDecoder(databis, null);
//    						GenericRecord record = (GenericRecord) contentReader.read(null, dataDecoder);
//    						logger.info("cscdr: " + record.toString());
//
//    						al.add(record.toString());
//    					}
//    					//DataDispatcher.runCsCdrTask(al);
//                	}
//                	else{
//                		logger.error("docSchemaName : " + docSchemaName + " wrong!!!");
//                	}
//					
//
//                    
//                }
//            }
//        } catch (Exception ex) {
//            String errInfo = "req " + reqID + ":internal error for sending data unsuccessfully for " + ex.getMessage();
//            logger.error(errInfo, ex);
//
//        }
        
    }

}
