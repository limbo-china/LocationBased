package cn.ac.iie.hy.datadispatch.task;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.datadispatch.data.SMetaData;


public class HiveLoadTask implements Runnable {

	private List<SMetaData> al = new ArrayList<SMetaData>();
	private String url = null;
	private String schemaName = null;
	private Schema docsSchema = null;
    static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(HiveLoadTask.class.getName());
    }
    
    public HiveLoadTask(List<SMetaData> al, String url, String schemaName, String FileName) {
		super();
		this.al = al;
		this.url = url;
		this.schemaName = schemaName;
		
		Schema.Parser parser = new Schema.Parser();
        String schemaStr = getSchemaStr("schema/" + FileName);
        System.out.println("Schema is " + schemaStr.replaceAll("\\s*", ""));
        docsSchema = parser.parse(schemaStr);
        
	}
    public static String getSchemaStr(String filePath){
        try {
            String encoding="UTF-8";
            File file=new File(filePath);
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                StringBuilder result = new StringBuilder();
                while((lineTxt = bufferedReader.readLine()) != null){
                    result.append(lineTxt);
                }
                read.close();
                return result.toString();
            }else{
                System.out.println("找不到指定的文件");
                return null;
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
            return null;
        }

    }

	public byte[] getAvroData() {
		try {
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(docsSchema);
            // ~=10MB
            ByteArrayOutputStream out = new ByteArrayOutputStream(10000000);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            out.reset();
            for (Iterator<SMetaData> it = al.iterator(); it.hasNext(); ) {
            	SMetaData smd = it.next();
				
				if(smd.getC_imei().length() > 20 
						|| smd.getC_imsi().length() > 20
						|| smd.getC_msisdn().length() > 16
						|| smd.getC_areacode().length() > 8){
					continue;
				}
				//String homecode = areaID;
                GenericRecord dataRecord = new GenericData.Record(docsSchema);
                dataRecord.put("c_imsi", smd.getC_imsi());
                dataRecord.put("c_imei", smd.getC_imei());
				dataRecord.put("c_msisdn", smd.getC_msisdn());
				dataRecord.put("c_uli", smd.getC_uli());
				dataRecord.put("c_areacode", smd.getC_areacode());
				dataRecord.put("c_timestamp", smd.getC_timestamp());
                //System.out.println(smd);
                //GenericRecord dataRecord = converter.convertToGenericDataRecord(smd.getBytes(), docsSchema);
                //System.out.println(dataRecord);
                writer.write(dataRecord, encoder);
                //System.out.println(out.size());
            }
            encoder.flush();

            //return out.toByteArray();
//            byte[] value = out.toByteArray();
//            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(docsSchema);
//            BinaryDecoder defaultDecoder = null;
//            defaultDecoder = DecoderFactory.get().binaryDecoder(value, defaultDecoder);
//            GenericRecord gRecord = new GenericData.Record(docsSchema);
//            while (!defaultDecoder.isEnd()) {
//                try {
//                    datumReader.read(gRecord, defaultDecoder);
//                    System.out.println(gRecord);
//                    //log.info("to find key====================================================="+key);
//                } catch (IOException e1) {
//                    e1.printStackTrace();
//                }
//            }

            return out.toByteArray();
        } catch (IOException ex) {
            logger.error(ex.getMessage());
            return null;
        }
	}

	public Boolean sendData() {
		HttpClient httpClient = new DefaultHttpClient();
        Boolean ret = true;
        try {
            byte[] data = getAvroData();
            if (data != null) {
                HttpPost httppost = new HttpPost(url);
                httppost.addHeader("content-type", "utf-8");
                httppost.addHeader("Topic", schemaName + "_dt");
                httppost.addHeader("Format", "avro");

                InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), data.length);
                reqEntity.setContentType("binary/octet-stream");

                httppost.setEntity(reqEntity);
                HttpResponse response = httpClient.execute(httppost);
                logger.info("Transfer status: " + response.getStatusLine());
                httppost.releaseConnection();
            }

        } catch (IOException ex) {
            ret = false;
            logger.error(ex.getMessage());
        } finally {
            httpClient.getConnectionManager().shutdown();
        }

        return ret;
	}
	
	@Override
	public void run() {
		sendData();
	}
	
	public static void main(String[] argv){
//		HiveLoadTask task = new HiveLoadTask("http://10.224.82.62:10080", "t_lbs_trace_history", "t_lbs_trace_history.json");
//		task.al.add(new SMetaData("460111111111", "22222222", "333333", "11-22-44-55", "102252", 123123213));
//		task.sendData();
	}

}
