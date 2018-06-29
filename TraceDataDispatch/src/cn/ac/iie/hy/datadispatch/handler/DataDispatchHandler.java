/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.hy.datadispatch.handler;

import cn.ac.iie.hy.datadispatch.data.SMetaData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.Gson;

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
public class DataDispatchHandler extends AbstractHandler {

    private static Schema docsSchema = null;
    private static DatumReader<GenericRecord> docsReader = null;
    private static DatumReader<GenericRecord> contentReader = null;
    private static DataDispatchHandler dataDispatchHandler = null;
    static Logger logger = null;
    Gson gson = new Gson();

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataDispatchHandler.class.getName());
    }

    private DataDispatchHandler() {
    }

    public static DataDispatchHandler getDataDispatchHandler() {
        if (dataDispatchHandler != null) {
            return dataDispatchHandler;
        }
        dataDispatchHandler = new DataDispatchHandler();
        Protocol protocol;
		try {
			protocol = Protocol.parse(new File("t_lbs_trace_history.json"));
			docsSchema = protocol.getType("docs");
	        //GenericRecord docsRecord = new GenericData.Record(docsSchema);
	        //GenericArray docSet = new GenericData.Array<GenericRecord>(batchSize, docsSchema.getField("doc_set").schema());

	        //docsSchema = protocol.getType(schemaName);
            docsReader = new GenericDatumReader<GenericRecord>(docsSchema);
	        //数据包内每个record的具体格式
	        Schema dataSchema = protocol.getType("t_lbs_trace_history");
	        contentReader = new GenericDatumReader<GenericRecord>(dataSchema);

		} catch (IOException e) {
			e.printStackTrace();
		}

        return dataDispatchHandler;
    }


    @Override
    public void handle(String string, Request baseRequest, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        String remoteHost = baseRequest.getRemoteAddr();
        int remotePort = baseRequest.getRemotePort();
        String reqID = String.valueOf(System.nanoTime());
        String region = getRegion(baseRequest);
        logger.info("receive request from " + remoteHost + ":" + remotePort + "@" + region + " and assigned id " + reqID);

        //retrive data
        ServletInputStream servletInputStream = null;
        byte[] req = null;
        try {
            logger.debug("req " + reqID + ":retriving bussisness data for request  ...");
            baseRequest.setHandled(true);
            servletInputStream = baseRequest.getInputStream();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            int i = 0;
            while ((i = servletInputStream.read(b, 0, 1024)) > 0) {
                out.write(b, 0, i);
            }
            req = out.toByteArray();
            logger.debug("req " + reqID + ":length of bussisness data is " + req.length);
        } catch (Exception ex) {
            String errInfo = "req " + reqID + ":retrive bussiness data unsuccessfully for " + ex.getMessage();
            logger.error(errInfo, ex);
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            httpServletResponse.getWriter().println("-1\n" + errInfo);
            return;
        } finally {
            try {
                servletInputStream.close();
            } catch (Exception ex) {
            }
        }

        if (req == null || req.length == 0) {
            String warnInfo = "req " + reqID + ":retrive bussiness data unsuccessfully for content is empty";
            logger.error(warnInfo);
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            httpServletResponse.getWriter().println("-1\n" + warnInfo);
            return;
        }

        //decode data
        ByteArrayInputStream docsbis = null;
        GenericRecord docsRecord = null;
        try {
            logger.debug("req " + reqID + ":decoding bussisness data ...");
            docsbis = new ByteArrayInputStream(req);//tuning
            BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);

            new GenericData.Record(docsSchema);
            docsRecord = docsReader.read(docsRecord, docsbd);
            //System.out.println(docsRecord);
            logger.debug("req " + reqID + ":decode bussisness data sccessfully");
        } catch (Exception ex) {
            String errInfo = "req " + reqID + ":decode bussiness data unsuccessfully for " + ex.getMessage();
            logger.error(errInfo, ex);
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            httpServletResponse.getWriter().println("-1\n" + errInfo);
            return;
        } finally {
            try {
                docsbis = null;
            } catch (Exception ex) {
            }
        }

        //check format of docs         
        String docSchemaName = null;
        try {
            docSchemaName = docsRecord.get("doc_schema_name").toString();
            if (docSchemaName == null || docSchemaName.isEmpty()) {
                String errInfo = "req " + reqID + ":docSchemaName is empty";
                logger.error(errInfo);
                httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                httpServletResponse.getWriter().println("-1\n" + errInfo);
                return;
            }
        } catch (Exception ex) {
            String errInfo = "req " + reqID + ":wrong format of bussiness data,can't get docSchemaName";
            logger.error(errInfo, ex);
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            httpServletResponse.getWriter().println("-1\n" + errInfo);
            return;
        }

        String docSchemaFullName = docSchemaName + (region.isEmpty() ? "" : "@" + region);
        logger.debug("req " + reqID + ":doc shcema full name is " + docSchemaFullName);

        
        try {
            Object bzSysSign = docsRecord.get("sign");
            GenericArray<?> docsSet = (GenericArray<?>) docsRecord.get("doc_set");

            if (docsSet == null) {
                String errInfo = "req " + reqID + ":wrong format of bussiness data, doc_set is null";
                logger.error(errInfo);
                httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                httpServletResponse.getWriter().println("-1\n" + errInfo);
            } else {
                if (docsSet.size() <= 0) {
                    String errInfo = "req " + reqID + ":doc_set is empty";
                    httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                    httpServletResponse.getWriter().println("-1\n" + errInfo);
                } else {
                    
                	GenericArray<?> docSet = (GenericArray<?>) docsRecord.get("doc_set");
                	List<SMetaData> al = new ArrayList<SMetaData>();
                	Iterator<?> itor = docSet.iterator();
					while (itor.hasNext()) {
						ByteArrayInputStream databis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
						BinaryDecoder dataDecoder = new DecoderFactory().binaryDecoder(databis, null);
						GenericRecord record = (GenericRecord) contentReader.read(null, dataDecoder);
						
						SMetaData smd = gson.fromJson(record.toString(), SMetaData.class);
					
						al.add(smd);

						//System.out.println(record);
						//System.exit(0);
						// al.add(new SMetaData(record.get("field1").toString(),
						// (int) record.get("field2"),
						// (long)record.get("field3")));
					}
                	
                    logger.info("req " + reqID + ":sending " + al.size() + " records of " + docSchemaName + " to queue successfully");
                    DataDispatcher.runTask(al);
                    httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                    httpServletResponse.getWriter().println("0\n" + bzSysSign + "\n" + reqID);
                }
            }
        } catch (Exception ex) {
            String errInfo = "req " + reqID + ":internal error for sending data unsuccessfully for " + ex.getMessage();
            logger.error(errInfo, ex);
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            httpServletResponse.getWriter().println("-1\n" + errInfo);
        }
    }

    private String getRegion(Request req) {
        Object val = req.getParameter("region");
        return val == null ? "" : ((String) val).toLowerCase();
    }
}
