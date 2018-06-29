package cn.ac.iie.hy.datadispatch.metadata;
import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.datadispatch.crypt.DataCrypt;
import cn.ac.iie.hy.datadispatch.task.DBUpdateTask;


public class SMetaData implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5311306462835188881L;
	String imsi;
	String imei;
	String msisdn;
	String regionCode;
	String lac;
	String ci;
	String uli;
	String homeCode;
	double lngi;
	double lati;
	long timestamp;
	
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(SMetaData.class.getName());
	}
	
	public SMetaData(){
		
	}
	
	public SMetaData(String imsi, String imei, String msisdn, String regionCode, String lac, String ci, String uli,
			String homeCode, double lngi, double lati, long timestamp) {
		super();
		this.imsi = imsi;
		this.imei = imei;
		this.msisdn = msisdn;
		this.regionCode = regionCode;
		this.lac = lac;
		this.ci = ci;
		this.uli = uli;
		this.homeCode = homeCode;
		this.lngi = lngi;
		this.lati = lati;
		this.timestamp = timestamp;
	}
	
	public String getImsi() {
		return imsi;
	}
	public void setImsi(String imsi) {
		this.imsi = imsi;
	}
	public String getImei() {
		return imei;
	}
	public void setImei(String imei) {
		this.imei = imei;
	}
	public String getMsisdn() {
		return msisdn;
	}
	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}
	public String getRegionCode() {
		return regionCode;
	}
	public void setRegionCode(String regionCode) {
		this.regionCode = regionCode;
	}
	public String getLac() {
		return lac;
	}
	public void setLac(String lac) {
		this.lac = lac;
	}
	public String getCi() {
		return ci;
	}
	public void setCi(String ci) {
		this.ci = ci;
	}
	public String getUli() {
		return uli;
	}
	public void setUli(String uli) {
		this.uli = uli;
	}
	public String getHomeCode() {
		return homeCode;
	}
	public void setHomeCode(String homeCode) {
		this.homeCode = homeCode;
	}
	public double getLngi() {
		return lngi;
	}
	public void setLngi(double lngi) {
		this.lngi = lngi;
	}
	public double getLati() {
		return lati;
	}
	public void setLati(double lati) {
		this.lati = lati;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public int decryptData(){
		
		int block = 1;
		int base64en = 0;
		int result = 0;
		
		if(imsi.length() == 15){
		byte[] datain2 = imsi.substring(3, 15).getBytes();
        //logger.info("datain.length: "+datain4.length);
        //logger.info("datain.content: "+new String(datain4));
		byte[] dataout2 = new byte[datain2.length];
		result = DataCrypt.decrypt(datain2, dataout2, datain2.length, block, base64en);
		if(result == -1)
			return -1;
		String t2 = new String(dataout2);
		//logger.info("dataout.content: "+t4);
		imsi = imsi.substring(0,3)+t2;
		}
		
		if(imei.length() == 14){
		byte[] datain3 = imei.substring(7, 14).getBytes();
        //logger.info("datain.length: "+datain4.length);
        //logger.info("datain.content: "+new String(datain4));
		byte[] dataout3 = new byte[datain3.length];
		result = DataCrypt.decrypt(datain3, dataout3, datain3.length, block, base64en);
		if(result == -1)
			return -1;
		String t3 = new String(dataout3);
		//logger.info("dataout.content: "+t4);
		imei = imei.substring(0,7)+t3;
		}
		
		if(msisdn.length()==13){
		byte[] datain4 = msisdn.substring(6, 13).getBytes();
        //logger.info("datain.length: "+datain4.length);
        //logger.info("datain.content: "+new String(datain4));
		byte[] dataout4 = new byte[datain4.length];
		result = DataCrypt.decrypt(datain4, dataout4, datain4.length, block, base64en);
		if(result == -1)
			return -1;
		String t4 = new String(dataout4);
		//logger.info("dataout.content: "+t4);
		msisdn = msisdn.substring(0,6)+t4;
		}
		
		return 0;
	}
	
	public void print(){
		if(msisdn.length() == 13 && imsi.length() == 15 && imei.length() == 14)
			logger.info("imsi: "+imsi+"\n"+"imei: "+imei+"\n"+"homeCode: "+homeCode+"\n"+"msisdn: "+msisdn);
	}
	
}
