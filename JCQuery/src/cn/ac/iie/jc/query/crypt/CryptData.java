package cn.ac.iie.jc.query.crypt;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class CryptData implements Serializable {

	private static final long serialVersionUID = -5311306462835188881L;

	String imsi;
	String imei;
	String msisdn;

	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(CryptData.class.getName());
	}

	public CryptData() {

	}

	public CryptData(String imsi, String imei, String msisdn) {
		super();
		this.imsi = imsi;
		this.imei = imei;
		this.msisdn = msisdn;
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

	public int decryptData() {

		int block = 1;
		int base64en = 0;
		int result = 0;

		if (imsi.length() == 15) {
			byte[] datain2 = imsi.substring(3, 15).getBytes();
			// logger.info("datain.length: "+datain4.length);
			// logger.info("datain.content: "+new String(datain4));
			byte[] dataout2 = new byte[datain2.length];
			result = DataCrypt.decrypt(datain2, dataout2, datain2.length, block, base64en);
			if (result == -1)
				return -1;
			String t2 = new String(dataout2);
			// logger.info("dataout.content: "+t4);
			imsi = imsi.substring(0, 3) + t2;
		}

		if (imei.length() == 14) {
			byte[] datain3 = imei.substring(7, 14).getBytes();
			// logger.info("datain.length: "+datain4.length);
			// logger.info("datain.content: "+new String(datain4));
			byte[] dataout3 = new byte[datain3.length];
			result = DataCrypt.decrypt(datain3, dataout3, datain3.length, block, base64en);
			if (result == -1)
				return -1;
			String t3 = new String(dataout3);
			// logger.info("dataout.content: "+t4);
			imei = imei.substring(0, 7) + t3;
		}

		if (msisdn.length() == 13) {
			byte[] datain4 = msisdn.substring(6, 13).getBytes();
			// logger.info("datain.length: "+datain4.length);
			// logger.info("datain.content: "+new String(datain4));
			byte[] dataout4 = new byte[datain4.length];
			result = DataCrypt.decrypt(datain4, dataout4, datain4.length, block, base64en);
			if (result == -1)
				return -1;
			String t4 = new String(dataout4);
			// logger.info("dataout.content: "+t4);
			msisdn = msisdn.substring(0, 6) + t4;
		}

		return 0;
	}

	public void print() {
		logger.info("imsi: " + imsi + " " + "imei: " + imei + " " + "msisdn: " + msisdn);
	}

}
