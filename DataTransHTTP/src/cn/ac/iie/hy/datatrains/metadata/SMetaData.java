package cn.ac.iie.hy.datatrains.metadata;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.iie.hy.datatrains.handler.DataDispatchHandler;

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
 *
 */
public class SMetaData implements Serializable{

	/**
	 * 
	 */
	static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataDispatchHandler.class.getName());
    }
    
	private static final long serialVersionUID = -5311306462835188881L;
	public SMetaData(String imsi, String imei, String msisdn, String relateNum, String regionCode, String lac,
			String ci, String uli, String homeCode, double lngi, double lati, long timestamp, int cdrType) {
		super();
		this.imsi = imsi;
		this.imei = imei;
		this.msisdn = msisdn;
		this.relateNum = relateNum;
		this.regionCode = regionCode;
		this.lac = lac;
		this.ci = ci;
		this.uli = uli;
		this.homeCode = homeCode;
		this.lngi = lngi;
		this.lati = lati;
		this.timestamp = timestamp;
		this.cdrType = cdrType;
	}
	
	String imsi;
	String imei;
	String msisdn;
	String relateNum;
	String regionCode;
	String lac;
	String ci;
	String uli;
	String homeCode;
	String cdrContent;
	double lngi;
	double lati;
	long timestamp;
	int cdrType;
	String sourceData;
	int isJmFlag; //add by yyq
	
	public int getIsJmFlag() {
		return isJmFlag;
	}

	public void setIsJmFlag(int isJmFlag) {
		this.isJmFlag = isJmFlag;
	}

	public String getCdrContent() {
		return cdrContent;
	}

	public void setCdrContent(String cdrContent) {
		this.cdrContent = cdrContent;
	}

	
	public String getRelateNum() {
		return relateNum;
	}

	public void setRelateNum(String relateNum) {
		this.relateNum = relateNum;
	}

	public int getCdrType() {
		return cdrType;
	}

	public void setCdrType(int cdrType) {
		this.cdrType = cdrType;
	}

	public SMetaData(){
		
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

	public String getSourceData() {
		return sourceData;
	}

	public void setSourceData(String sourceData) {
		this.sourceData = sourceData;
	}
	
	public void print(){
		logger.info("imsi: "+imsi+" "+"imei: "+imei+" "+"msisdn: "+msisdn + "areacode: "+regionCode);
	}

	
	
	
}
