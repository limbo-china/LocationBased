package cn.ac.iie.hy.datadispatch.crypt;

import com.scistor.softcrypto.SoftCrypto;
import com.scistor.softcrypto.T_ncs_ticket;

import cn.ac.iie.hy.datadispatch.task.DBUpdateTask;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.ncs.dbcontrol.dbcontrol;
import com.ncs.dbcontrol.struct.TICKET_INFO;

public class DataCrypt {
	
	int crypto_time ;
	String str_crypto_content ;
	int block ;
	int base64en;
	
	static dbcontrol db;
	static SoftCrypto sc;
	static T_ncs_ticket redgo;
	
	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataCrypt.class.getName());
	}
	
	public DataCrypt(String str_crypto_content,int crypto_time,int block, int base64en){
		this.str_crypto_content=str_crypto_content;
		this.crypto_time=crypto_time;
		this.block=block;
		this.base64en=base64en;
	}
	
    public static void auth(String confPath) throws IOException{

        int ret, i;
		String str_pro_name = "beijing";
		
		Properties conf = new Properties();
		InputStream configInputStream = new FileInputStream(new File(confPath));
        conf.load(configInputStream);
        
        String ip = conf.getProperty("serverip");
        int port = Integer.parseInt(conf.getProperty("serverport"));
        String authPath = conf.getProperty("authpath");
        String pwd = conf.getProperty("password");
        
				
        sc = new SoftCrypto();
        //InitData vr=new InitData("10.213.72.10",3222,"/home/iie/distJM/cert.p12","111111");
        //InitData vr = null;
        ret = sc.Initialize(str_pro_name);
        //sc.cipher_initialize(pro_name, ip, port, p12Path, p12Pwd)
        //System.out.println("the result of initialize : " + ret);
        if(0 != ret)
            return;
		
        db = new dbcontrol();
        
        long lret =0 ;
        
        lret = db.jni_ncs_init(ip,(short) port,authPath,pwd);
		if(lret != 0)
		{
			logger.info("ncs_init : "+lret);
			System.exit(0);
		}
        
        
		logger.info("jni_ncs_init success!");
		
		TICKET_INFO ticketinfo = new TICKET_INFO();
		//System.out.println("~~~~~~~~");
		lret = db.jni_ncs_auth_login(ticketinfo);
		if(lret != 0)
		{
			logger.info("ncs_auth_login : "+lret);
			db.jni_ncs_uninit();
			System.exit(0);
		}
		else
		{
			logger.info("SN : "+ticketinfo.SN+"\nnext_time : " +ticketinfo.next_time +
					"\nend_time : " + ticketinfo.end_time + "\nsignLen : " +
					ticketinfo.signLen + "\n");
		}
		
		logger.info("jni_ncs_auth_login success!");
		
		lret = db.jni_ncs_verify_ticket(ticketinfo);
		if(lret != 0)
		{
			logger.info("ncs_verify_ticket : "+lret);
			db.jni_ncs_uninit();
			System.exit(0);
		}
		
		logger.info("jni_ncs_verify_ticket success!");
		
		byte[] byHashData = new byte[32];
		Integer hash_data_len = new Integer(32);
		//System.out.print("ncs_make_hash ("+hash_data_len+")+"+"\n");
		
		lret = db.jni_ncs_make_hash(ticketinfo, byHashData, hash_data_len);
		if(lret != 0)
		{
			logger.info("ncs_make_hash : "+lret);
			db.jni_ncs_uninit();
			System.exit(0);
		}
		logger.info("ncs_make_hash ("+hash_data_len+"): "+"\n");
        
        
        
        
//		TICKET_INFO newticketinfo = new TICKET_INFO();
//		
//		lret = db.jni_ncs_get_new_ticket(ticketinfo, newticketinfo);
//		if(lret != 0)
//		{
//			System.out.print("ncs_make_hash : "+lret);
//			db.jni_ncs_uninit();
//			System.exit(0);
//		}
//		else
//		{
//			System.out.print("SN : "+newticketinfo.SN+"\nnext_time : " +newticketinfo.next_time +
//				"\nend_time : " + newticketinfo.end_time + "\nsignLen : " +
//				newticketinfo.signLen + "\n");
//		}
//		
//		hash_data_len = 16;
//		lret = db.jni_ncs_make_hash(newticketinfo, byHashData, hash_data_len);
//		if(lret != 0)
//		{
//			System.out.print("ncs_make_hash : "+lret);
//			db.jni_ncs_uninit();
//			System.exit(0);
//		}
//		System.out.print("ncs_make_hash ("+hash_data_len+"): "+"\n");
		
		redgo = new T_ncs_ticket("999", "00", 2, byHashData, hash_data_len);
//		byte[] byHashData2 = {1,2,3,4,5};
//		Integer hash_data_len2 = 5;
//		T_ncs_ticket redgo1 = new T_ncs_ticket("999", "00", 2, byHashData2, hash_data_len2);
    }
	static public int encrypt(byte[] datain,byte[] dataout,int datainLen,int block,int base64en){
		return sc.crypto_encrypt(datain, dataout, datainLen, block, base64en);
	}
	static public int decrypt(byte[] datain,byte[] dataout,int datainLen,int block,int base64en){
		return sc.crypto_decrypt(datain, dataout, datainLen, block, base64en,redgo);
	}
}

