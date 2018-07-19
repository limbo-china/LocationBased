package cn.ac.iie.jc.query.crypt;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.ncs.dbcontrol.dbcontrol;
import com.ncs.dbcontrol.struct.TICKET_INFO;
import com.scistor.softcrypto.SoftCrypto;
import com.scistor.softcrypto.T_ncs_ticket;

public class DataCrypt {

	int crypto_time;
	String str_crypto_content;
	int block;
	int base64en;

	private static dbcontrol db;
	private static SoftCrypto sc;
	private static T_ncs_ticket redgo;

	static Logger logger = null;

	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(DataCrypt.class.getName());
	}

	public DataCrypt(String str_crypto_content, int crypto_time, int block, int base64en) {
		this.str_crypto_content = str_crypto_content;
		this.crypto_time = crypto_time;
		this.block = block;
		this.base64en = base64en;
	}

	public static void auth(String confPath) throws IOException {

		int ret, i;
		String str_pro_name = "liaoning";

		Properties conf = new Properties();
		InputStream configInputStream = new FileInputStream(new File(confPath));
		conf.load(configInputStream);

		String ip = conf.getProperty("serverip");
		int port = Integer.parseInt(conf.getProperty("serverport"));
		String authPath = conf.getProperty("authpath");
		String pwd = conf.getProperty("password");

		sc = new SoftCrypto();

		ret = sc.Initialize(str_pro_name);

		logger.info("ret:" + ret);
		if (0 != ret)
			return;

		db = new dbcontrol();

		long lret = 0;

		lret = db.jni_ncs_init(ip, (short) port, authPath, pwd);
		if (lret != 0) {
			logger.info("ncs_init : " + lret);
			System.exit(0);
		}

		logger.info("jni_ncs_init success!");

		TICKET_INFO ticketinfo = new TICKET_INFO();
		lret = db.jni_ncs_auth_login(ticketinfo);
		if (lret != 0) {
			logger.info("ncs_auth_login : " + lret);
			db.jni_ncs_uninit();
			System.exit(0);
		} else {
			logger.info("SN : " + ticketinfo.SN + "\nnext_time : " + ticketinfo.next_time + "\nend_time : "
					+ ticketinfo.end_time + "\nsignLen : " + ticketinfo.signLen + "\n");
		}

		logger.info("jni_ncs_auth_login success!");

		lret = db.jni_ncs_verify_ticket(ticketinfo);
		if (lret != 0) {
			logger.info("ncs_verify_ticket : " + lret);
			db.jni_ncs_uninit();
			System.exit(0);
		}

		logger.info("jni_ncs_verify_ticket success!");

		byte[] byHashData = new byte[32];
		Integer hash_data_len = new Integer(32);

		lret = db.jni_ncs_make_hash(ticketinfo, byHashData, hash_data_len);
		if (lret != 0) {
			logger.info("ncs_make_hash : " + lret);
			db.jni_ncs_uninit();
			System.exit(0);
		}
		logger.info("ncs_make_hash (" + hash_data_len + "): " + "\n");

		redgo = new T_ncs_ticket("999", "00", 2, byHashData, hash_data_len);

	}

	static public int encrypt(byte[] datain, byte[] dataout, int datainLen, int block, int base64en) {
		return sc.crypto_encrypt(datain, dataout, datainLen, block, base64en);
	}

	static public int decrypt(byte[] datain, byte[] dataout, int datainLen, int block, int base64en) {
		return sc.crypto_decrypt(datain, dataout, datainLen, block, base64en, redgo);
	}

}
