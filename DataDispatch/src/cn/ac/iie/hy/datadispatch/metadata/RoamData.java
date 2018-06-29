package cn.ac.iie.hy.datadispatch.metadata;

import cn.ac.iie.hy.datadispatch.crypt.DataCrypt;

public class RoamData {

	private String RoamProvince;
	private String Region;
	private String HomeCode;
	private String UserNumber;
	private int Time;
	private int Action;

	public RoamData() {
		super();
	}

	


	public RoamData(String roamProvince, String region, String homeCode, String userNumber, int time, int action) {
		super();
		RoamProvince = roamProvince;
		Region = region;
		HomeCode = homeCode;
		UserNumber = userNumber;
		Time = time;
		Action = action;
	}


	public String getRoamProvince() {
		return RoamProvince;
	}

	public void setRoamProvince(String roamProvince) {
		RoamProvince = roamProvince;
	}

	public String getRegion() {
		return Region;
	}

	public void setRegion(String region) {
		Region = region;
	}

	public String getHomeCode() {
		return HomeCode;
	}

	public void setHomeCode(String homeCode) {
		HomeCode = homeCode;
	}



	public String getUserNumber() {
		return UserNumber;
	}

	public void setUserNumber(String userNumber) {
		UserNumber = userNumber;
	}

	public int getTime() {
		return Time;
	}

	public void setTime(int time) {
		Time = time;
	}

	public int getAction() {
		return Action;
	}

	public void setAction(int action) {
		Action = action;
	}

	public int decryptData(){
		
		int block = 1;
		int base64en = 0;
		int dedataout_len = 0;
		
//		byte[] datain1 = RoamProvince.getBytes();
//		byte[] dataout1 = new byte[datain1.length];
//		DataCrypt.decrypt(datain1, dataout1, datain1.length, block, base64en);
//		String t1 = new String(dataout1);
//		RoamProvince = t1;
//		
//		byte[] datain2 = Region.getBytes();
//		byte[] dataout2 = new byte[datain2.length];
//		DataCrypt.decrypt(datain2, dataout2, datain2.length, block, base64en);
//		String t2 = new String(dataout2);
//		Region = t2;
//		
//		byte[] datain3 = HomeCode.getBytes();
//		byte[] dataout3 = new byte[datain3.length];
//		DataCrypt.decrypt(datain3, dataout3, datain3.length, block, base64en);
//		String t3 = new String(dataout3);
//		HomeCode = t3;
		
		byte[] datain4 = UserNumber.substring(UserNumber.length()-7, UserNumber.length()).getBytes();
		byte[] dataout4 = new byte[datain4.length];
		dedataout_len = DataCrypt.decrypt(datain4, dataout4, datain4.length, block, base64en);
		String t4 = new String(dataout4);
		UserNumber = UserNumber.substring(0,UserNumber.length()-7)+t4;
		return dedataout_len;
	}
	
	public void encryptData(){
		int block = 1;
		int base64en = 0;
		
		byte[] datain1 = RoamProvince.getBytes();
		byte[] dataout1 = new byte[(datain1.length+16)*block*2];
		DataCrypt.encrypt(datain1, dataout1, datain1.length, block, base64en);
		String t1 = new String(dataout1);
		RoamProvince = t1;
		
		byte[] datain2 = Region.getBytes();
		byte[] dataout2 = new byte[(datain2.length+16)*block*2];
		DataCrypt.encrypt(datain2, dataout2, datain2.length, block, base64en);
		String t2 = new String(dataout2);
		Region = t2;
		
		byte[] datain3 = HomeCode.getBytes();
		byte[] dataout3 = new byte[(datain3.length+16)*block*2];
		DataCrypt.encrypt(datain3, dataout3, datain3.length, block, base64en);
		String t3 = new String(dataout3);
		HomeCode = t3;
		
		byte[] datain4 = UserNumber.getBytes();
		byte[] dataout4 = new byte[(datain4.length+16)*block*2];
		DataCrypt.encrypt(datain4, dataout4, datain4.length, block, base64en);
		String t4 = new String(dataout4);
		UserNumber = t4;	
	}
	public void print(){
		System.out.println("RoamProvince: "+RoamProvince);
		System.out.println("Region: "+Region);
		System.out.println("HomeCode: "+HomeCode);
		System.out.println("UserNumber: "+UserNumber);
	}
}
