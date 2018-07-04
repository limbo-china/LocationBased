package com.w.limbo.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class adjustDataFile {
	
	private static String[] prov_code = {"11","12","13","14","15","21","22","23","31","32",
		"33","34","35","36","37","41","42","43","44","45","46","50","51","52","53","54",
		"61","62","63","64","65"};
	private static Random random = new Random();
	private static HashMap<String, String> uliMap = new HashMap<String, String>();
	
	public static void main(String[] args) throws IOException{
		//adjustMsisdn();
		//adjustUli();
		getUli();
		adjustPosition();
	}
	private void adjustMsisdn() throws IOException{
		FileInputStream input = new FileInputStream("msisdn.txt");
		FileWriter writer = new FileWriter("msisdn2.txt");
		Scanner scan = new Scanner(input);
		
		while(scan.hasNext()){
			String line = scan.nextLine();
			String msisdn = line.split(";")[0];
			String imsi = line.split(";")[1];
			String imei = line.split(";")[2];
			String region = prov_code[random.nextInt(prov_code.length)]+ line.split(";")[3].substring(2, 6);
			writer.write(msisdn +";"+imsi+";"+imei+";"+region+"\n");
		}
		input.close();
		writer.close();
		scan.close();
	}
	private static void adjustUli() throws IOException{
		FileInputStream input = new FileInputStream("uliredis.txt");
		FileWriter writer = new FileWriter("uliredis2.txt");
		Scanner scan = new Scanner(input,"UTF-8");
		
		while(scan.hasNext()){
			String line = scan.nextLine();
			String uli = line.split(",")[0];
			String prov = prov_code[random.nextInt(prov_code.length)];
			String city = line.split(",")[4].substring(2,4);
			String region = line.split(",")[5].substring(4, 6);
			String address = line.split(",")[7];
			writer.write(uli+","+0.0+","+0.0+","+prov+"0000"+","+
					prov+city+"00"+","+
					prov+city+region+","+" "+","+
					address+"\n"
					);
		}
		input.close();
		writer.close();
		scan.close();
	}
	private static void getUli() throws IOException{
		FileInputStream input = new FileInputStream("uliredis.txt");
		Scanner scan = new Scanner(input,"UTF-8");
		while(scan.hasNext()){
			String line = scan.nextLine();
			uliMap.put(line.split(",")[0], line.split(",")[5]);
		}
		scan.close();
	}
	private static void adjustPosition() throws IOException{
		FileInputStream input = new FileInputStream("position.txt");
		FileWriter writer = new FileWriter("position2.txt");
		Scanner scan = new Scanner(input,"UTF-8");
		
		while(scan.hasNext()){
			String line = scan.nextLine();
			String uli = line.split(";")[0];
			String ca = line.split(";")[1];
			String lac = line.split(";")[2];
			String region = uliMap.get(uli);
			writer.write(uli+";"+ca+";"+lac+";"+region+"\n");
		}
		input.close();
		writer.close();
		scan.close();
	}
}
