package com.w.limbo.test;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class GenerateUliRedis {

	
	public static void main(String[] args) throws IOException{
		Scanner scan = new Scanner(new FileInputStream("position.txt"),"UTF-8");
		FileWriter writer = new FileWriter("uliredis.txt"); 
		
		while(scan.hasNext()){
			String line = scan.nextLine();
			String regioncode = line.split(";")[3];
			String uli = line.split(";")[0];
			String baseinfo = line.split(";")[4];
			
			writer.write(uli+","+0.0+","+0.0+","+
					regioncode.substring(0,2)+"0000"+","+
					regioncode.substring(0,4)+"00"+","+
					regioncode+","+" "+","+
					baseinfo+"\n"
					);		
		}
		
		scan.close();
		writer.close();
	}
}
