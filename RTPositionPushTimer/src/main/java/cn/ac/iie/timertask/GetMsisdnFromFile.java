package cn.ac.iie.timertask;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class GetMsisdnFromFile {

	private String filename = null;
	private static GetMsisdnFromFile INSTANCE = null;

	private GetMsisdnFromFile(String filename) {
		this.filename = filename;
	}

	public static GetMsisdnFromFile getInstance(String filename) {
		if (INSTANCE == null)
			return new GetMsisdnFromFile(filename);
		return INSTANCE;
	}

	public List<String> getMsisdnList() throws FileNotFoundException {

		FileInputStream input = new FileInputStream(filename);
		Scanner scan = new Scanner(input, "UTF-8");
		List<String> msisdnList = new ArrayList<String>();

		String res = "";
		int size = 10000;
		while (scan.hasNext()) {
			if (size-- <= 0) {
				msisdnList.add(res);
				res = "";
				size = 10000;
			}
			String line = scan.nextLine();
			res += line + ",";
		}
		msisdnList.add(res);
		scan.close();

		return msisdnList;

	}
}
