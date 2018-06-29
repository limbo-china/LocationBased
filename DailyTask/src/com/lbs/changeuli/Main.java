package com.lbs.changeuli;
import java.text.ParseException;
import java.util.concurrent.ThreadPoolExecutor;



public class Main {
	//static ThreadPoolManager threadpool = ThreadPoolManager.newInstance();
  public static void main(String[] args) {

	  ReadWriteFile2 getTraceDate = new ReadWriteFile2();
	  getTraceDate.readwrite();
}
}
