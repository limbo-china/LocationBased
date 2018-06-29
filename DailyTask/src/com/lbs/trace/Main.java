package com.lbs.trace;
import java.text.ParseException;
import java.util.concurrent.ThreadPoolExecutor;



public class Main {
	//static ThreadPoolManager threadpool = ThreadPoolManager.newInstance();
  public static void main(String[] args) {
 //GetTraceData3:读取phonenumlist.txt中的手机号码，查询位置信息，此时为uli,需进一步转成详细地址信息
	  GetTraceData3 getTraceDate = new GetTraceData3();
	  getTraceDate.query();
}
}
