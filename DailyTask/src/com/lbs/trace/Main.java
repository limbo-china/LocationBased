package com.lbs.trace;
import java.text.ParseException;
import java.util.concurrent.ThreadPoolExecutor;



public class Main {
	//static ThreadPoolManager threadpool = ThreadPoolManager.newInstance();
  public static void main(String[] args) {
 //GetTraceData3:��ȡphonenumlist.txt�е��ֻ����룬��ѯλ����Ϣ����ʱΪuli,���һ��ת����ϸ��ַ��Ϣ
	  GetTraceData3 getTraceDate = new GetTraceData3();
	  getTraceDate.query();
}
}
