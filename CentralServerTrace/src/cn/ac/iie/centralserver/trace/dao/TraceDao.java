package cn.ac.iie.centralserver.trace.dao;

import java.util.ArrayList;

import cn.ac.iie.centralserver.trace.bean.TraceDBData;

public interface TraceDao {

	public ArrayList<TraceDBData> getTraceData(String queryType, String index,
			String starttime, String endtime);
}
