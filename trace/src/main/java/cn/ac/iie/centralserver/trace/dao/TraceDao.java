package cn.ac.iie.centralserver.trace.dao;

import java.util.List;

import cn.ac.iie.centralserver.trace.bean.TraceDBData;

public interface TraceDao {

	public List<TraceDBData> getDBData(String queryType, String index,
			String starttime, String endtime);
}
