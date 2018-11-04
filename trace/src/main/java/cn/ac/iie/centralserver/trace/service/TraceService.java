package cn.ac.iie.centralserver.trace.service;

import java.util.List;

import cn.ac.iie.centralserver.trace.bean.QueryRequest;
import cn.ac.iie.centralserver.trace.bean.TracePersonResult;

public interface TraceService {

	public List<TracePersonResult> queryTrace(List<String> indexList,
			QueryRequest request);
}
