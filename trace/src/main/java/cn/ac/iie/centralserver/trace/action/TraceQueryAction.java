package cn.ac.iie.centralserver.trace.action;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.struts2.ServletActionContext;

import cn.ac.iie.centralserver.trace.bean.QueryRequest;
import cn.ac.iie.centralserver.trace.bean.TracePersonResult;
import cn.ac.iie.centralserver.trace.log.LogUtil;
import cn.ac.iie.centralserver.trace.service.TraceServiceImpl;

import com.google.gson.Gson;
import com.opensymphony.xwork2.ActionSupport;

public class TraceQueryAction extends ActionSupport {

	private static final long serialVersionUID = 1L;

	public String doTraceQuery() throws IOException {

		HttpServletRequest httpServletRequest = ServletActionContext
				.getRequest();
		HttpServletResponse httpServletResponse = ServletActionContext
				.getResponse();
		QueryRequest request = parseRequest(httpServletRequest);
		List<String> indexList = fetchIndexListByRequest(request);

		List<TracePersonResult> result = new TraceServiceImpl().queryTrace(
				indexList, request);

		Gson gson = new Gson();

		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		httpServletResponse.getWriter().println(gson.toJson(result));

		return NONE;
	}

	private QueryRequest parseRequest(HttpServletRequest httpServletRequest) {

		QueryRequest request = new QueryRequest();
		request.setUrl(new String(httpServletRequest.getRequestURL()));
		request.setToken(httpServletRequest.getParameter("token"));
		request.setQueryType(httpServletRequest.getParameter("querytype"));
		request.setIndex(httpServletRequest.getParameter("index"));
		request.setStartTime(httpServletRequest.getParameter("starttime"));
		request.setEndTime(httpServletRequest.getParameter("endtime"));
		request.setRemoteHost(httpServletRequest.getRemoteAddr());
		request.generateIndexList();
		LogUtil.info(request.toString());
		return request;
	}

	private List<String> fetchIndexListByRequest(QueryRequest request) {

		return Arrays.asList(request.getIndexList());
	}

}
