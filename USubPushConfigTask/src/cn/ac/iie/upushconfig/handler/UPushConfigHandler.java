package cn.ac.iie.upushconfig.handler;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import cn.ac.iie.upushconfig.log.LogUtil;
import cn.ac.iie.upushconfig.task.UPushConfigTask;
import cn.ac.iie.upushcongfig.bean.QueryRequest;

public class UPushConfigHandler extends AbstractHandler {

	// private static ThreadPoolManager threadpool = ThreadPoolManager
	// .newInstance();
	private static UPushConfigHandler uPushConfigHandler = new UPushConfigHandler();

	private UPushConfigHandler() {
	}

	public static UPushConfigHandler getHandler() {
		return uPushConfigHandler;
	}

	@Override
	public void handle(String string, Request baseRequest,
			HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException,
			ServletException {
		QueryRequest request = parseRequest(baseRequest, httpServletRequest);

		List<String> indexList = fetchIndexListByRequest(request);

		String result = execTask(indexList);

		writeResponse(result, baseRequest, httpServletResponse);

	}

	private QueryRequest parseRequest(Request baseRequest,
			HttpServletRequest httpServletRequest) {

		QueryRequest request = new QueryRequest();
		request.setUrl(new String(httpServletRequest.getRequestURL()));
		request.setIndex(httpServletRequest.getParameter("index"));
		request.setRemoteHost(baseRequest.getRemoteAddr());
		request.generateIndexList();
		LogUtil.info(request.toString());
		return request;
	}

	private List<String> fetchIndexListByRequest(QueryRequest request) {

		return request.getIndexList();
	}

	private void writeResponse(String result, Request baseRequest,
			HttpServletResponse httpServletResponse) {
		httpServletResponse.setContentType("text/json;charset=utf-8");
		httpServletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		try {
			httpServletResponse.getWriter().println(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String execTask(List<String> msisdns) {
		return new UPushConfigTask(msisdns).exec();
	}

}
