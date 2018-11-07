package cn.ac.iie.timertask;

import cn.ac.iie.timertask.bean.TracePersonResult;
import cn.ac.iie.timertask.bean.TracePosition;
import cn.ac.iie.timertask.bean.TraceResult;

import com.google.gson.Gson;

public class ResultParse {

	public static String parseResult(String jsonString) {
		Gson gson = new Gson();

		TraceResult results = gson.fromJson(jsonString, TraceResult.class);

		String res = "";
		for (TracePersonResult result : results.getResults())
			for (TracePosition position : result.getTracelist())
				res = res + position.toString() + "\n";

		return res;
	}

}
