package cn.ac.iie.timertask.bean;

import java.io.Serializable;
import java.util.ArrayList;

public class TracePersonResult implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2645667382326939695L;
	private int status;
	// private String reason = null;
	private ArrayList<TracePosition> tracelist = null;

	public int getStatus() {
		return status;
	}

	// public void setStatus(int status) {
	// this.status = status;
	// setReason(mapReason(status));
	// }

	public void setTracelist(ArrayList<TracePosition> tracelist) {
		this.tracelist = tracelist;
	}

	public ArrayList<TracePosition> getTracelist() {
		return tracelist;
	}

	public boolean isEmpty() {
		return tracelist == null;
	}

	// public String getReason() {
	// return reason;
	// }
	//
	// private void setReason(String reason) {
	// this.reason = reason;
	// }

	private String mapReason(int ret) {
		switch (ret) {
		case 0:
			return null;
		case 1:
			return "服务器错�?";
		case 2:
			return "请求参数非法";
		case 3:
			return "权限校验失败";
		case 4:
			return "配额不足";
		case 5:
			return "token不存在或非法";
		case 6:
			return "手机号映射缺�?";
		case 7:
			return "查询结果为空";
		case 8:
			return "位置未知";
		default:
			return "未知错误";
		}
	}
}
