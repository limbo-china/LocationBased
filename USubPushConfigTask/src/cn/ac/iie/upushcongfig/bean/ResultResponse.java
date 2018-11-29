package cn.ac.iie.upushcongfig.bean;

public class ResultResponse {

	private Integer status;
	private Integer size;
	private String reason;

	public int getStatus() {
		return status;
	}

	public int getSize() {
		return size;
	}

	public String getReason() {
		return reason;
	}

	public void setStatus(int status, int size) {
		this.status = status;
		switch (status) {
		case 0:
			setSize(size);
			break;
		case 1:
			setReason("服务器内部错误");
		default:
			break;
		}
	}

	private void setSize(int size) {
		this.size = size;
	}

	private void setReason(String reason) {
		this.reason = reason;
	}

}
