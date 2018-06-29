package cn.ac.iie.hy.centralserver.data;

import java.io.Serializable;

public class TraceQueryData implements Serializable{

	private static final long serialVersionUID = -2509845860519483925L;

	private String uli;
	private Long time;
	public String getUli() {
		return uli;
	}
	public void setUli(String uli) {
		this.uli = uli;
	}
	public Long getTime() {
		return time;
	}
	public void setTime(Long time) {
		this.time = time;
	}
	
}
