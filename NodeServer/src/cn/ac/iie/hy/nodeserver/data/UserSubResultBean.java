package cn.ac.iie.hy.nodeserver.data;

import java.util.HashMap;
import java.util.List;

public class UserSubResultBean {

	String jobid = null;
	List<HashMap> result = null;
	public UserSubResultBean() {
		super();
	}
	public String getJobid() {
		return jobid;
	}
	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	public List<HashMap> getResult() {
		return result;
	}
	public void setResult(List<HashMap> result) {
		this.result = result;
	}
	
}
