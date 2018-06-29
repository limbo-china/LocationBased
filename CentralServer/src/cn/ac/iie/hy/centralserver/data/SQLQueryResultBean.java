package cn.ac.iie.hy.centralserver.data;

import java.util.HashMap;
import java.util.List;

public class SQLQueryResultBean {
	String jobID;
	String sql;
	List<HashMap> result;

	public SQLQueryResultBean() {
		super();
	}

	public SQLQueryResultBean(String jobID, String sql, List<HashMap> result) {
		super();
		this.jobID = jobID;
		this.sql = sql;
		this.result = result;
	}

	@Override
	public String toString() {
		return "SQLQueryResultBean [jobID=" + jobID + ", sql=" + sql + ", result=" + result + "]";
	}

	public String getJobID() {
		return jobID;
	}

	public void setJobID(String jobID) {
		this.jobID = jobID;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public List<HashMap> getResult() {
		return result;
	}

	public void setResult(List<HashMap> result) {
		this.result = result;
	}

}
