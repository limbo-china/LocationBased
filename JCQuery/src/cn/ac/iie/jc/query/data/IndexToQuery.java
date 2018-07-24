package cn.ac.iie.jc.query.data;

public class IndexToQuery {

	private String groupId = "";
	private String imsi = "";
	private String msisdn = "";
	private int status = -1;

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public String getMsisdn() {
		return msisdn;
	}

	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getKeyByQueryType(String queryType) {
		return queryType.equals("msisdn") ? msisdn : imsi;
	}

	public boolean isSuccess() {
		return status == 0;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof IndexToQuery))
			return false;
		IndexToQuery index = (IndexToQuery) o;
		return index.imsi.equals(this.imsi);
	}

	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + imsi.hashCode();
		return result;
	}

}
