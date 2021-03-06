package cn.ac.iie.centralserver.data;

public class IndexToQuery {

	private String imsi = "";
	private String msisdn = "";
	private int status = -1;

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
		if (!this.imsi.equals(""))
			return index.imsi.equals(this.imsi);
		else
			return index.msisdn.equals(this.msisdn);
	}

	@Override
	public int hashCode() {
		int result = 1;
		if (!imsi.equals(""))
			result = 31 * result + imsi.hashCode();
		else
			result = 31 * result + msisdn.hashCode();
		return result;
	}

}
