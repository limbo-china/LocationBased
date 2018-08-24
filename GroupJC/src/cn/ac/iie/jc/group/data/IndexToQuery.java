package cn.ac.iie.jc.group.data;

public class IndexToQuery {

	private String imsi = "";
	private String msisdn = "";

	public IndexToQuery(String msisdn) {
		this.msisdn = msisdn;
	}

	public IndexToQuery(String msisdn, String imsi) {
		this.msisdn = msisdn;
		this.imsi = imsi;
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

	@Override
	public String toString() {
		return "IndexToQuery [imsi=" + imsi + ", msisdn=" + msisdn + "]";
	}

}
