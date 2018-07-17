package cn.ac.iie.hy.centralserver.data;

import java.io.Serializable;
import java.util.ArrayList;

public class TracePersonResult implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2645667382326939695L;
	private int status;
	private ArrayList<TracePosition> tracelist = null;

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public void setTracelist(ArrayList<TracePosition> tracelist) {
		this.tracelist = tracelist;
	}

	public ArrayList<TracePosition> getTracelist() {
		return tracelist;
	}

	public boolean isEmpty() {
		return tracelist == null;
	}
}
