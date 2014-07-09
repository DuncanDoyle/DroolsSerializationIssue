package org.jboss.ddoyle.drools.model;

import java.io.Serializable;

public class SimpleEvent implements Serializable {
	
	/**
	 * SerialVersionUID.
	 */
	private static final long serialVersionUID = 1L;
	
	private final long timestamp;
	
	public SimpleEvent(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public long getTimestamp() {
		return timestamp;
	}

}
