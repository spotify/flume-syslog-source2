package com.spotify.flume.syslog2;

import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.reporter.ReportEvent;


/**
 * A Flume source that adds a metric on rejected messages.
**/
public abstract class BaseSource extends EventSource.Base {
	final public static String R_NUM_REJECTED = "number of rejected messages";

	private AtomicLong numRejectedMessages = new AtomicLong();
	
	/**
	 * Note that another message has been rejected.
	**/
	protected void addRejectedMessage() {
		addRejectedMessages(1);
	}
	
	/**
	 * Increment the message rejection counter.
	 *
	 * @param n the delta count.
	**/
	protected void addRejectedMessages(int n) {
		numRejectedMessages.addAndGet(n);
	}
	
	@Override
	synchronized public ReportEvent getMetrics() {
		ReportEvent e = super.getMetrics();
		
		e.setLongMetric(R_NUM_REJECTED, numRejectedMessages.longValue());
		
		return e;
	}
}
