package com.spotify.flume.syslog2;

import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.reporter.ReportEvent;


public abstract class BaseSource extends EventSource.Base {
	final public static String R_NUM_REJECTED = "number of rejected messages";

	private AtomicLong numRejectedMessages = new AtomicLong();
	
	protected void addRejectedMessage() {
		numRejectedMessages.addAndGet(1);
	}
	
	@Override
	synchronized public ReportEvent getMetrics() {
		ReportEvent e = super.getMetrics();
		
		e.setLongMetric(R_NUM_REJECTED, numRejectedMessages.longValue());
		
		return e;
	}
}
