/**
 * Copyright 2011 Spotify Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
