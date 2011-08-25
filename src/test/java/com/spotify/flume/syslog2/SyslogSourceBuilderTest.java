package com.spotify.flume.syslog2;

import com.cloudera.flume.core.EventSource;
import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;


public class SyslogSourceBuilderTest {
	@Test
	public void testTcp() {
		assertTrue(new SyslogSourceBuilder().build("tcp") instanceof EventSource);
		assertTrue(new SyslogSourceBuilder().build("tcp", "localhost") instanceof EventSource);
		assertTrue(new SyslogSourceBuilder().build("tcp", "localhost", "12345") instanceof EventSource);
	}

	@Test
	public void testUdp() {
		assertTrue(new SyslogSourceBuilder().build("udp") instanceof EventSource);
		assertTrue(new SyslogSourceBuilder().build("udp", "localhost") instanceof EventSource);
		assertTrue(new SyslogSourceBuilder().build("udp", "localhost", "12345") instanceof EventSource);
		assertTrue(new SyslogSourceBuilder().build("udp", "localhost", "12345", "4096") instanceof EventSource);
	}
}
