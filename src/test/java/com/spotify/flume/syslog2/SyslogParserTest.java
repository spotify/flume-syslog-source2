package com.spotify.flume.syslog2;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.util.Clock;
import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;


public class SyslogParserTest {
	final static String ENCODING = "UTF-8";

	@Test
	public void testCreate() throws Exception {
		ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
		SyslogParser p = new SyslogParser(in, true, ENCODING);

		p.close();
	}

	@Test
	public void testSkipLine() throws Exception {
		InputStream in = toInputStream("a\nb\r\nc");
		SyslogParser p = new SyslogParser(in, true, ENCODING);

		p.skipLine();
		p.skipLine();
		p.skipLine();
	}

	@Test(dataProvider = "messages")
	public void testReadEvent(String msg, Event target) throws Exception {
		InputStream in = toInputStream(msg);
		SyslogParser p = new SyslogParser(in, true, ENCODING);
		Event e = p.readEvent();
		
		assertEventEquals(target, e);
	}
	
	@DataProvider(name = "messages")
	public Object[][] createValidMessageData() throws Exception {
		// XXX: The BSD timestamp tests will fail if we run them while
		//      a new year is rolling. Worth fixing? Nah...
		return new Object[][] {
			{ "", null },
			// BSD date formats
			{ "<11>Oct  5 12:23:34 hostname tag: hello world", createEventImpl("hello world", false, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			// Not really valid, but want to check double-digit day.
			{ "<11>Oct 05 12:23:34 hostname tag: hello world", createEventImpl("hello world", false, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			// Priority
			{ "<1>2011-10-05 12:23:34Z hostname tag: hello world", createEventImpl("hello world", true, Event.Priority.FATAL, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 0 } },
				{ "syslogseverity", new byte[] { 1 } } }) },
			{ "<111>2011-10-05 12:23:34Z hostname tag: hello world", createEventImpl("hello world", true, Event.Priority.DEBUG, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 13 } },
				{ "syslogseverity", new byte[] { 7 } } }) },
			// ISO date formats
			{ "<11>2011-10-05 12:23:34Z hostname tag: hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>2011-10-05T12:23:34Z hostname tag: hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>2011-10-05T12:23:34.567Z hostname tag: hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			// Time zones
			{ "<11>2011-10-05T12:23:34+00 hostname tag: hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>2011-10-05T12:23:34+00:00 hostname tag: hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>2011-10-05T12:23:34-00 hostname tag: hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>2011-10-05T12:23:34-00:00 hostname tag: hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			// PID
			{ "<11>2011-10-05T12:23:34Z hostname tag[pid]: hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "tag" },
				{ "syslog.procId", "pid" },
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			// RFC 5424
			{ "<11>1 2011-10-05T12:23:34Z hostname - - - - hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.version", new byte[] { 1 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>1 2011-10-05T12:23:34Z hostname app proc msg - hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.appname", "app" },
				{ "syslog.procId", "proc" },
				{ "syslog.msgId", "msg" },
				{ "syslog.version", new byte[] { 1 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			// Structured data
			{ "<11>1 2011-10-05T12:23:34Z hostname - - - [a b=\"\"] hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.sd", "[a b=\"\"]" },
				{ "syslog.version", new byte[] { 1 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>1 2011-10-05T12:23:34Z hostname - - - [a b=\"c\"] hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.sd", "[a b=\"c\"]" },
				{ "syslog.version", new byte[] { 1 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>1 2011-10-05T12:23:34Z hostname - - - [a@123 b=\"c\"] hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.sd", "[a@123 b=\"c\"]" },
				{ "syslog.version", new byte[] { 1 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>1 2011-10-05T12:23:34Z hostname - - - [a b=\"c\" d=\"e\"] hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.sd", "[a b=\"c\" d=\"e\"]" },
				{ "syslog.version", new byte[] { 1 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
			{ "<11>1 2011-10-05T12:23:34Z hostname - - - [a b=\"c\"][aa bb=\"cc\"] hello world", createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.sd", "[a b=\"c\"][aa bb=\"cc\"]" },
				{ "syslog.version", new byte[] { 1 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }) },
		};
	}
	
	@Test()
	public void testReadEventNoParseTag() throws Exception {
		InputStream in = toInputStream("<11>2011-10-05T12:23:34Z hostname tag[pid]: hello world");
		SyslogParser p = new SyslogParser(in, false, ENCODING);
		
		assertEventEquals(createEventImpl("tag[pid]: hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }), p.readEvent());
	}
	
	@Test()
	public void testReadEventNoTag() throws Exception {
		InputStream in = toInputStream("<11>2011-10-05T12:23:34Z hostname hello world");
		SyslogParser p = new SyslogParser(in, false, ENCODING);
		
		assertEventEquals(createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }), p.readEvent());
	}
	
	@Test(dataProvider = "invalid-messages", expectedExceptions={ IOException.class })
	public void testReadInvalidEvent(String msg) throws Exception {
		InputStream in = toInputStream(msg);
		SyslogParser p = new SyslogParser(in, true, ENCODING);

		p.readEvent();
	}
	
	@DataProvider(name = "invalid-messages")
	public Object[][] createInvalidMessageData() {
		return new Object[][] {
			// Missing tag
			{ "<11>Oct  5 12:23:34 hostname hello world" },
			// Invalid time
			{ "<11>Oct  5 a2:23:34 hostname hello world" },
			// Unknown version
			{ "<11>2 2011-10-05T12:23:34-00:00 hostname tag: hello world" },
		};
	}
	
	@Test()
	public void testLineBreaks() throws Exception {
		InputStream in = toInputStream("<11>2011-10-05T12:23:34Z hostname hello world\n" +
			"<11>2011-10-05T12:23:34Z hostname hello world 2\r\n" +
			"<11>2011-10-05T12:23:34Z hostname hello world 3");
		SyslogParser p = new SyslogParser(in, false, ENCODING);
		
		assertEventEquals(createEventImpl("hello world", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }), p.readEvent());

		assertEventEquals(createEventImpl("hello world 2", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }), p.readEvent());

		assertEventEquals(createEventImpl("hello world 3", true, Event.Priority.ERROR, new Object[][] {
				{ "syslog.version", new byte[] { 0 } },
				{ "syslogfacility", new byte[] { 1 } },
				{ "syslogseverity", new byte[] { 3 } } }), p.readEvent());

		assertEventEquals(null, p.readEvent());
	}
	
	EventImpl createEventImpl(String msg, boolean isUtc, Event.Priority pri, Object[][] fields) throws Exception {
		Calendar cal = new GregorianCalendar(2011, 9, 5, 12, 23, 34);
		Map<String, byte[]> f = new HashMap<String, byte[]>();
		
		if (isUtc) cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		for (Object[] entry : fields) {
			if (entry[1] instanceof byte[])
				f.put((String) entry[0], (byte[]) entry[1]);
			else
				f.put((String) entry[0], ((String) entry[1]).getBytes(ENCODING));
		}

		return new EventImpl(msg.getBytes(ENCODING), cal.getTimeInMillis(), pri, 0, "hostname", f);
	}

	void assertEventEquals(Event target, Event real) {
		// This works since EventImpl.toString() sorts the fields first.
		if (target == null || real == null)
			assertEquals(target, real);
		else
			assertEquals(target.toString(), real.toString());
	}
	
	protected InputStream toInputStream(String data) throws Exception {
		return new ByteArrayInputStream(data.getBytes(ENCODING));
	}
}
