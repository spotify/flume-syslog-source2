package com.spotify.flume.syslog2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.cloudera.flume.core.Event;
import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;


/**
 * Given the SyslogSocketSource class is so simple, this is mostly a test of
 * the ServerSocketSource class.
**/
public class SyslogSocketSourceTest {
	/// A very random ephemeral port number...
	final public static int PORT = 53858;
	
	@Test
	public void testCreate() throws Exception {
		SyslogSocketSource s = new SyslogSocketSource(new InetSocketAddress("localhost", PORT));
	}
	
	@Test
	public void testOpen() throws Exception {
		SyslogSocketSource s = new SyslogSocketSource(new InetSocketAddress("localhost", PORT));
		
		s.open();
		s.close();
	}
	
	@Test
	public void testNext() throws Exception {
		SyslogSocketSource s = new SyslogSocketSource(new InetSocketAddress("localhost", PORT));
		String data = "<11>2011-10-05T12:23:34.567Z hostname tag: hello world";
		byte[] bytes = data.getBytes("UTF-8");
		
		s.open();

		try {
			Socket sender = new Socket("localhost", PORT);
			PrintStream ps = new PrintStream(sender.getOutputStream());

			ps.println(data);
			ps.flush();
			sender.close();
			assertTrue(s.next() instanceof Event);
		} finally {
			s.close();
		}
	}
	
	@Test
	public void testNextOnEof() throws Exception {
		SyslogSocketSource s = new SyslogSocketSource(new InetSocketAddress("localhost", PORT));
		String data = "<11>2011-10-05T12:23:34.567Z hostname tag: hello world";
		byte[] bytes = data.getBytes("UTF-8");
		
		s.open();

		try {
			Socket sender = new Socket("localhost", PORT);
			PrintStream ps = new PrintStream(sender.getOutputStream());

			ps.println(data);
			ps.flush();
			sender.close();
			assertTrue(s.next() instanceof Event);
		} finally {
			s.close();
		}

		assertEquals(null, s.next());
	}
	
	@Test
	public void testRecover() throws Exception {
		SyslogSocketSource s = new SyslogSocketSource(new InetSocketAddress("localhost", PORT));
		// Invalid date
		String data = "<11>2011-AA-05T12:23:34.567Z hostname tag: hello world\n<11>2011-10-05T12:23:34.567Z hostname tag: hello world";
		byte[] bytes = data.getBytes("UTF-8");
		
		s.open();

		try {
			Socket sender = new Socket("localhost", PORT);
			PrintStream ps = new PrintStream(sender.getOutputStream());

			ps.println(data);
			ps.flush();
			sender.close();
			assertTrue(s.next() instanceof Event);
		} finally {
			s.close();
		}
	}
}
