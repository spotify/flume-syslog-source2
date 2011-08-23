package com.spotify.flume.syslog2;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.cloudera.flume.core.Event;
import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;


public class SyslogDatagramSocketSourceTest {
	final public static int PORT = 43858;
	
	@Test
	public void testCreate() throws Exception {
		SyslogDatagramSocketSource s = new SyslogDatagramSocketSource(new InetSocketAddress("localhost", PORT), 4096);
	}
	
	@Test
	public void testOpen() throws Exception {
		SyslogDatagramSocketSource s = new SyslogDatagramSocketSource(new InetSocketAddress("localhost", PORT), 4096);
		
		s.open();
		s.close();
	}
	
	@Test
	public void testNext() throws Exception {
		SyslogDatagramSocketSource s = new SyslogDatagramSocketSource(new InetSocketAddress("localhost", PORT), 4096);
		DatagramSocket sender = new DatagramSocket();
		String data = "<11>2011-10-05T12:23:34.567Z hostname tag: hello world";
		byte[] bytes = data.getBytes("UTF-8");
		
		s.open();

		try {
			sender.send(new DatagramPacket(bytes, bytes.length, InetAddress.getByName("localhost"), PORT));
			assertTrue(s.next() instanceof Event);
		} finally {
			s.close();
		}
	}
}
