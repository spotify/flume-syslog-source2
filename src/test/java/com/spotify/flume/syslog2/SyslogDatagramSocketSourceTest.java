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
