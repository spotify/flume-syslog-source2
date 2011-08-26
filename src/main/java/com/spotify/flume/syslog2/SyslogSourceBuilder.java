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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Pair;


/**
 * Create a new syslog source.
 *
 * This builder handles both streaming and datagram sockets.
 *
 * Create a socket using one of the following forms:
 *
 *  * syslog2(tcp)
 *  * syslog2(tcp, localhost)
 *  * syslog2(tcp, localhost, 4711)
 *  * syslog2(udp, localhost, 4711, 16384)
 *
 * The host defaults to "localhost" and the port defaults to 514. The
 * buffer size argument (last) is only used for UDP sockets and defines
 * the size of the DatagramPacket buffer.
 */
public class SyslogSourceBuilder extends SourceBuilder {
	final public static String NAME = "syslog2";
	final public static String USAGE = NAME + "(tcp|udp[, host[, port[, bufferSize]]])";

	final public static int SYSLOG_PORT = 514;

	@Override
	public EventSource build(Context ctx, String... argv) {
		if (argv.length < 1)
			throw new IllegalArgumentException("usage: " + USAGE);
		
		// So far we only do InetSocketAddress:
		String host = "localhost";
		int port = SYSLOG_PORT;
		int bufferSize = 1 << 16;
		
		if (argv.length >= 2)
			host = argv[1];

		if (argv.length >= 3)
			port = Integer.parseInt(argv[2]);
		
		if (argv.length >= 4)
			bufferSize = Integer.parseInt(argv[3]);
		
		SocketAddress addr = new InetSocketAddress(host, port);
		
		if ("tcp".equals(argv[0]))
			return new SyslogSocketSource(addr);
		else if ("udp".equals(argv[0]))
			return new SyslogDatagramSocketSource(addr, bufferSize);
		else
			throw new IllegalArgumentException("unknown protocol: " + argv[0]);
	}
}
