package com.spotify.flume.syslog2;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;

import com.cloudera.flume.core.Event;


/**
 * A Flume event source backed by a streaming socket and a syslog parser.
**/
public class SyslogSocketSource extends ServerSocketSource {
	public SyslogSocketSource(SocketAddress addr) {
		// 1 in backlog. We expect at most one connection anyway.
		super(addr, 1);
	}
	
	@Override
	protected SocketSource createSocketSource(Socket socket) throws IOException {
		return new SyslogSocketSourceImpl(socket);
	}
	
	private class SyslogSocketSourceImpl implements SocketSource {
		private Socket socket;
		private SyslogParser parser;
		
		public SyslogSocketSourceImpl(Socket socket) throws IOException {
			this.socket = socket;
			parser = new SyslogParser(new BufferedInputStream(socket.getInputStream()));
		}

		@Override
		public void close() throws IOException, InterruptedException {
			parser.close();
			socket.close();
		}
		
		@Override
		public Event next() throws IOException, InterruptedException {
			return parser.readEvent();
		}

		@Override
		public int recover() throws IOException, InterruptedException {
			parser.skipLine();

			return 1;
		}
	}
}
