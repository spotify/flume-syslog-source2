package com.spotify.flume.syslog2;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;

import com.cloudera.flume.core.Event;


/**
 * A Flume event source backed by a streaming socket and a syslog parser.
**/
public class SyslogSocketSource extends BaseSource {
	private SocketAddress addr;
	private Socket socket;
	private SyslogParser parser;
	
	public SyslogSocketSource(SocketAddress addr) {
		this.addr = addr;
	}
	
	@Override
	public void open() throws IOException {
		socket = createSocket();
		// TODO: Buffering?
		parser = new SyslogParser(socket.getInputStream());
	}

	protected Socket createSocket() throws IOException {
		Socket s = new Socket();
		
		s.connect(addr);
		
		return s;
	}
	
	@Override
	public Event next() throws IOException {
		for (;;) {
			try {
				Event e = parser.readEvent();
				
				updateEventProcessingStats(e);

				return e;
			} catch (IOException ex) {
				addRejectedMessage();
				parser.skipLine();
			}
		}
	}
	
	@Override
	public void close() throws IOException {
		parser.close();
		parser = null;
		socket.close();
		socket = null;
	}
}
