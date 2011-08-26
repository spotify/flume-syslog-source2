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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Set;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Flume event source backed by a server socket.
 *
 * The source will listen for incoming connections and dispatch the sockets to
 * one thread each. A blocking queue is used to collect events from the threads.
 */
public abstract class ServerSocketSource extends BaseSource {
	static final Logger LOG = LoggerFactory.getLogger(ServerSocketSource.class);

	/// Object used to wake up the next() call.
	static final Event WAKE_EVENT = new EventImpl();
	
	private SocketAddress addr;
	private int backlog;
	private boolean opened = false;
	private boolean accepting = false;
	private ServerSocket socket;
	private Thread acceptorThread;
	private Set<Processor> processors = new HashSet<Processor>();
	private BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(2);
	
	/**
	 * Construct a new source.
	 *
	 * @param addr the address to bind to and listen for connections on.
	 * @param backlog the maximum number of outstanding incoming connections.
	 */
	public ServerSocketSource(SocketAddress addr, int backlog) {
		this.addr = addr;
		this.backlog = backlog;
	}
	
	@Override
	public void open() throws IOException {
		opened = true;
		accepting = true;
		socket = createServerSocket(addr, backlog);

		acceptorThread = new Thread("socket-acceptor-" + addr) {
			public void run() {
				processServerSocket();
			}
		};
		
		acceptorThread.start();
	}

	/**
	 * Create a new server socket for this source.
	 *
	 * @param addr the address to bind to and listen for connections on.
	 * @param backlog the maximum number of outstanding incoming connections.
	 * @return a ServerSocket ready for accept()ing.
	 */
	protected ServerSocket createServerSocket(SocketAddress addr, int backlog) throws IOException {
		// For some reason ServerSocket doesn't take a SocketAddress.
		InetSocketAddress saddr = (InetSocketAddress) addr;
		ServerSocket s = new ServerSocket(saddr.getPort(), backlog, saddr.getAddress());
		
		return s;
	}
	
	@Override
	public Event next() throws IOException, InterruptedException {
		for (;;) {
			// Somewhat racy, but I don't think it matters.
			// The acceptor thread shouldn't normally die anyway.
			if (processors.isEmpty() && !accepting)
				return null;

			Event e = eventQueue.take();
			
			// If we got a wake event, re-evaluate our situation.
			if (e == WAKE_EVENT)
				continue;

			updateEventProcessingStats(e);

			return e;
		}
	}
	
	@Override
	public void close() throws IOException, InterruptedException {
		opened = false;
		socket.close();
		socket = null;
		acceptorThread.join();
	
		// Create a copy as the threads will be mutating the set.
		for (Processor p : new HashSet<Processor>(processors))
			p.close();
	}

	/**
	 * Schedule a wake up of the next() call.
	 */
	private void wakeUp() {
		eventQueue.offer(WAKE_EVENT);
	}
	
	/**
	 * Process incoming connections on the server socket.
	 *
	 * Creates a new Processor per connection and starts it.
	 */
	private void processServerSocket() {
		try {
			for (;;) {
				final Socket s = socket.accept();
				Processor p = new Processor(s);

				processors.add(p);
				p.start();
			}
		} catch (Exception ex) {
			if (opened) LOG.error("Acceptor failed", ex);
		} finally {
			accepting = false;
			wakeUp();
		}
	}
	
	/**
	 * Create a new socket source for the given socket.
	 *
	 * The socket is connected and ready to be read from/written to.
	 */
	protected abstract SocketSource createSocketSource(Socket socket) throws IOException;
	
	/**
	 * One thread per streaming socket.
	 */
	private class Processor extends Thread {
		private Socket socket;
		
		public Processor(Socket socket) {
			super("socket-processor-" + socket.getRemoteSocketAddress());
			this.socket = socket;
		}

		/**
		 * Close the processor.
		 *
		 * This stops the processor, closes the socket and frees
		 * all resources.
		 */
		public void close() throws IOException, InterruptedException {
			socket.close();
			socket = null;
			interrupt(); // For eventQueue.put()
			join();
		}
		
		public void run() {
			try {
				SocketSource source = createSocketSource(socket);
				
				try {
					for (;;) {
						Event e;

						try {
							e = source.next();
						} catch (Exception ex) {
							addRejectedMessages(source.recover());
							continue;
						}

						// EOF is fine if we are not opened.
						if (e == null || !opened)
							break;
			
						eventQueue.put(e);
					}
				} finally {
					source.close();
				}
			} catch (Exception ex) {
				if (opened) LOG.error("Processor failed", ex);
			} finally {
				processors.remove(this);
				wakeUp();
			}
		}
	}
	
	/**
	 * A source for stream sockets.
	 *
	 * Note that since the socket is already connected, we skip the open()
	 * method.
	 *
	 * @see com.cloudera.flume.core.EventSource
	 */
	public static interface SocketSource {
		/**
		 * Close the socket and free any resources used by this source.
		 *
		 * Calls to any method after close() yields undefined behaviour.
		 */
		public void close() throws IOException, InterruptedException;
		
		/**
		 * Return the next event, waiting if necessary.
		 */
		public Event next() throws IOException, InterruptedException;
		
		/**
		 * Attempt to recover from a failure.
		 *
		 * This will be called if next() throws an exception. If this
		 * method throws an exception, the socket will be closed.
		 *
		 * @return the number of rejected/skipped messages.
		 */
		public int recover() throws IOException, InterruptedException;
	}
}
