package com.sitrica.japson.shared;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Sets;
import com.google.common.flogger.FluentLogger;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

public abstract class Japson {

	protected final FluentLogger logger = FluentLogger.forEnclosingClass();
	protected final Set<InetAddress> acceptable = new HashSet<>();
	protected final Set<Handler> handlers = new HashSet<>();
	private final Set<Integer> ignored = new HashSet<>();

	protected int PACKET_SIZE = 64000; // UDP standard
	protected int TIMEOUT = 2000; // milliseconds
	protected String password;
	protected boolean debug;

	public Japson registerHandlers(Handler... handlers) {
		Sets.newHashSet(handlers).stream()
				.filter(handler -> !this.handlers.stream().anyMatch(existing -> existing.getID() == handler.getID()))
				.forEach(handler -> this.handlers.add(handler));
		return this;
	}

	public abstract Japson setAllowedAddresses(InetAddress... addesses);

	public abstract Japson setPacketBufferSize(int buffer);

	public abstract Japson setPassword(String password);

	/**
	 * Set the timeout in milliseconds to wait for returnable packets.
	 * 
	 * @param timeout The time in Milliseconds to set as the timeout.
	 * @return Japson instance for chaining.
	 */
	public abstract Japson setTimeout(int timeout);

	public abstract Japson enableDebug();

	public boolean passwordMatches(String password) {
		return this.password.equals(password);
	}

	public boolean isAllowed(InetAddress address) {
		if (acceptable.isEmpty())
			return true;
		return acceptable.contains(address);
	}

	public Set<Handler> getHandlers() {
		return handlers;
	}

	public FluentLogger getLogger() {
		return logger;
	}

	public boolean hasPassword() {
		return password != null;
	}

	public boolean isDebug() {
		return debug;
	}

	public final void addIgnoreDebugPackets(Integer... packets) {
		ignored.addAll(Sets.newHashSet(packets));
	}

	public final Set<Integer> getIgnoredPackets() {
		return Collections.unmodifiableSet(ignored);
	}

	public <T> T sendPacket(InetSocketAddress address, ReturnablePacket<T> packet) throws TimeoutException, InterruptedException, ExecutionException {
		return sendPacket(address.getAddress(), address.getPort(), packet, new GsonBuilder()
				.enableComplexMapKeySerialization()
				.serializeNulls()
				.setLenient()
				.create());
	}

	public <T> T sendPacket(InetAddress address, int port, ReturnablePacket<T> packet) throws TimeoutException, InterruptedException, ExecutionException {
		return sendPacket(address, port, packet, new GsonBuilder()
				.enableComplexMapKeySerialization()
				.serializeNulls()
				.setLenient()
				.create());
	}

	public <T> T sendPacket(InetSocketAddress address, ReturnablePacket<T> japsonPacket, Gson gson) throws TimeoutException, InterruptedException, ExecutionException {
		return sendPacket(address.getAddress(), address.getPort(), japsonPacket, gson);
	}

	public <T> T sendPacket(InetAddress address, int port, ReturnablePacket<T> japsonPacket, Gson gson) throws TimeoutException, InterruptedException, ExecutionException {
		return CompletableFuture.supplyAsync(() -> {
			try {
				Socket socket = new Socket();
				InetSocketAddress socketAddress = new InetSocketAddress(address.getHostAddress(), port);
				socket.connect(socketAddress);

				DataInputStream inputStream = new DataInputStream(socket.getInputStream());
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());

				String data = gson.toJson(japsonPacket.toJson());
				PacketInfo packet = new PacketInfo(japsonPacket.getID(), data);
				outputStream.writeObject(packet);
				outputStream.flush();

//				ByteArrayDataOutput out = ByteStreams.newDataOutput();
//				out.writeInt(japsonPacket.getID());
//				out.writeUTF(gson.toJson(japsonPacket.toJson()));
//				byte[] buf = out.toByteArray();
//				socket.setSoTimeout(TIMEOUT);
//				outputStream.write(buf);
//				outputStream.flush();

				// Reset the byte buffer
//				buf = new byte[PACKET_SIZE];
				if (inputStream == null) {
					logger.atSevere().log("Packet with id %s returned null or an incorrect readable object for Japson", japsonPacket.getID());
					return null;
				}
				int id = inputStream.readInt();
				if (id != japsonPacket.getID()) {
					logger.atSevere().log("Sent returnable packet with id %s, but did not get correct packet id returned", japsonPacket.getID());
					return null;
				}
				String json = inputStream.readUTF();
				if (debug && (ignored.isEmpty() || !ignored.contains(japsonPacket.getID())))
					logger.atInfo().log("Sent returnable packet with id %s and recieved %s", japsonPacket.getID(), json);
				outputStream.close();
				inputStream.close();
				socket.close();
				return japsonPacket.getObject(new JsonParser().parse(json).getAsJsonObject());
			} catch (SocketException socketException) {
				logger.atSevere().withCause(socketException)
						.atMostEvery(15, TimeUnit.SECONDS)
						.log("Socket error: " + socketException.getMessage());
			} catch (IOException exception) {
				logger.atSevere().withCause(exception)
						.atMostEvery(15, TimeUnit.SECONDS)
						.log("IO error: " + exception.getMessage());
			}
			return null;
		}).get(TIMEOUT, TimeUnit.MILLISECONDS);
	}

	public void sendPacket(InetSocketAddress address, Packet japsonPacket) throws InterruptedException, ExecutionException, TimeoutException {
		sendPacket(address.getAddress(), address.getPort(), japsonPacket, new GsonBuilder()
				.enableComplexMapKeySerialization()
				.serializeNulls()
				.setLenient()
				.create());
	}

	public void sendPacket(InetAddress address, int port, Packet japsonPacket) throws InterruptedException, ExecutionException, TimeoutException {
		sendPacket(address, port, japsonPacket, new GsonBuilder()
				.enableComplexMapKeySerialization()
				.serializeNulls()
				.setLenient()
				.create());
	}

	public void sendPacket(InetSocketAddress address, Packet japsonPacket, Gson gson) throws InterruptedException, ExecutionException, TimeoutException {
		sendPacket(address.getAddress(), address.getPort(), japsonPacket, gson);
	}

	public void sendPacket(InetAddress address, int port, Packet japsonPacket, Gson gson) throws InterruptedException, ExecutionException, TimeoutException {
		CompletableFuture.runAsync(() -> {
			try {
				Socket socket = new Socket();
				InetSocketAddress socketAddress = new InetSocketAddress(address.getHostAddress(), port);
				socket.connect(socketAddress);

				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());

				String data = gson.toJson(japsonPacket.toJson());
				PacketInfo packet = new PacketInfo(japsonPacket.getID(), data);
				outputStream.writeObject(packet);
				outputStream.flush();

				socket.setSoTimeout(TIMEOUT);
				if (debug && (ignored.isEmpty() || !ignored.contains(japsonPacket.getID())))
					logger.atInfo().log("Sent non-returnable packet with id %s and data %s", japsonPacket.getID(), data);
				outputStream.close();
				socket.close();
			} catch (SocketException socketException) {
				logger.atSevere().withCause(socketException)
						.atMostEvery(15, TimeUnit.SECONDS)
						.log("Socket error: " + socketException.getMessage());
			} catch (IOException exception) {
				logger.atSevere().withCause(exception)
						.atMostEvery(15, TimeUnit.SECONDS)
						.log("IO error: " + exception.getMessage());
			}
		}).get(TIMEOUT, TimeUnit.MILLISECONDS);
	}

}
