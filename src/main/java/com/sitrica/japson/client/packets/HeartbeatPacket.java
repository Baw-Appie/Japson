package com.sitrica.japson.client.packets;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sitrica.japson.shared.Packet;

public class HeartbeatPacket extends Packet {

	private final String password;

	public HeartbeatPacket(String password) {
		super(0x00);
		this.password = password;
	}

	@Override
	public String toJson(Gson gson) {
		JsonObject object = new JsonObject();
		if (password != null)
			object.addProperty("password", password);
		return gson.toJson(object);
	}

}
