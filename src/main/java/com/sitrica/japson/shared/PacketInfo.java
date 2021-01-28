package com.sitrica.japson.shared;

import java.io.Serializable;

public class PacketInfo implements Serializable {
    public int id;
    public String value;

    public PacketInfo(int id, String value) {
        this.id = id;
        this.value = value;
    }
}
