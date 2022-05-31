package org.min.watergap.intake.incre.rdbms.mysql.parser.driver.packets.client;

import org.min.watergap.intake.incre.rdbms.mysql.parser.driver.packets.CommandPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AuthSwitchResponsePacket extends CommandPacket {

    public byte[] authData;

    public void fromBytes(byte[] data) {
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(authData);
        return out.toByteArray();
    }

}