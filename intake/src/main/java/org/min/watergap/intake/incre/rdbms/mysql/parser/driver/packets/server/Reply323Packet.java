package org.min.watergap.intake.incre.rdbms.mysql.parser.driver.packets.server;

import org.min.watergap.common.utils.ByteHelper;
import org.min.watergap.intake.incre.rdbms.mysql.parser.driver.packets.PacketWithHeaderPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Reply323Packet extends PacketWithHeaderPacket {

    public byte[] seed;

    public void fromBytes(byte[] data) throws IOException {

    }

    public byte[] toBytes() throws IOException {
        if (seed == null) {
            return new byte[] { (byte) 0 };
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteHelper.writeNullTerminated(seed, out);
            return out.toByteArray();
        }
    }

}
