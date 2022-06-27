package org.min.watergap.common.rdbms.driver.mysql.utils;

import org.min.watergap.common.rdbms.driver.mysql.packets.HeaderPacket;
import org.min.watergap.common.rdbms.driver.mysql.socket.SocketChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Todo
 *
 * @Create by metaX.h on 2022/5/21 4:58
 */
public class PacketManager {

    public static HeaderPacket readHeader(SocketChannel ch, int len) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.fromBytes(ch.read(len));
        return header;
    }

    public static HeaderPacket readHeader(SocketChannel ch, int len, int timeout) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.fromBytes(ch.read(len, timeout));
        return header;
    }

    public static byte[] readBytes(SocketChannel ch, int len) throws IOException {
        return ch.read(len);
    }

    public static byte[] readBytes(SocketChannel ch, int len, int timeout) throws IOException {
        return ch.read(len, timeout);
    }

    public static void writePkg(SocketChannel ch, byte[]... srcs) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] src : srcs) {
            out.write(src);
        }
        ch.write(out.toByteArray());
    }

    public static void writeBody(SocketChannel ch, byte[] body) throws IOException {
        writeBody0(ch, body, (byte) 0);
    }

    public static void writeBody0(SocketChannel ch, byte[] body, byte packetSeqNumber) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(body.length);
        header.setPacketSequenceNumber(packetSeqNumber);
        ch.write(header.toBytes(), body);
    }

}
