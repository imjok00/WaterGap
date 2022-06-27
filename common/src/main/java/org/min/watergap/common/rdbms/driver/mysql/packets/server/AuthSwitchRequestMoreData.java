package org.min.watergap.common.rdbms.driver.mysql.packets.server;


import org.min.watergap.common.rdbms.driver.mysql.packets.CommandPacket;
import org.min.watergap.common.utils.ByteHelper;

import java.io.IOException;

public class AuthSwitchRequestMoreData extends CommandPacket {

    public int    status;
    public byte[] authData;

    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read status
        status = data[index];
        index += 1;
        authData = ByteHelper.readNullTerminatedBytes(data, index);
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

}
