package org.min.watergap.intake.incre.rdbms.mysql.parser.driver.packets;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.min.watergap.intake.incre.rdbms.mysql.parser.utils.CanalToStringStyle;

public abstract class CommandPacket implements IPacket {

    private byte command;

    // arg

    public void setCommand(byte command) {
        this.command = command;
    }

    public byte getCommand() {
        return command;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
