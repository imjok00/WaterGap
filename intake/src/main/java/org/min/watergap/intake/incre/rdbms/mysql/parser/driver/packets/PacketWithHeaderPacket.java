package org.min.watergap.intake.incre.rdbms.mysql.parser.driver.packets;


import org.apache.commons.lang3.builder.ToStringBuilder;
import org.min.watergap.intake.incre.rdbms.mysql.parser.utils.CanalToStringStyle;

public abstract class PacketWithHeaderPacket implements IPacket {

    protected HeaderPacket header;

    protected PacketWithHeaderPacket(){
    }

    protected PacketWithHeaderPacket(HeaderPacket header){
        setHeader(header);
    }

    public void setHeader(HeaderPacket header) {
        if (header == null) {
            throw new NullPointerException();
        }
        this.header = header;
    }

    public HeaderPacket getHeader() {
        return header;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
