package org.min.watergap.common.rdbms.misc.binlog.event;

import org.min.watergap.common.rdbms.misc.binlog.LogBuffer;
import org.min.watergap.common.rdbms.misc.binlog.LogEvent;

/**
 * Logs xid of the transaction-to-be-committed in the 2pc protocol. Has no
 * meaning in replication, slaves ignore it.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class XidLogEvent extends LogEvent {

    private final long xid;

    public XidLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        /* The Post-Header is empty. The Variable Data part begins immediately. */
        buffer.position(descriptionEvent.commonHeaderLen + descriptionEvent.postHeaderLen[XID_EVENT - 1]);
        xid = buffer.getLong64(); // !uint8korr
    }

    public final long getXid() {
        return xid;
    }
}
