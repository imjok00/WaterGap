package org.min.watergap.common.rdbms.misc.binlog.event;

import org.min.watergap.common.rdbms.misc.binlog.BaseLogEvent;

/**
 * Unknown_log_event
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class UnknownLogEvent extends BaseLogEvent {

    public UnknownLogEvent(LogHeader header){
        super(header);
    }
}
