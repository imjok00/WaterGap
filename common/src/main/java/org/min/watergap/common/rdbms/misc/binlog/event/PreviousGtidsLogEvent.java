package org.min.watergap.common.rdbms.misc.binlog.event;

import org.min.watergap.common.rdbms.misc.binlog.LogBuffer;
import org.min.watergap.common.rdbms.misc.binlog.BaseLogEvent;

/**
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6
 */
public class PreviousGtidsLogEvent extends BaseLogEvent {

    public PreviousGtidsLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);
        // do nothing , just for mysql gtid search function
    }
}
