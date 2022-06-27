package org.min.watergap.common.rdbms.misc.binlog.event.mariadb;

import org.min.watergap.common.rdbms.misc.binlog.LogBuffer;
import org.min.watergap.common.rdbms.misc.binlog.BaseLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.FormatDescriptionLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.LogHeader;

/**
 * mariadb的Start_encryption_log_event
 * 
 * @author agapple 2018年5月7日 下午7:23:02
 * @version 1.0.26
 */
public class StartEncryptionLogEvent extends BaseLogEvent {

    public StartEncryptionLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);
    }
}
