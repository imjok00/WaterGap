package org.min.watergap.piping.translator.impl.header;

import org.min.watergap.common.rdbms.misc.binlog.type.EventType;

/**
 * 增量日志header
 *
 * @Create by metaX.h on 2022/5/30 23:56
 */
public class IncreLogHeader {

    protected EventType eventType;

    protected String schemaName;

    protected String tableName;

    public IncreLogHeader(EventType eventType) {
        this.eventType = eventType;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
