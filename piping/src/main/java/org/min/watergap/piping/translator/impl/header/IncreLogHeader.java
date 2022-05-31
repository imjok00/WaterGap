package org.min.watergap.piping.translator.impl.header;

import org.min.watergap.common.rdbms.misc.binlog.type.EventType;

/**
 * 增量日志header
 *
 * @Create by metaX.h on 2022/5/30 23:56
 */
public class IncreLogHeader {

    protected EventType eventType;

    protected boolean isDDL;

    protected String sql;

    public IncreLogHeader(EventType eventType) {
        this.eventType = eventType;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public boolean isDDL() {
        return isDDL;
    }

    public void setDDL(boolean DDL) {
        isDDL = DDL;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
