package org.min.watergap.piping.translator.impl;

import org.min.watergap.common.rdbms.inclog.OrgIncEvent;
import org.min.watergap.common.rdbms.misc.binlog.type.EventType;
import org.min.watergap.piping.translator.PipingEvent;
import org.min.watergap.piping.translator.impl.header.IncreLogHeader;
import org.min.watergap.piping.translator.type.PipingType;

/**
 * 增量日志对象
 *
 * @Create by metaX.h on 2022/5/1 23:55
 */
public class IncreLogEventPipingData implements PipingEvent {

    private OrgIncEvent event;

    private PipingType entryType;

    private IncreLogHeader header;

    public IncreLogEventPipingData(OrgIncEvent event, PipingType entryType) {
        this.event = event;
        this.entryType = entryType;
        header = new IncreLogHeader(EventType.QUERY);
    }

    public IncreLogEventPipingData(OrgIncEvent event, PipingType entryType, EventType eventType) {
        this.event = event;
        this.entryType = entryType;
        this.header = new IncreLogHeader(eventType);
    }

    @Override
    public PipingType getPipingType() {
        return entryType;
    }

    public static PipingEvent createBeginTransaction(OrgIncEvent event) {
        return new IncreLogEventPipingData(event, PipingType.TRANSACTIONBEGIN);
    }

    public static PipingEvent createEndTransaction(OrgIncEvent event) {
        return new IncreLogEventPipingData(event, PipingType.TRANSACTIONEND);
    }

    public static PipingEvent createRowData(OrgIncEvent event, EventType eventType) {
        return new IncreLogEventPipingData(event, PipingType.ROWDATA, eventType);
    }

    public static PipingEvent createRowData(OrgIncEvent event, EventType eventType, String sql, boolean isDDL) {
        IncreLogEventPipingData increLogEventPipingData = new IncreLogEventPipingData(event, PipingType.ROWDATA, eventType);
        increLogEventPipingData.header.setDDL(isDDL);
        increLogEventPipingData.header.setSql(sql);
        return increLogEventPipingData;
    }
}
