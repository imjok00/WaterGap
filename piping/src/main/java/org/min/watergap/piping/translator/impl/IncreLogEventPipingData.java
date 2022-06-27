package org.min.watergap.piping.translator.impl;

import org.min.watergap.common.config.DatabaseType;
import org.min.watergap.common.position.Position;
import org.min.watergap.common.position.incre.MysqlIncrePosition;
import org.min.watergap.common.rdbms.inclog.*;
import org.min.watergap.common.rdbms.misc.binlog.type.EventType;
import org.min.watergap.common.sql.parse.DdlResult;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.piping.translator.PipingEvent;
import org.min.watergap.piping.translator.impl.header.IncreLogHeader;
import org.min.watergap.piping.translator.type.PipingType;

import java.util.List;
import java.util.Map;

/**
 * 增量日志对象
 *
 * @Create by metaX.h on 2022/5/1 23:55
 */
public class IncreLogEventPipingData implements PipingEvent {

    private List<IncEvent> events = CollectionsUtils.emptyList();

    private PipingType pipingType;

    private IncreLogHeader header;

    public IncreLogEventPipingData() {

    }

    public IncreLogEventPipingData(IncEvent event, PipingType entryType, String schemaName) {
         this(event, entryType, EventType.QUERY, schemaName);
    }

    public IncreLogEventPipingData(IncEvent event, PipingType pipingType, EventType eventType) {
        this.events.add(event);
        this.pipingType = pipingType;
        this.header = new IncreLogHeader(eventType);
    }

    public IncreLogEventPipingData(IncEvent event, PipingType entryType, EventType eventType, String schemaName, String tableName) {
        this.events.add(event);
        this.pipingType = entryType;
        this.header = new IncreLogHeader(eventType);
        header.setSchemaName(schemaName);
        header.setTableName(tableName);
    }

    public IncreLogEventPipingData(IncEvent event, PipingType entryType, EventType eventType, String schemaName) {
        this.events.add(event);
        this.pipingType = entryType;
        this.header = new IncreLogHeader(eventType);
        header.setSchemaName(schemaName);
    }

    @Override
    public PipingType getPipingType() {
        return pipingType;
    }

    @Override
    public IncreLogHeader getHeader() {
        return header;
    }

    public static PipingEvent createBeginTransaction(String schema) {
        return new IncreLogEventPipingData(new BeginTransIncEvent(), PipingType.TRANSACTIONBEGIN, schema);
    }

    public static PipingEvent createEndTransaction(String schema) {
        return new IncreLogEventPipingData(new EndTransIncEvent(), PipingType.TRANSACTIONEND, schema);
    }

    public static PipingEvent createXidEvent(long xid) {
        return new IncreLogEventPipingData(new EndTransIncEvent(xid), PipingType.TRANSACTIONEND, "");
    }

    public static PipingEvent createXACommit(String schema) {
        return new IncreLogEventPipingData(new EndTransIncEvent(), PipingType.TRANSACTIONEND, schema);
    }

    public static PipingEvent createXARollBack(String schema) {
        return new IncreLogEventPipingData(new EndTransIncEvent(), PipingType.TRANSACTIONEND, EventType.XAROLLBACK, schema);
    }

    public static PipingEvent createRowData(String schema, String tableName, EventType eventType) {
        return new IncreLogEventPipingData(new RowUpdateDataIncEvent(), PipingType.ROWDATA, eventType, schema, tableName);
    }

    public static PipingEvent createDDLEvent(String schema, List<DdlResult> ddlResults) {
        return new IncreLogEventPipingData(new DDLIncEvent(schema, ddlResults), PipingType.DDLDATA, schema);
    }

    public static PipingEvent createQueryEvent(String schema, String tableName, String sql) {
        return new IncreLogEventPipingData(new QueryLogIncEvent(sql), PipingType.ROWDATA, EventType.QUERY, schema, tableName);
    }

    public void addRowDatas(IncEvent incEvent) {
        events.add(incEvent);
    }

    public static PipingEvent createPositionEvent(String positionStr, DatabaseType databaseType, Map<String, String> properties) {
        Position position = null;
        switch (databaseType) {
            case MySQL:
                position = new MysqlIncrePosition(positionStr);

        }
        PositionIncEvent positionIncEvent = new PositionIncEvent(position);
        if (properties != null) {
            for (String key : properties.keySet()) {
                positionIncEvent.addProperties(key, properties.get(key));
            }
        }
       return new IncreLogEventPipingData(positionIncEvent, PipingType.GTIDLOG, EventType.GTID);
    }

    public static PipingEvent createHeartbeatEvent() {
        return new IncreLogEventPipingData(new HeartbeatIncEvent(), PipingType.ROWDATA, EventType.MHEARTBEAT);
    }
}
