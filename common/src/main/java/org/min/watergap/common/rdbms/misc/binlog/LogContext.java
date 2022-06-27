package org.min.watergap.common.rdbms.misc.binlog;

import org.min.watergap.common.position.incre.GTIDSet;
import org.min.watergap.common.rdbms.misc.binlog.event.FormatDescriptionLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.TableMapLogEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Document Me!! NOTE: Log context will NOT write multi-threaded.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class LogContext {

    private final Map<Long, TableMapLogEvent> mapOfTable = new HashMap<>();

    private FormatDescriptionLogEvent formatDescription;

    private LogPosition                       logPosition;

    private GTIDSet gtidSet;

    private BaseLogEvent gtidLogEvent; // save current gtid log event

    public LogContext(){
        this.formatDescription = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
    }

    public LogContext(FormatDescriptionLogEvent descriptionEvent){
        this.formatDescription = descriptionEvent;
    }

    public final LogPosition getLogPosition() {
        return logPosition;
    }

    public final void setLogPosition(LogPosition logPosition) {
        this.logPosition = logPosition;
    }

    public final FormatDescriptionLogEvent getFormatDescription() {
        return formatDescription;
    }

    public final void setFormatDescription(FormatDescriptionLogEvent formatDescription) {
        this.formatDescription = formatDescription;
    }

    public final void putTable(TableMapLogEvent mapEvent) {
        mapOfTable.put(Long.valueOf(mapEvent.getTableId()), mapEvent);
    }

    public final TableMapLogEvent getTable(final long tableId) {
        return mapOfTable.get(Long.valueOf(tableId));
    }

    public final void clearAllTables() {
        mapOfTable.clear();
    }

    public void reset() {
        formatDescription = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
        mapOfTable.clear();
    }

    public GTIDSet getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(GTIDSet gtidSet) {
        this.gtidSet = gtidSet;
    }

    public BaseLogEvent getGtidLogEvent() {
        return gtidLogEvent;
    }

    public void setGtidLogEvent(BaseLogEvent gtidLogEvent) {
        this.gtidLogEvent = gtidLogEvent;
    }
}
