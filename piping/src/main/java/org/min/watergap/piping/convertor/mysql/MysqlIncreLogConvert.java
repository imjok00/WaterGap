package org.min.watergap.piping.convertor.mysql;

import org.min.watergap.common.rdbms.inclog.OrgIncEvent;
import org.min.watergap.common.rdbms.misc.binlog.LogEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.*;
import org.min.watergap.common.rdbms.misc.binlog.event.UnknownLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.type.EventType;
import org.min.watergap.common.sql.parse.DdlResult;
import org.min.watergap.common.sql.parse.DruidDdlParser;
import org.min.watergap.common.utils.StringUtils;
import org.min.watergap.piping.convertor.IncreLogConvert;
import org.min.watergap.piping.translator.PipingEvent;
import org.min.watergap.piping.translator.impl.IncreLogEventPipingData;
import org.min.watergap.piping.translator.type.PipingType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;

/**
 * mysql binlog 事件转换成中间结果
 *
 * @Create by metaX.h on 2022/5/29 20:37
 */
public class MysqlIncreLogConvert implements IncreLogConvert {

    public static final String          XA_XID              = "XA_XID";
    public static final String          XA_TYPE             = "XA_TYPE";
    public static final String          XA_START            = "XA START";
    public static final String          XA_END              = "XA END";
    public static final String          XA_COMMIT           = "XA COMMIT";
    public static final String          XA_ROLLBACK         = "XA ROLLBACK";
    public static final String          ISO_8859_1          = "ISO-8859-1";
    public static final String          UTF_8               = "UTF-8";
    public static final int             TINYINT_MAX_VALUE   = 256;
    public static final int             SMALLINT_MAX_VALUE  = 65536;
    public static final int             MEDIUMINT_MAX_VALUE = 16777216;
    public static final long            INTEGER_MAX_VALUE   = 4294967296L;
    public static final BigInteger BIGINT_MAX_VALUE    = new BigInteger("18446744073709551616");
    public static final int             version             = 1;
    public static final String          BEGIN               = "BEGIN";
    public static final String          COMMIT              = "COMMIT";

    public static final Logger logger              = LoggerFactory.getLogger(MysqlIncreLogConvert.class);

    public PipingEvent convert(OrgIncEvent event) {
        LogEvent logEvent = (LogEvent) event;
        if (logEvent == null || logEvent instanceof UnknownLogEvent) {
            return null;
        }

        int eventType = logEvent.getHeader().getType();
        switch (eventType) {
            case LogEvent.QUERY_EVENT:
                return parseQueryEvent((QueryLogEvent) logEvent);
            case LogEvent.XID_EVENT:
                return parseXidEvent((XidLogEvent) logEvent);
            case LogEvent.TABLE_MAP_EVENT:
                parseTableMapEvent((TableMapLogEvent) logEvent);
                break;
            case LogEvent.WRITE_ROWS_EVENT_V1:
            case LogEvent.WRITE_ROWS_EVENT:
                return parseRowsEvent((WriteRowsLogEvent) logEvent);
            case LogEvent.UPDATE_ROWS_EVENT_V1:
            case LogEvent.PARTIAL_UPDATE_ROWS_EVENT:
            case LogEvent.UPDATE_ROWS_EVENT:
                return parseRowsEvent((UpdateRowsLogEvent) logEvent);
            case LogEvent.DELETE_ROWS_EVENT_V1:
            case LogEvent.DELETE_ROWS_EVENT:
                return parseRowsEvent((DeleteRowsLogEvent) logEvent);
            case LogEvent.ROWS_QUERY_LOG_EVENT:
                return parseRowsQueryEvent((RowsQueryLogEvent) logEvent);
            case LogEvent.ANNOTATE_ROWS_EVENT:
                return parseAnnotateRowsEvent((AnnotateRowsEvent) logEvent);
            case LogEvent.USER_VAR_EVENT:
                return parseUserVarLogEvent((UserVarLogEvent) logEvent);
            case LogEvent.INTVAR_EVENT:
                return parseIntrvarLogEvent((IntvarLogEvent) logEvent);
            case LogEvent.RAND_EVENT:
                return parseRandLogEvent((RandLogEvent) logEvent);
            case LogEvent.GTID_LOG_EVENT:
                return parseGTIDLogEvent((GtidLogEvent) logEvent);
            case LogEvent.HEARTBEAT_LOG_EVENT:
                return parseHeartbeatLogEvent((HeartbeatLogEvent) logEvent);
            case LogEvent.GTID_EVENT:
            case LogEvent.GTID_LIST_EVENT:
                return parseMariaGTIDLogEvent(logEvent);
            default:
                break;
        }
        return null;
    }

    private PipingEvent parseQueryEvent(QueryLogEvent event) {
        String queryString = event.getQuery();
        if (StringUtils.startsWithIgnoreCase(queryString, XA_START)) {
            // xa start use TransactionBegin
            return IncreLogEventPipingData.createBeginTransaction(event);
        } else if (StringUtils.startsWithIgnoreCase(queryString, XA_END)) {
            // xa start use TransactionEnd
            return IncreLogEventPipingData.createEndTransaction(event);
        } else if (StringUtils.startsWithIgnoreCase(queryString, XA_COMMIT)) {
            // xa commit
            return IncreLogEventPipingData.createRowData(event, EventType.XACOMMIT);
        } else if (StringUtils.startsWithIgnoreCase(queryString, XA_ROLLBACK)) {
            // xa rollback
            return IncreLogEventPipingData.createRowData(event, EventType.XAROLLBACK);
        } else if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
            return IncreLogEventPipingData.createBeginTransaction(event);
        } else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
            return IncreLogEventPipingData.createEndTransaction(event);
        } else {
            boolean notFilter = false;
            EventType type = EventType.QUERY;
            String tableName = null;
            String schemaName = null;
            List<DdlResult> results = DruidDdlParser.parse(queryString, event.getDbName());
            for (DdlResult result : results) {
//                if (!processFilter(queryString, result)) {
//                    // 只要有一个数据不进行过滤
//                    notFilter = true;
//                }
            }
            if (results.size() > 0) {
                // 如果针对多行的DDL,只能取第一条
                type = results.get(0).getType();
                schemaName = results.get(0).getSchemaName();
                tableName = results.get(0).getTableName();
            }

            if (!notFilter) {
                // 如果是过滤的数据就不处理了
                return null;
            }

            boolean isDml = (type == EventType.INSERT || type == EventType.UPDATE || type == EventType.DELETE);

            if (!isDml) {
                // 使用新的表结构元数据管理方式
//                EntryPosition position = createPosition(event.getHeader());
//                tableMetaCache.apply(position, event.getDbName(), queryString, null);
            }
            return IncreLogEventPipingData.createRowData(event, type, queryString, !isDml);
        }
    }

    private PipingEvent parseXidEvent(XidLogEvent event) {
        return IncreLogEventPipingData.createEndTransaction(event);
    }

}
