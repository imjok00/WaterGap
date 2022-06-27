package org.min.watergap.piping.convertor.mysql;

import org.min.watergap.common.config.DatabaseType;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.rdbms.inclog.BaseIncEvent;
import org.min.watergap.common.rdbms.inclog.RowUpdateDataIncEvent;
import org.min.watergap.common.rdbms.misc.LogEvent;
import org.min.watergap.common.rdbms.misc.binlog.BaseLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.*;
import org.min.watergap.common.rdbms.misc.binlog.event.mariadb.AnnotateRowsEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.mariadb.MariaGtidListLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.mariadb.MariaGtidLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.type.EventType;
import org.min.watergap.common.sql.parse.DdlResult;
import org.min.watergap.common.sql.parse.DruidDdlParser;
import org.min.watergap.common.utils.StringUtils;
import org.min.watergap.piping.convertor.IncreLogConvert;
import org.min.watergap.piping.translator.PipingEvent;
import org.min.watergap.piping.translator.impl.IncreLogEventPipingData;
import org.min.watergap.piping.translator.impl.TableStructBasePipingData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private Charset charset             = Charset.defaultCharset();

    public static final Logger logger              = LoggerFactory.getLogger(MysqlIncreLogConvert.class);

    private Map<String, TableStructBasePipingData> tableMetaCache;

    public MysqlIncreLogConvert(Charset charset) {
        this.charset = charset;
    }

    public PipingEvent convert(BaseLogEvent event) {
        if (event == null || event instanceof UnknownLogEvent) {
            return null;
        }

        int eventType = event.getHeader().getType();
        switch (eventType) {
            case BaseLogEvent.QUERY_EVENT:
                return parseQueryEvent((QueryLogEvent) event);
            case BaseLogEvent.XID_EVENT:
                return parseXidEvent((XidLogEvent) event);
            case BaseLogEvent.TABLE_MAP_EVENT:
                parseTableMapEvent((TableMapLogEvent) event);
                break;
            case BaseLogEvent.WRITE_ROWS_EVENT_V1:
            case BaseLogEvent.WRITE_ROWS_EVENT:
                return parseRowsEvent((WriteRowsLogEvent) event);
            case BaseLogEvent.UPDATE_ROWS_EVENT_V1:
            case BaseLogEvent.PARTIAL_UPDATE_ROWS_EVENT:
            case BaseLogEvent.UPDATE_ROWS_EVENT:
                return parseRowsEvent((UpdateRowsLogEvent) event);
            case BaseLogEvent.DELETE_ROWS_EVENT_V1:
            case BaseLogEvent.DELETE_ROWS_EVENT:
                return parseRowsEvent((DeleteRowsLogEvent) event);
            case BaseLogEvent.ROWS_QUERY_LOG_EVENT:
                return parseRowsQueryEvent((RowsQueryLogEvent) event);
            case BaseLogEvent.ANNOTATE_ROWS_EVENT:
                return parseAnnotateRowsEvent((AnnotateRowsEvent) event);
            case BaseLogEvent.USER_VAR_EVENT:
                return parseUserVarLogEvent((UserVarLogEvent) event);
            case BaseLogEvent.INTVAR_EVENT:
                return parseIntrvarLogEvent((IntvarLogEvent) event);
            case BaseLogEvent.RAND_EVENT:
                return parseRandLogEvent((RandLogEvent) event);
            case BaseLogEvent.GTID_LOG_EVENT:
                return parseGTIDLogEvent((GtidLogEvent) event);
            case BaseLogEvent.HEARTBEAT_LOG_EVENT:
                return parseHeartbeatLogEvent();
            case BaseLogEvent.GTID_EVENT:
            case BaseLogEvent.GTID_LIST_EVENT:
                return parseMariaGTIDLogEvent(event);
            default:
                break;
        }
        return null;
    }

    private PipingEvent parseQueryEvent(QueryLogEvent event) {
        String queryString = event.getQuery();
        if (StringUtils.startsWithIgnoreCase(queryString, XA_START)) {
            // xa start use TransactionBegin
            return IncreLogEventPipingData.createBeginTransaction(event.getDbName());
        } else if (StringUtils.startsWithIgnoreCase(queryString, XA_END)) {
            // xa start use TransactionEnd
            return IncreLogEventPipingData.createEndTransaction(event.getDbName());
        } else if (StringUtils.startsWithIgnoreCase(queryString, XA_COMMIT)) {
            // xa commit
            return IncreLogEventPipingData.createXACommit(event.getDbName());
        } else if (StringUtils.startsWithIgnoreCase(queryString, XA_ROLLBACK)) {
            // xa rollback
            return IncreLogEventPipingData.createXARollBack(event.getDbName());
        } else if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
            return IncreLogEventPipingData.createBeginTransaction(event.getDbName());
        } else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
            return IncreLogEventPipingData.createEndTransaction(event.getDbName());
        } else {
            EventType type = EventType.QUERY;
            String tableName = null;
            String schemaName = null;
            List<DdlResult> results = DruidDdlParser.parse(queryString, event.getDbName());
            if (results.size() > 0) {
                // 如果针对多行的DDL,只能取第一条
                type = results.get(0).getType();
                schemaName = results.get(0).getSchemaName();
                tableName = results.get(0).getTableName();
            }
            boolean isDml = (type == EventType.INSERT || type == EventType.UPDATE || type == EventType.DELETE);

            if (!isDml) {
                // DDL 语句更新到中间结果
                String fullname = schemaName + "." + tableName;
                TableStructBasePipingData tableStructBasePipingData = tableMetaCache.get(fullname);
                tableStructBasePipingData.applyTableMeta(results);
            }
            return IncreLogEventPipingData.createDDLEvent(schemaName, results);
        }
    }

    private PipingEvent parseXidEvent(XidLogEvent event) {
        return IncreLogEventPipingData.createXidEvent(event.getXid());
    }

    private PipingEvent parseTableMapEvent(TableMapLogEvent event) {
        try {
            String charsetDbName = new String(event.getDbName().getBytes(ISO_8859_1), charset.name());
            event.setDbname(charsetDbName);

            String charsetTbName = new String(event.getTableName().getBytes(ISO_8859_1), charset.name());
            event.setTblname(charsetTbName);
            return null;
        } catch (UnsupportedEncodingException e) {
            throw new WaterGapException("UnsupportedEncoding", e);
        }
    }

    public PipingEvent parseRowsEvent(RowsLogEvent event) {
        try {
            TableStructBasePipingData tableMeta = parseRowsEventForTableMeta(event);

            if (tableMeta == null) {
                // 拿不到表结构,执行忽略
                return null;
            }

            EventType eventType = null;
            int type = event.getHeader().getType();
            if (BaseLogEvent.WRITE_ROWS_EVENT_V1 == type || BaseLogEvent.WRITE_ROWS_EVENT == type) {
                eventType = EventType.INSERT;
            } else if (BaseLogEvent.UPDATE_ROWS_EVENT_V1 == type || BaseLogEvent.UPDATE_ROWS_EVENT == type
                    || BaseLogEvent.PARTIAL_UPDATE_ROWS_EVENT == type) {
                eventType = EventType.UPDATE;
            } else if (BaseLogEvent.DELETE_ROWS_EVENT_V1 == type || BaseLogEvent.DELETE_ROWS_EVENT == type) {
                eventType = EventType.DELETE;
            } else {
                throw new WaterGapException("unsupport event type :" + event.getHeader().getType(), "");
            }
            IncreLogEventPipingData resultEvent = (IncreLogEventPipingData)IncreLogEventPipingData.createRowData(tableMeta.getSchemaName(), tableMeta.getTableName(), eventType);
            RowUpdateDataIncEvent incEvent = new RowUpdateDataIncEvent();

            RowsLogBuffer buffer = event.getRowsBuf(charset.name());
            BitSet columns = event.getColumns();
            BitSet changeColumns = event.getChangeColumns();

            boolean tableError = false;
            int rowsCount = 0;
            while (buffer.nextOneRow(columns, false)) {
                // 处理row记录
                if (EventType.INSERT == eventType) {
                    // insert的记录放在before字段中
                    tableError |= parseOneRow(incEvent, event, buffer, columns, true, tableMeta);
                } else if (EventType.DELETE == eventType) {
                    // delete的记录放在before字段中
                    tableError |= parseOneRow(incEvent, event, buffer, columns, false, tableMeta);
                } else {
                    // update需要处理before/after
                    tableError |= parseOneRow(incEvent, event, buffer, columns, false, tableMeta);
                    if (!buffer.nextOneRow(changeColumns, true)) {
                        resultEvent.addRowDatas(incEvent);
                        break;
                    }

                    tableError |= parseOneRow(incEvent, event, buffer, changeColumns, true, tableMeta);
                }

                rowsCount++;
                resultEvent.addRowDatas(incEvent);
            }

            return resultEvent;
        } catch (Exception e) {
            throw new WaterGapException("parse row data failed.", e);
        }
    }


    public TableStructBasePipingData parseRowsEventForTableMeta(RowsLogEvent event) {
        TableMapLogEvent table = event.getTable();
        if (table == null) {
            // tableId对应的记录不存在
            throw new WaterGapException("not found tableId:" + event.getTableId(), "");
        }


        String fullname = table.getDbName() + "." + table.getTableName();

        TableStructBasePipingData tableMeta = tableMetaCache.get(fullname);
        if (tableMeta == null) {
            throw new WaterGapException("not found table meta " + fullname, "");
        }

        return tableMeta;
    }

    private boolean isAliSQLHeartBeat(String schema, String table) {
        return "heartbeat".equalsIgnoreCase(table);
    }

    private boolean parseOneRow(RowUpdateDataIncEvent incEvent, RowsLogEvent event, RowsLogBuffer buffer, BitSet cols,
                                boolean isAfter, TableStructBasePipingData tableMeta) throws UnsupportedEncodingException {

        int columnCnt = event.getTable().getColumnCnt();
        TableMapLogEvent.ColumnInfo[] columnInfo = event.getTable().getColumnInfo();
        // mysql8.0针对set @@global.binlog_row_metadata='FULL' 可以记录部分的metadata信息
        boolean existOptionalMetaData = event.getTable().isExistOptionalMetaData();
        boolean tableError = false;

        for (int i = 0; i < columnCnt; i++) {
            TableMapLogEvent.ColumnInfo info = columnInfo[i];
            // mysql 5.6开始支持nolob/mininal类型,并不一定记录所有的列,需要进行判断
            if (!cols.get(i)) {
                continue;
            }

            TableStructBasePipingData.Column fieldMeta = tableMeta.getColumnByName(info.name);
            BaseIncEvent.ColumnInfo columnBuilder = new BaseIncEvent.ColumnInfo();
            if (fieldMeta != null) {
                columnBuilder.setName(fieldMeta.getColumnName());
                columnBuilder.setKey(fieldMeta.isFullKey());
                // 增加mysql type类型,issue 73
                columnBuilder.setSqlType(fieldMeta.getColumnType());
            } else if (existOptionalMetaData) {
                columnBuilder.setName(info.name);
                columnBuilder.setKey(info.pk);
                // mysql8.0里没有mysql type类型
                // columnBuilder.setMysqlType(fieldMeta.getColumnType());
            }
            columnBuilder.setIndex(i);
            columnBuilder.setNull(false);

            // fixed issue
            // https://github.com/alibaba/canal/issues/66，特殊处理binary/varbinary，不能做编码处理
            boolean isBinary = false;
            if (fieldMeta != null) {
                if (StringUtils.containsIgnoreCase(fieldMeta.getTypeName(), "VARBINARY")) {
                    isBinary = true;
                } else if (StringUtils.containsIgnoreCase(fieldMeta.getTypeName(), "BINARY")) {
                    isBinary = true;
                }
            }

            buffer.nextValue(columnBuilder.getName(), i, info.type, info.meta, isBinary);
            int javaType = buffer.getJavaType();
            if (buffer.isNull()) {
                columnBuilder.setNull(true);
            } else {
                final Serializable value = buffer.getValue();
                // 处理各种类型
                switch (javaType) {
                    case Types.INTEGER:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                        // 处理unsigned类型
                        Number number = (Number) value;
                        boolean isUnsigned = (fieldMeta != null ? fieldMeta.isUnsigned() : (existOptionalMetaData ? info.unsigned : false));
                        if (isUnsigned && number.longValue() < 0) {
                            switch (buffer.getLength()) {
                                case 1: /* MYSQL_TYPE_TINY */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(TINYINT_MAX_VALUE
                                            + number.intValue())));
                                    javaType = Types.SMALLINT; // 往上加一个量级
                                    break;

                                case 2: /* MYSQL_TYPE_SHORT */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(SMALLINT_MAX_VALUE
                                            + number.intValue())));
                                    javaType = Types.INTEGER; // 往上加一个量级
                                    break;

                                case 3: /* MYSQL_TYPE_INT24 */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(MEDIUMINT_MAX_VALUE
                                            + number.intValue())));
                                    javaType = Types.INTEGER; // 往上加一个量级
                                    break;

                                case 4: /* MYSQL_TYPE_LONG */
                                    columnBuilder.setValue(String.valueOf(Long.valueOf(INTEGER_MAX_VALUE
                                            + number.longValue())));
                                    javaType = Types.BIGINT; // 往上加一个量级
                                    break;

                                case 8: /* MYSQL_TYPE_LONGLONG */
                                    columnBuilder.setValue(BIGINT_MAX_VALUE.add(BigInteger.valueOf(number.longValue()))
                                            .toString());
                                    javaType = Types.DECIMAL; // 往上加一个量级，避免执行出错
                                    break;
                            }
                        } else {
                            // 对象为number类型，直接valueof即可
                            columnBuilder.setValue(String.valueOf(value));
                        }
                        break;
                    case Types.REAL: // float
                    case Types.DOUBLE: // double
                        // 对象为number类型，直接valueof即可
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.BIT:// bit
                        // 对象为number类型
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.DECIMAL:
                        columnBuilder.setValue(((BigDecimal) value).toPlainString());
                        break;
                    case Types.TIMESTAMP:
                        // 修复时间边界值
                        // String v = value.toString();
                        // v = v.substring(0, v.length() - 2);
                        // columnBuilder.setValue(v);
                        // break;
                    case Types.TIME:
                    case Types.DATE:
                        // 需要处理year
                        columnBuilder.setValue(value.toString());
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        // fixed text encoding
                        // https://github.com/AlibabaTech/canal/issues/18
                        // mysql binlog中blob/text都处理为blob类型，需要反查table
                        // meta，按编码解析text
                        if (fieldMeta != null && isText(fieldMeta.getColumnType(), fieldMeta.getTypeName())) {
                            columnBuilder.setValue(new String((byte[]) value, charset));
                            javaType = Types.CLOB;
                        } else {
                            // byte数组，直接使用iso-8859-1保留对应编码，浪费内存
                            columnBuilder.setValue(new String((byte[]) value, ISO_8859_1));
                            // columnBuilder.setValueBytes(ByteString.copyFrom((byte[])
                            // value));
                            javaType = Types.BLOB;
                        }
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        columnBuilder.setValue(value.toString());
                        break;
                    default:
                        columnBuilder.setValue(value.toString());
                }
            }

            columnBuilder.setSqlType(javaType);

            if (isAfter) {
                incEvent.addAfterColumns(columnBuilder);
            } else {
                incEvent.addBeforeColumns(columnBuilder);
            }
        }

        return tableError;

    }

    private boolean isText(int columnType, String typeName) {
        // java.sql.Types
        if (Types.LONGVARCHAR == columnType || Types.LONGNVARCHAR == columnType) {
            return typeName.contains("TEXT");
        }
        return false;
    }

    private PipingEvent parseRowsQueryEvent(RowsQueryLogEvent event) {
        // mysql5.6支持，需要设置binlog-rows-query-log-events=1，可详细打印原始DML语句
        String queryString = null;
        try {
            queryString = new String(event.getRowsQuery().getBytes(ISO_8859_1), charset.name());
            String tableName = null;
            List<DdlResult> results = DruidDdlParser.parse(queryString, null);
            if (results.size() > 0) {
                tableName = results.get(0).getTableName();
            }
            return IncreLogEventPipingData.createQueryEvent("", tableName, queryString);
        } catch (UnsupportedEncodingException e) {
            // ignore unsupported
        }
        return null;
    }

    private PipingEvent parseAnnotateRowsEvent(AnnotateRowsEvent event) {
        // mariaDb支持，需要设置binlog_annotate_row_events=true，可详细打印原始DML语句
        String queryString = null;
        try {
            queryString = new String(event.getRowsQuery().getBytes(ISO_8859_1), charset.name());
            return IncreLogEventPipingData.createQueryEvent("", "", queryString);
        } catch (UnsupportedEncodingException e) {
            // ignore unsupported
        }
        return null;
    }

    private PipingEvent parseUserVarLogEvent(UserVarLogEvent event) {
        return IncreLogEventPipingData.createQueryEvent("", "", event.getQuery());
    }

    private PipingEvent parseIntrvarLogEvent(IntvarLogEvent event) {
        return IncreLogEventPipingData.createQueryEvent("", "", event.getQuery());
    }

    private PipingEvent parseRandLogEvent(RandLogEvent event) {
        return IncreLogEventPipingData.createQueryEvent("", "", event.getQuery());
    }

    private PipingEvent parseGTIDLogEvent(GtidLogEvent logEvent) {
        Map<String, String> properties = null;
        if (logEvent.getLastCommitted() != null) {
            properties = new HashMap<>();
            properties.put("lastCommitted", String.valueOf(logEvent.getLastCommitted()));
            properties.put("sequenceNumber", String.valueOf(logEvent.getSequenceNumber()));
        }

        return IncreLogEventPipingData.createPositionEvent(logEvent.getGtidStr(), DatabaseType.MySQL, properties);
    }

    private PipingEvent parseHeartbeatLogEvent() {
        return IncreLogEventPipingData.createHeartbeatEvent();
    }

    private PipingEvent parseMariaGTIDLogEvent(LogEvent logEvent) {
        if (logEvent instanceof MariaGtidLogEvent) {
            return IncreLogEventPipingData.createPositionEvent(((MariaGtidLogEvent) logEvent).getGtidStr(), DatabaseType.MySQL, null);
        } else if (logEvent instanceof MariaGtidListLogEvent) {
            return IncreLogEventPipingData.createPositionEvent(((MariaGtidListLogEvent) logEvent).getGtidStr(), DatabaseType.MySQL, null);
        }
        return null;
    }
}
