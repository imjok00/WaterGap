package org.min.watergap.outfall.rdbms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.datasource.DataSourceWrapper;
import org.min.watergap.common.enums.PumpExceptionEnum;
import org.min.watergap.common.exception.ErrorCode;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.lifecycle.AbstractWaterGapLifeCycle;
import org.min.watergap.common.local.storage.orm.FullTableStructORM;
import org.min.watergap.common.rdbms.inclog.BaseIncEvent;
import org.min.watergap.common.rdbms.inclog.IncEvent;
import org.min.watergap.common.rdbms.inclog.RowUpdateDataIncEvent;
import org.min.watergap.common.rdbms.misc.binlog.type.EventType;
import org.min.watergap.common.utils.StringUtils;
import org.min.watergap.common.utils.ThreadLocalUtils;
import org.min.watergap.piping.translator.PipingData;
import org.min.watergap.piping.translator.impl.FullTableDataPipingData;
import org.min.watergap.piping.translator.impl.IncreLogEventPipingData;
import org.min.watergap.piping.translator.impl.TableStructBasePipingData;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 关系型数据库，SQL执行器
 */
public class RdbmsDataExecutor extends AbstractWaterGapLifeCycle implements DataExecutor {
    private static final Logger LOG = LogManager.getLogger(RdbmsDataExecutor.class);

    protected DataSourceWrapper dataSource;

    @Override
    public int execute(String schema, String sql, PipDataAck callback) throws WaterGapException{
        try {
            return executeUpdate(schema, sql, callback);
        } catch (Exception e) {
            LOG.error("## execute update sql error, schema : {} , sql : {}", schema, sql, e);
            throw new WaterGapException(e.getMessage(), e);
        }
    }

    @Override
    public int executeBatch(String schema, String sql, FullTableDataPipingData.ColumnValContain contain, PipDataAck callback) throws WaterGapException {
        Connection connection = null;
        PreparedStatement statement = null;
        boolean executeSuccess = false;
        try {

            connection = dataSource.getDataSource().getConnection();
            if (schema != null) {
                connection.setCatalog(schema);
            }
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            for (Map<String, Object> oneColVal : contain.getValMapList()) {
                int index = 1;
                for(TableStructBasePipingData.Column column : contain.getColumns()) {
                    statement.setObject(index, oneColVal.get(column.getColumnName()), column.getColumnType());
                    index++;
                }
                statement.addBatch();
            }

            statement.executeBatch();
            connection.commit();
            executeSuccess = true;
            return 1;
        } catch (Exception e) {
            LOG.warn("## execute update sql error, schema : {} , sql : {}, try to execute single insert!", schema, sql, e);
        } finally {
            try {
                if (executeSuccess) {
                    callback.callAck();
                } else {
                    connection.rollback();
                }
                releaseConnect(connection, statement);
            } catch (SQLException e) {
                LOG.warn("call ack error", e);
            }

        }

        // 批量出状况，就一笔笔插入
        try {
            insertBatchBySingle(schema, sql, contain.getValMapList(), contain.getColumns());
            executeSuccess = true;
            return 1;
        } catch (SQLException ex) {
            executeSuccess = false;
            throw new WaterGapException("insertBatchBySingle error", ex);
        } finally {
            if (executeSuccess) {
                try {
                    callback.callAck();
                } catch (SQLException e) {
                    LOG.error("execute callback error", e);
                }
            }
        }
    }

    /**
     * 解析出从canal中获取的Event事件<br>
     * Oracle:有变更的列值. <br>
     * <i>insert:从afterColumns中获取所有的变更数据<br>
     * <i>delete:从beforeColumns中获取所有的变更数据<br>
     * <i>update:在before中存放所有的主键和变化前的非主键值，在after中存放变化后的主键和非主键值,如果是复合主键，只会存放变化的主键<br>
     * Mysql:可以得到所有变更前和变更后的数据.<br>
     * <i>insert:从afterColumns中获取所有的变更数据<br>
     * <i>delete:从beforeColumns中获取所有的变更数据<br>
     * <i>update:在beforeColumns中存放变更前的所有数据,在afterColumns中存放变更后的所有数据<br>
     */
    @Override
    public int executeIncDataWithTrans(String schema, List<PipingData> transactionBuffer) {
        Connection connection = null;
        boolean executeSuccess = false;
        try {

            connection = dataSource.getDataSource().getConnection();
            if (schema != null) {
                connection.setCatalog(schema);
            }
            connection.setAutoCommit(false);
            for(PipingData pipingData : transactionBuffer) {
                internParse(connection, pipingData);
            }
            connection.commit();
            executeSuccess = true;
            return 1;
        } catch (Exception e) {
            LOG.warn("## execute update sql error, schema : {} , try to execute single insert!", schema, e);
            if (e.getMessage().contains("Duplicate entry")) {
                executeSuccess = true;
                return 1;
            }
        } finally {
            try {
                if (!executeSuccess) {
                    assert connection != null;
                    connection.rollback();
                }
                releaseConnect(connection, null);
            } catch (SQLException e) {
                LOG.warn("call ack error", e);
            }

        }
        return 0;
    }

    private void internParse(Connection connection, PipingData pipingData) throws SQLException {
        IncreLogEventPipingData increData = (IncreLogEventPipingData) pipingData;

        FullTableStructORM fullTableStruct = ThreadLocalUtils.getFullTableStructService().queryOne(increData.getHeader().getSchemaName(), increData.getHeader().getTableName());
        if (fullTableStruct == null) { // not exist
            LOG.error("table {}.{} not exist", fullTableStruct.getSchemaName(), fullTableStruct.getTableName());
            return;
        }
        TableStructBasePipingData tableStruct = TableStructBasePipingData.getInstance(fullTableStruct);

        if (increData.getHeader().getEventType() == EventType.INSERT) {
            // RowUpdateDataIncEvent rowData = increData.getEvents();
            for (IncEvent incEvent : increData.getEvents()) {
                RowUpdateDataIncEvent rowData = (RowUpdateDataIncEvent) incEvent;
                // insert 取 after
                StringBuilder sqlBuilder = new StringBuilder("insert into ");
                sqlBuilder.append(increData.getHeader().getSchemaName()).append(".")
                        .append(increData.getHeader().getTableName()).append(" (");

                List<BaseIncEvent.ColumnInfo> columnInfos = rowData.getAfterColumns();
                for (int i = 0, length = columnInfos.size(); i < length; i++) {
                    sqlBuilder.append(columnInfos.get(i).name);
                    if (i != length - 1) {
                        sqlBuilder.append(",");
                    }
                }
                sqlBuilder.append(") values (");
                sqlBuilder.append(StringUtils.createReplaceStr("?", columnInfos.size()));
                sqlBuilder.append(")");

                doInsert(connection, sqlBuilder.toString(), columnInfos);
            }
        } else if (increData.getHeader().getEventType() == EventType.UPDATE) {
            // before 是老数据，after是新数据
            // RowUpdateDataIncEvent rowData = (RowUpdateDataIncEvent)increData.getEvents();
            for (IncEvent incEvent : increData.getEvents()) {
                RowUpdateDataIncEvent rowData = (RowUpdateDataIncEvent) incEvent;

                StringBuilder stringBuilder = new StringBuilder("update ");
                stringBuilder.append(increData.getHeader().getSchemaName()).append(".")
                        .append(increData.getHeader().getTableName()).append(" set ");

                List<BaseIncEvent.ColumnInfo> columnInfos = rowData.getAfterColumns();

                for (int i = 0, length = columnInfos.size(); i < length; i++) {
                    stringBuilder.append(columnInfos.get(i).name).append("=?");
                    if (i != length - 1){
                        stringBuilder.append(",");
                    }

                }
                stringBuilder.append(" where ");

                List<TableStructBasePipingData.Column> uniqueKeys = findUKKeys(tableStruct);
                for (int i = 0, length = uniqueKeys.size(); i < length; i++) {
                    stringBuilder.append(uniqueKeys.get(i).getColumnName()).append("=?");
                    if (i != length - 1){
                        stringBuilder.append(" and ");
                    }
                }
                List<BaseIncEvent.ColumnInfo> keyColumnInfos = findOldKeyColumnInfo(uniqueKeys, rowData.getBeforeColumns());
                int updateCount = doUpdate(connection, stringBuilder.toString(), columnInfos, keyColumnInfos);
                if (updateCount != 1) {
                    throw new WaterGapException(ErrorCode.UPDATE_FAIL, "update column fail");
                }
            }
        } else if (increData.getHeader().getEventType() == EventType.DELETE) {
            //RowUpdateDataIncEvent rowData = (RowUpdateDataIncEvent)increData.getEvents();
            for (IncEvent incEvent : increData.getEvents()) {
                RowUpdateDataIncEvent rowData = (RowUpdateDataIncEvent) incEvent;
                StringBuilder stringBuilder = new StringBuilder("delete from ")
                        .append(increData.getHeader().getSchemaName()).append(".")
                        .append(increData.getHeader().getTableName()).append(" where ");
                List<TableStructBasePipingData.Column> uniqueKeys = findUKKeys(tableStruct);
                for (TableStructBasePipingData.Column column : uniqueKeys) {
                    stringBuilder.append(column.getColumnName()).append("=?");
                }
                List<BaseIncEvent.ColumnInfo> keyColumnInfos = findOldKeyColumnInfo(uniqueKeys, rowData.getBeforeColumns());
                int updateCount = doDelete(connection, stringBuilder.toString(), keyColumnInfos);
                if (updateCount != 1) {
                    throw new WaterGapException(ErrorCode.UPDATE_FAIL, "delete column fail");
                }
            }
        }
    }

    private void setColumnValue(BaseIncEvent.ColumnInfo columnInfo,PreparedStatement statement, int paramIndex) throws SQLException {
        if (columnInfo.isNull()) {
            statement.setNull(paramIndex, columnInfo.sqlType);
            return;
        }
        statement.setObject(paramIndex, columnInfo.value, columnInfo.sqlType);
    }

    private int doDelete(Connection connection, String sql, List<BaseIncEvent.ColumnInfo> columnInfos) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(sql);
        ) {
            for (int i = 0; i < columnInfos.size(); i++) {
                setColumnValue(columnInfos.get(i), statement, i + 1);
            }
            return statement.executeUpdate();
        }
    }

    private void doInsert(Connection connection, String sql, List<BaseIncEvent.ColumnInfo> columnInfos) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(sql);
        ) {
            for (int i = 0; i < columnInfos.size(); i++) {
                setColumnValue(columnInfos.get(i), statement, i + 1);
            }
            statement.execute();
        }
    }

    private List<BaseIncEvent.ColumnInfo> findOldKeyColumnInfo(List<TableStructBasePipingData.Column> uniqueKeys, List<BaseIncEvent.ColumnInfo> beforeColumnInfos) {
        Map<String, BaseIncEvent.ColumnInfo> beforeColumnMap = new HashMap<>(beforeColumnInfos.size());
        for (BaseIncEvent.ColumnInfo columnInfo : beforeColumnInfos) {
            beforeColumnMap.put(columnInfo.name, columnInfo);
        }
        List<BaseIncEvent.ColumnInfo> resultColumn = new ArrayList<>(uniqueKeys.size());
        for (TableStructBasePipingData.Column column : uniqueKeys) {
            BaseIncEvent.ColumnInfo columnInfo = beforeColumnMap.get(column.getColumnName());
            if (columnInfo == null) {
                LOG.error("can not found column {}", column.getColumnName());
            } else {
                resultColumn.add(columnInfo);
            }
        }
        return resultColumn;
    }

    private int doUpdate(Connection connection, String sql, List<BaseIncEvent.ColumnInfo> columnInfos, List<BaseIncEvent.ColumnInfo> keyColumnInfos) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(sql);
        ) {
            columnInfos.addAll(keyColumnInfos);
            for (int i = 0; i < columnInfos.size(); i++) {
                setColumnValue(columnInfos.get(i), statement, i + 1);
            }
            return statement.executeUpdate();
        }
    }

    private List<TableStructBasePipingData.Column> findUKKeys(TableStructBasePipingData tableStruct) {
        List<TableStructBasePipingData.Column> uniqueKeys = tableStruct.getIndexInfo().getPrimaryKeys();
        if (uniqueKeys == null) {
            uniqueKeys = tableStruct.getIndexInfo().getRandomUK();
        }
        if(uniqueKeys == null) {
            LOG.error("can not found uk ,table is {}.{}", tableStruct.getSchemaName(), tableStruct.getTableName());
            uniqueKeys = tableStruct.getColumns();
        }
        return uniqueKeys;
    }

    private Map<String, TableStructBasePipingData.Column> covertToColumnMap(List<TableStructBasePipingData.Column> columns) {
        return columns.stream().collect(Collectors.toMap(TableStructBasePipingData.Column::getColumnName, obj -> obj));
    }

    private void insertBatchBySingle(String schema, String sql, List<Map<String, Object>> colvals, List<TableStructBasePipingData.Column> columns) throws SQLException {
        for (Map<String, Object> singleOne : colvals) {
            // 异常分类处理
            insertSingle(schema, sql, singleOne, columns);
        }
    }

    private void insertSingle(String schema, String sql, Map<String, Object> oneColVal, List<TableStructBasePipingData.Column> columns) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getDataSource().getConnection();
            if (schema != null) {
                connection.setCatalog(schema);
            }
            statement = connection.prepareStatement(sql);

            int index = 1;
            for(TableStructBasePipingData.Column column : columns) {
                statement.setObject(index, oneColVal.get(column.getColumnName()), column.getColumnType());
                index ++;
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            LOG.error("## insert single row error, schema : {} , record : {}", schema, oneColVal.values(), e);
            throw e;
        } finally {
            releaseConnect(connection, statement);
        }
    }

    private int executeUpdate(String schema, String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dataSource.getDataSource().getConnection();
            if (schema != null) {
                connection.setCatalog(schema);
            }
            statement = connection.createStatement();
            return statement.executeUpdate(sql);
        } catch (Exception e) {
            throw e;
        } finally {
            releaseConnect(connection, statement);
        }
    }

    private int executeUpdate(String schema, String sql, PipDataAck callback) throws SQLException{
        Statement statement = null;
        Connection connection = null;
        int result;
        boolean executeSucc = false;
        try {
            connection = dataSource.getDataSource().getConnection();
            connection.setAutoCommit(false);
            if (schema != null) {
                connection.setCatalog(schema);
            }
            statement = connection.createStatement();
            result = statement.executeUpdate(sql);
            executeSucc = true;
            connection.commit();
        } catch (Exception e) {
            if (connection != null) {
                connection.rollback();
            }
            if (e.getMessage().contains(PumpExceptionEnum.DATABASE_EXISTS.getMsg())) {
                LOG.warn("## execute update sql error, schema : {} , sql : {}", schema, sql);
                executeSucc = true;
                result = 1;
            } else if (e.getMessage().contains(PumpExceptionEnum.TABLE_EXISTS.getMsg())) {
                LOG.warn("## execute update sql error, schema : {} , sql : {}", schema, sql);
                executeSucc = true;
                result = 1;
            } else {
                throw e;
            }
        } finally {
            if (executeSucc) {
                callback.callAck();
            }
            releaseConnect(connection, statement);
        }
        return result;
    }

    protected void releaseConnect(Connection connection, Statement statement) throws SQLException {
        releaseConnect(connection, statement, null);
    }

    protected void releaseConnect(Connection connection, Statement statement, ResultSet resultSet) throws SQLException {
        if (connection != null) {
            connection.close();
        }

        if (statement != null) {
            statement.close();
        }

        if (resultSet != null) {
            resultSet.close();
        }
    }

    @Override
    public void init(WaterGapContext waterGapContext) {
        dataSource = waterGapContext.getOutDataSource();
        ThreadLocalUtils.init();
    }

    @Override
    public void destroy() {
        try {
            dataSource.revert();
        } catch (Exception e) {
            LOG.error("target data executor destroy error", e);
        }

    }
}
