package org.min.watergap.outfall.rdbms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.datasource.DataSourceWrapper;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.lifecycle.AbstractWaterGapLifeCycle;
import org.min.watergap.common.piping.data.impl.FullTableDataBasePipingData;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;

import java.sql.*;
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
    public int execute(String schema, String sql, PipDataAck callback){
        try {
            return executeUpdate(schema, sql, callback);
        } catch (Exception e) {
            LOG.error("## execute update sql error, schema : {} , sql : {}", schema, sql, e);
            return 0;
        }
    }

    @Override
    public int executeBatch(String schema, String sql, FullTableDataBasePipingData.ColumnValContain contain, PipDataAck callback) throws WaterGapException {
        Connection connection = null;
        PreparedStatement statement = null;
        boolean executeSuccess = false;
        try {

            connection = dataSource.getDataSource().getConnection();
            if (schema != null) {
                connection.setCatalog(schema);
            }
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
            executeSuccess = true;
            return 1;
        } catch (Exception e) {
            LOG.error("## execute update sql error, schema : {} , sql : {}", schema, sql, e);
        } finally {
            try {
                if (executeSuccess) {
                    callback.callAck();
                }
                releaseConnect(connection, statement);
            } catch (SQLException e) {
                throw new WaterGapException("release connect error", e);
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
                    throw new WaterGapException("execute callback error", e);
                }
            }
        }
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
            throw e;
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
