package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.intake.full.FullExtractor;
import org.min.watergap.intake.full.rdbms.rs.NormalResultSetCallback;
import org.min.watergap.intake.full.rdbms.rs.ResultSetCallback;
import org.min.watergap.intake.full.rdbms.to.ColumnStruct;
import org.min.watergap.intake.full.rdbms.to.TableStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 关系形数据库全量数据导出
 *
 * @Create by metaX.h on 2021/11/10 23:16
 */
public abstract class RdbmsFullExtractor extends FullExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsFullExtractor.class);

    protected abstract void extractTableSchema() throws WaterGapException;

    protected abstract void extractTableDatas();

    @Override
    public void extractor() throws WaterGapException {
        // step 1: get all table schema into localstorage
        extractTableSchema();
        // step 2: get all table data into localstorage
        extractTableDatas();
    }

    protected List<TableStruct> getAllTableStruct() throws SQLException {
        final List<TableStruct> result = new ArrayList<>();
        switch (baseConfig.scope) {
            case SOME_TABLE: // 只是迁移部分表
                baseConfig.getScopePairs().forEach(scopePair -> {
                    result.add(new TableStruct(scopePair.getSchemaName(), scopePair.getTableName()));
                });
                break;
            case ALL_DATABASE: // 迁移整个库
                result.addAll(showAllDatabases());
                break;
            case SOME_DATABASE:
                baseConfig.getScopePairs().forEach(scopePair -> {
                    try {
                        result.addAll(showAllTables(scopePair.getSchemaName()));
                    } catch (SQLException e) {
                        LOG.error("can not get table from schema {}", scopePair.getSchemaName(), e);
                        throw new WaterGapException("get source tables error", e);
                    }
                });
                break;
            default:
                LOG.warn("scope config is error");
                break;
        }

        result.forEach(tableStruct -> {

        });

        return result;
    }

    private List<TableStruct> showAllTables(String catalog) throws SQLException {
        List<TableStruct> tableStructs = new ArrayList<>();
        executeQuery(catalog, dbDialect.SHOW_TABLES(), (resultSet) -> {
            tableStructs.add(new TableStruct(catalog, resultSet.getString(1)));
        });
        return tableStructs;
    }

    private List<TableStruct> showAllDatabases() throws SQLException {
        List<TableStruct> tableStructs = new ArrayList<>();
        executeQuery(dbDialect.SHOW_DATABASES(), (resultSet -> {
            tableStructs.addAll(showAllTables(resultSet.getString(1)));
        }));
        return tableStructs;
    }

    private TableStruct showTableMeta(TableStruct tableStruct) throws SQLException {
        executeQuery(tableStruct.getSchema(), dbDialect.SHOW_CREATE_TABLE(tableStruct.getTable()), new ResultSetCallback() {
            @Override
            public void callBack(ResultSet resultSet) throws SQLException {
                //tableStruct.setCreateSql(resultSet.getString(2));
            }
        });
        return tableStruct;

    }

    public List<ColumnStruct> showTableMetaData(String catalog, String schema, String table) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getColumns(catalog, schema, table, null);
            NormalResultSetCallback normalResultSetCallback = new NormalResultSetCallback();
            normalResultSetCallback.callBack(resultSet);
            return normalResultSetCallback.getBaseStructs();
        } catch (Exception e) {
            throw e;
        } finally {
            releaseConnect(connection, statement, resultSet);
        }
    }
}
