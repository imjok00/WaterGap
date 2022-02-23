package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.intake.full.DBStructPumper;
import org.min.watergap.intake.full.rdbms.result.NormalResultSetCallback;
import org.min.watergap.intake.full.rdbms.result.ResultSetCallback;
import org.min.watergap.intake.full.rdbms.struct.ColumnStruct;
import org.min.watergap.intake.full.rdbms.struct.SchemaStruct;
import org.min.watergap.intake.full.rdbms.struct.TableStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 关系型数据库数据结构导出
 *
 * @Create by metaX.h on 2021/11/10 23:16
 */
public abstract class RdbmsDBStructPumper extends DBStructPumper {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsDBStructPumper.class);

    protected abstract void extractTableSchema() throws WaterGapException;

    protected abstract void extractTableStructs() throws WaterGapException;

    protected abstract void extractTableDatas() throws WaterGapException;

    @Override
    public void pump() throws WaterGapException {
        // step 1: get all table schema into localstorage
        extractTableSchema();
        // step 2: get all table struct into localstorage
        extractTableStructs();
    }

    /**
     * 存储所有的表表名，为了控制内存占用，边取表名边存
     * @return
     * @throws SQLException
     */
    //protected int saveAllTableNames() throws SQLException {
        //switch (baseConfig.getScope()) {
            //case ALL_DATABASE: // 迁移整个数据库
           //     break;
        //}
    //}

    /**
     * 存入所有的schema对象
     *
     * @return
     * @throws SQLException
     */
    protected List<SchemaStruct> getAllSchemaStructs() throws SQLException {
        final List<SchemaStruct> result = new ArrayList<>();
        switch (baseConfig.getScope().getScopeType()) {
            case PARTIAL_DATABASE: // 只是迁移部分库
            case PARTIAL_TABLE: // 只是迁移部分表
                baseConfig.getScope().getSchemaMaps().forEach(schemaMap -> {
                    result.add(SchemaStruct.newObject(schemaMap.getSchemaName()));
                });
                break;
            case ALL_DATABASE: // 迁移整个库
            default:
                result.addAll(showAllSchemas());
                break;
        }
        if (CollectionsUtils.isEmpty(result)) {
            LOG.warn("full stage, can't found any schema to migrate!");
        }
        return result;
    }

    private List<TableStruct> showAllTables(String catalog) throws SQLException {
        List<TableStruct> tableStructs = new ArrayList<>();
        executeQuery(catalog, pumperDBDialect.SHOW_TABLES(), (resultSet) -> {
            tableStructs.add(new TableStruct(catalog, resultSet.getString(1)));
        });
        return tableStructs;
    }

    private List<SchemaStruct> showAllSchemas() throws SQLException {
        List<SchemaStruct> schemaStructs = new ArrayList<>();
        executeQuery(pumperDBDialect.SHOW_DATABASES(), (resultSet) -> {
            schemaStructs.add(SchemaStruct.newObject(resultSet.getString(1)));
        });
        return schemaStructs;
    }

    private TableStruct showTableMeta(TableStruct tableStruct) throws SQLException {
        executeQuery(tableStruct.getSchema(), pumperDBDialect.SHOW_CREATE_TABLE(tableStruct.getTable()), new ResultSetCallback() {
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
            connection = dataSource.getDataSource().getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getColumns(catalog, schema, table, null);
            NormalResultSetCallback normalResultSetCallback = new NormalResultSetCallback(ColumnStruct.class);
            normalResultSetCallback.callBack(resultSet);
            return normalResultSetCallback.getBaseStructs();
        } catch (Exception e) {
            throw e;
        } finally {
            releaseConnect(connection, statement, resultSet);
        }
    }

}
