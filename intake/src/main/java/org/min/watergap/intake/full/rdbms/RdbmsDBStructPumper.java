package org.min.watergap.intake.full.rdbms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.local.storage.LocalDataSaveTool;
import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.data.impl.FullTableDataBasePipingData;
import org.min.watergap.common.piping.struct.impl.SchemaStructBasePipingData;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.common.utils.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 关系型数据库数据结构导出
 *
 * @Create by metaX.h on 2021/11/10 23:16
 */
public class RdbmsDBStructPumper extends RdbmsDataPumper {
    private static final Logger LOG = LogManager.getLogger(RdbmsDBStructPumper.class);

    @Override
    public void pump() throws WaterGapException {
        // step 1: get all table schema into localstorage
        extractDBSchema();
        // step 2: get ack to extract table
        runPumpWork(this::ackAndRunNextStage);
    }

    protected void ackAndRunNextStage() {
        for (;;) {
            try {
                PipingData pipingData = ackPiping.take();
                switch (pipingData.getType()) {
                    case SCHEMA:/* 数据库建立好之后，开始迁移表结构 */
                        runPumpWork(() -> {
                            try {
                                SchemaStructBasePipingData schemaData = (SchemaStructBasePipingData) pipingData;
                                showAllTables(schemaData.getName());
                            } catch (SQLException e) {
                                LOG.error("execute pump schema data from source error", e);
                            } catch (InterruptedException e) {
                                LOG.error("execute pump schema interrupt error", e);
                            }
                        });
                        break;
//                    case TABLE: /* 表结构迁移完成之后开始迁移数据 */
//                        runPumpWork(() -> {
//                            TableStructBasePipingData tableStruct = (TableStructBasePipingData) pipingData;
//                            try {
//                                startDataPumper(tableStruct);
//                            } catch (InterruptedException e) {
//                                LOG.error("start pump data interrupt error", e);
//                            }
//                        });
//                        break;
                    case FULL_DATA: /* 数据更新以后回调,进入下一part提取 */
                        runPumpWork(() -> {
                            FullTableDataBasePipingData tableDataBasePipingData = (FullTableDataBasePipingData) pipingData;
                            try {
                                startNextDataPumper(tableDataBasePipingData);
                            } catch (InterruptedException e) {
                                LOG.error("start pump data interrupt error", e);
                            } catch (SQLException e) {
                                LOG.error("get source table data error", e);
                            }
                        });
                        break;
                }
            } catch (InterruptedException interruptedException) {
                LOG.error("poll data from ack piping error", interruptedException);
            }

        }
    }

    private void startNextDataPumper(FullTableDataBasePipingData tableData) throws InterruptedException, SQLException {
        String selectSql = generateSelectSQL(tableData);
        executeStreamQuery(tableData.getSchemaName(), selectSql, (resultSet) -> {
            FullTableDataBasePipingData.ColumnValContain contain
                    = new FullTableDataBasePipingData.ColumnValContain(tableData.getColumns());
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (TableStructBasePipingData.Column column : tableData.getColumns()) {
                    map.put(column.getColumnName(), convertSqlType(column, resultSet));
                }
                contain.addVal(map);
            }
            tableData.setContain(contain);
            structPiping.put(tableData);
        });
    }

    /**
     * 存入所有的schema对象
     *
     * @return
     * @throws SQLException
     */
    protected List<PipingData> getAllSchemaStructs() throws SQLException, InterruptedException {
        final List<PipingData> result = new ArrayList<>();
        switch (waterGapContext.getGlobalConfig().getScope().getScopeType()) {
            case PARTIAL_DATABASE: // 只是迁移部分库
            case PARTIAL_TABLE: // 只是迁移部分表
                waterGapContext.getGlobalConfig().getScope().getSchemaMaps().forEach(schemaMap -> {
                    result.add(new SchemaStructBasePipingData(schemaMap.getSchemaName()));
                });
                break;
            case ALL: // 迁移整个库
            default:
                result.addAll(showAllSchemas());
                break;
        }
        if (CollectionsUtils.isEmpty(result)) {
            LOG.warn("full stage, can't found any schema to migrate!");
        }
        return result;
    }

    /**
     * 取表结构，边取边存
     * @param catalog
     * @return
     * @throws SQLException
     */
    private void showAllTables(String catalog) throws SQLException, InterruptedException {
        executeStreamQuery(catalog, pumperDBDialect.SHOW_TABLES(), (resultSet) -> {
            while (resultSet.next()) {
                TableStructBasePipingData pipingData = new TableStructBasePipingData(catalog, resultSet.getString(1));
                pipingData.setIdentical(isIdentical);
                assembleTableStruct(pipingData);
                LocalDataSaveTool.save(pipingData);
                structPiping.put(pipingData);
            }
        });
    }

    protected void assembleTableStruct(TableStructBasePipingData pipingData) throws SQLException, InterruptedException {
        executeQuery(pipingData.getSchemaName(), pumperDBDialect.SHOW_CREATE_TABLE(pipingData.getTableName()), (resultSet) -> {
            while (resultSet.next()) {
                pipingData.setSourceCreateSql(resultSet.getString(2));
            }
        });
        extractTableColumns(pipingData);
        extractTablePrimaryKeys(pipingData);
        extractTableUniqueKeys(pipingData);
        extractTableNormalKeys(pipingData);
        extractTableForeignKes(pipingData);
    }

    private void extractTableColumns(TableStructBasePipingData pipingData) throws SQLException {
        pipingData.initColumns();
        try(
                Connection connection = dataSource.getDataSource().getConnection();
                ResultSet rs = connection.getMetaData().getColumns(pipingData.getSchemaName(), pipingData.getSchemaName(),
                        pipingData.getTableName(), null);
        ) {
            while (rs.next()) {

                String columnName = rs.getString("COLUMN_NAME");
                // SQL type from java.sql.Types
                int columnType = rs.getInt("DATA_TYPE");
                String typeName = rs.getString("TYPE_NAME");
                columnType = convertSqlType(columnType, typeName);
                pipingData.addColumn(new TableStructBasePipingData.Column(columnName, columnType, typeName));
            }
        }
    }

    private static int convertSqlType(int columnType, String typeName) {
        String[] typeSplit = typeName.split(" ");
        if (typeSplit.length > 1) {
            if (columnType == Types.INTEGER && StringUtils.equalsIgnoreCase(typeSplit[1], "UNSIGNED")) {
                columnType = Types.BIGINT;
            }
        }

        if (columnType == Types.OTHER) {
            if (StringUtils.equalsIgnoreCase(typeName, "NVARCHAR")
                    || StringUtils.equalsIgnoreCase(typeName, "NVARCHAR2")) {
                columnType = Types.VARCHAR;
            }

            if (StringUtils.equalsIgnoreCase(typeName, "NCLOB")) {
                columnType = Types.CLOB;
            }

            if (StringUtils.startsWithIgnoreCase(typeName, "TIMESTAMP")) {
                columnType = Types.TIMESTAMP;
            }
        }
        return columnType;
    }



    private void extractTablePrimaryKeys(TableStructBasePipingData pipingData) throws SQLException {
        try(
                Connection connection = dataSource.getDataSource().getConnection();
                ResultSet rs = connection.getMetaData().getPrimaryKeys(pipingData.getSchemaName(), pipingData.getSchemaName(),
                        pipingData.getTableName());
        ) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                Integer keySeq = rs.getInt("KEY_SEQ");
                pipingData.addPrimaryKeys(new TableStructBasePipingData.Column(columnName));
            }
        }
    }

    private void extractTableUniqueKeys(TableStructBasePipingData pipingData) throws SQLException {
        try(
                Connection connection = dataSource.getDataSource().getConnection();
                ResultSet rs = connection.getMetaData().getIndexInfo(pipingData.getSchemaName(), pipingData.getSchemaName(),
                        pipingData.getTableName(), true, false);
        ) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                int indexType = rs.getInt("TYPE");
                int ordinalPosition = rs.getInt("ORDINAL_POSITION");
                String ascOrDesc = rs.getString("ASC_OR_DESC");
                pipingData.addUniqueKeys(indexName, new TableStructBasePipingData.Column(columnName));
            }
        }
    }

    private void extractTableNormalKeys(TableStructBasePipingData pipingData) throws SQLException {
        try(
                Connection connection = dataSource.getDataSource().getConnection();
                ResultSet rs = connection.getMetaData().getIndexInfo(pipingData.getSchemaName(), pipingData.getSchemaName(),
                        pipingData.getTableName(), false, false);
        ) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                int indexType = rs.getInt("TYPE");
                int ordinalPosition = rs.getInt("ORDINAL_POSITION");
                String ascOrDesc = rs.getString("ASC_OR_DESC");
                pipingData.addNormalKeys(indexName, new TableStructBasePipingData.Column(columnName));
            }
        }
    }

    private void extractTableForeignKes(TableStructBasePipingData pipingData) throws SQLException {
        try(
                Connection connection = dataSource.getDataSource().getConnection();
                ResultSet rs = connection.getMetaData().getImportedKeys(pipingData.getSchemaName(), pipingData.getSchemaName(),
                        pipingData.getTableName());
                //connection.getMetaData().getExportedKeys()
        ) {
            while (rs.next()) {
                String PKCOLUMN_NAME = rs.getString("PKCOLUMN_NAME");
                String FKCOLUMN_NAME = rs.getString("FKCOLUMN_NAME");
                int KEY_SEQ = rs.getInt("KEY_SEQ");
                String FK_NAME = rs.getString("FK_NAME");
                String PK_NAME = rs.getString("PK_NAME");
                pipingData.addForeignKes(FK_NAME, new TableStructBasePipingData.Column(FKCOLUMN_NAME));
            }
        }
    }

    private void extractDBSchema() {
        List<PipingData> pipingDataList = null;
        try {
            pipingDataList = getAllSchemaStructs();
            pipingDataList = filterPipData(pipingDataList);
        } catch (Exception e) {
            throw new WaterGapException("show table struct error", e);
        }
        if (CollectionsUtils.isNotEmpty(pipingDataList)) {
            pipingDataList.forEach(schemaStruct -> {
                try {
                    AbstractLocalStorageEntity.LocalStorageStatus status = LocalDataSaveTool.getLocalDataStatus(schemaStruct);
                    if (status == null) { // 还没有初始化
                        LocalDataSaveTool.save(schemaStruct);
                        structPiping.put(schemaStruct);
                    }
                } catch (SQLException e) {
                    throw new WaterGapException("save FullSchemaStatus to local fail", e);
                } catch (InterruptedException e) {
                    throw new WaterGapException("put schemaStruct to fsink fail", e);
                }
            });
        }
    }

    protected List<PipingData> filterPipData(List<PipingData> pipingDataList) {
        return pipingDataList;
    }

    private List<PipingData> showAllSchemas() throws SQLException, InterruptedException {
        List<PipingData> schemaStructs = new ArrayList<>();
        executeQuery(pumperDBDialect.SHOW_DATABASES(), (resultSet) -> {
            while (resultSet.next()) {
                schemaStructs.add(new SchemaStructBasePipingData(resultSet.getString(1)));
            }
        });
        return schemaStructs;
    }

    @Override
    public void isStart() {

    }

}
