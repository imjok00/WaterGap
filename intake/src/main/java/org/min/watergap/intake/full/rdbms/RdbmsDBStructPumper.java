package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.local.storage.LocalDataSaveTool;
import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.data.impl.SchemaStructBasePipingData;
import org.min.watergap.common.piping.data.impl.TableStructBasePipingData;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.intake.full.DBStructPumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 关系型数据库数据结构导出
 *
 * @Create by metaX.h on 2021/11/10 23:16
 */
public class RdbmsDBStructPumper extends DBStructPumper {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsDBStructPumper.class);

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
                    case SCHEMA:
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
                    case TABLE:
                        runPumpWork(() -> {
                            TableStructBasePipingData tableStruct = (TableStructBasePipingData) pipingData;

                        });
                }
            } catch (InterruptedException interruptedException) {
                LOG.error("poll data from ack piping error", interruptedException);
            }

        }
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
                assembleTableStruct(pipingData);
                LocalDataSaveTool.save(pipingData);
                structPiping.put(pipingData);
            }
        });
    }

    protected void assembleTableStruct(TableStructBasePipingData pipingData) throws SQLException, InterruptedException {
//        executeQuery(pipingData.getSchemaName(), pumperDBDialect.SHOW_CREATE_TABLE(pipingData.getTableName()), (resultSet) -> {
//            while (resultSet.next()) {
//                pipingData.setSourceCreateSql(resultSet.getString(2));
//            }
//        });
        pipingData.setColumns(new ArrayList<>());
        try(
                Connection connection = dataSource.getDataSource().getConnection()
        ) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet rs = metaData.getColumns(pipingData.getSchemaName(), pipingData.getSchemaName(),
                    pipingData.getTableName(), null);
            while (rs.next()) {
                String name = rs.getString(3);
                String columnName = rs.getString(4);
                int columnType = rs.getInt(5);
                String typeName = rs.getString(6);
                pipingData.addColumn(new TableStructBasePipingData.Column(columnName, columnType));
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
