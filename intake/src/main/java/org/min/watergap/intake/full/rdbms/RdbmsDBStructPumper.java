package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.local.storage.LocalDataSaveTool;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.data.impl.SchemaStructBasePipingData;
import org.min.watergap.common.piping.data.impl.TableStructBasePipingData;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.intake.full.DBStructPumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    }

    /**
     * 存储所有的表表名，为了控制内存占用，边取表名边存
     * @return
     * @throws SQLException
     */
//    protected int saveAllTableNames() throws SQLException {
//        switch (baseConfig.getScope()) {
//            case ALL_DATABASE: // 迁移整个数据库
//                break;
//        }
//    }



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

    /**
     * 取表结构，边取边存
     * @param catalog
     * @return
     * @throws SQLException
     */
    private void showAllTables(String catalog) throws SQLException, InterruptedException {
        executeStreamQuery(catalog, pumperDBDialect.SHOW_TABLES(), (resultSet) -> {
            TableStructBasePipingData pipingData = new TableStructBasePipingData(catalog, resultSet.getString(1));
            LocalDataSaveTool.save(pipingData);
            structPiping.put(pipingData);
        });
    }

    private void extractTableStruct() throws InterruptedException {
        PipingData pipingData = structPiping.poll(pollTimeout);
        if (pipingData == null) {
            return;
        }

    }

    private void extractDBSchema() {
        List<PipingData> tableStructs = null;
        try {
            tableStructs = getAllSchemaStructs();
        } catch (Exception e) {
            throw new WaterGapException("show table struct error", e);
        }
        if (CollectionsUtils.isNotEmpty(tableStructs)) {
            tableStructs.forEach(schemaStruct -> {
                try {
                    LocalDataSaveTool.save(schemaStruct);
                    structPiping.put(schemaStruct);
                } catch (SQLException e) {
                    throw new WaterGapException("save FullSchemaStatus to local fail", e);
                } catch (InterruptedException e) {
                    throw new WaterGapException("put schemaStruct to fsink fail", e);
                }
            });
        }
    }

    private List<PipingData> showAllSchemas() throws SQLException, InterruptedException {
        List<PipingData> schemaStructs = new ArrayList<>();
        executeQuery(pumperDBDialect.SHOW_DATABASES(), (resultSet) -> {
            schemaStructs.add(new SchemaStructBasePipingData(resultSet.getString(1)));
        });
        return schemaStructs;
    }


    @Override
    public void isStart() {

    }
}
