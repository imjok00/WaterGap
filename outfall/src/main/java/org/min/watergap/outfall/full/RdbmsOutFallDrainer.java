package org.min.watergap.outfall.full;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.orm.FullTableDataPositionORM;
import org.min.watergap.common.local.storage.orm.MigrateStageORM;
import org.min.watergap.common.local.storage.orm.service.MigrateStageService;
import org.min.watergap.common.utils.ThreadLocalUtils;
import org.min.watergap.outfall.OutFallDrainer;
import org.min.watergap.outfall.convertor.ConvertorChooser;
import org.min.watergap.outfall.convertor.StructConvertor;
import org.min.watergap.piping.translator.PipingData;
import org.min.watergap.piping.translator.impl.FullTableDataPipingData;
import org.min.watergap.piping.translator.impl.SchemaStructBasePipingData;
import org.min.watergap.piping.translator.impl.TableStructBasePipingData;

import java.sql.SQLException;

/**
 * 关系型数据库执行器
 *
 * @Create by metaX.h on 2022/3/4 22:55
 */
public class RdbmsOutFallDrainer extends OutFallDrainer {
    private static final Logger LOG = LogManager.getLogger(RdbmsOutFallDrainer.class);

    protected void doExecute(PipingData data) {
        LOG.info("## Start Drainer Data {}", data);
        StructConvertor structConvertor = ConvertorChooser.chooseConvertor(targetDBType);;
        switch (data.getType()) {
            case SCHEMA:
                SchemaStructBasePipingData schemaStruct = (SchemaStructBasePipingData) data;
                dataExecutor.execute(null, structConvertor.convert(schemaStruct), () -> {
                    ThreadLocalUtils.getSchemaStatusService().update(schemaStruct.getSchemaName(), MigrateStageService.LocalStorageStatus.COMPLETE.getStatus());
                    tryUpdateStage(data);
                    ack(data);
                    LOG.info("## Complete Draining schema Data {}", schemaStruct.getSchemaName());
                });
                break;
            case TABLE:
                TableStructBasePipingData tableStruct = (TableStructBasePipingData) data;
                dataExecutor.execute(tableStruct.getSchemaName(), structConvertor.convert(tableStruct), () -> {
                    ThreadLocalUtils.tryCreateDataPosition(
                            new FullTableDataPositionORM(tableStruct.getSchemaName(), tableStruct.getTableName(), ""));
                    tryUpdateStage(data);
                    // 表结构迁移完成，开始迁移数据
                    ackAndStartFullTableData(tableStruct);
                    LOG.info("## Complete Draining table struct {}.{}", tableStruct.getSchemaName(), tableStruct.getTableName());
                });
                break;
            case FULL_DATA:
                FullTableDataPipingData tableData = (FullTableDataPipingData) data;
                dataExecutor.executeBatch(tableData.getSchemaName(), structConvertor.convert(tableData), tableData.getContain(), () -> {
                    ThreadLocalUtils.getFullTableDataPositionService().updatePosition(
                            tableData.getPosition(), tableData.getSchemaName(), tableData.getTableName());
                    tryUpdateStage(data);
                    ack(tableData);
                    LOG.info("## Complete Drainer Data {}.{}", tableData.getSchemaName(), tableData.getTableName());
                });
                break;

        }

    }

    private void ackAndStartFullTableData(TableStructBasePipingData tableStruct) {
        FullTableDataPipingData fullTableDataBasePipingData = new FullTableDataPipingData(tableStruct);
        fullTableDataBasePipingData.setNeedInit(true);
        ack(fullTableDataBasePipingData);
    }

    public void tryUpdateStage(PipingData pipingData) throws SQLException {
        if (pipingData instanceof SchemaStructBasePipingData) { // schema 更新
            if(ThreadLocalUtils.getSchemaStatusService().isAllComplete()) {
                ThreadLocalUtils.getMigrateStageService()
                        .updateStage(MigrateStageORM.StageEnum.SCHEMA_MIGRATED.toString());
            }
        } else if (pipingData instanceof FullTableDataPipingData) {// 表数据更新
            if(ThreadLocalUtils.getFullTableDataPositionService().isAllComplete()) {
                ThreadLocalUtils.getMigrateStageService()
                        .updateStage(MigrateStageORM.StageEnum.TABLE_MIGRATED.toString());
            }
        } else if (pipingData instanceof TableStructBasePipingData) {// 表结构更新
            if(ThreadLocalUtils.getFullTableStructService().isAllComplete()) {
                ThreadLocalUtils.getMigrateStageService()
                        .updateStage(MigrateStageORM.StageEnum.FULL_OVER.toString());
            }
        }
    }

    @Override
    public void destroy() {
        super.destroy();
    }
}
