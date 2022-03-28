package org.min.watergap.outfall;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.LocalDataSaveTool;
import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.piping.data.impl.FullTableDataBasePipingData;
import org.min.watergap.common.piping.struct.impl.BasePipingData;
import org.min.watergap.common.piping.struct.impl.SchemaStructBasePipingData;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.outfall.convertor.ConvertorChooser;
import org.min.watergap.outfall.convertor.StructConvertor;

/**
 * 关系型数据库执行器
 *
 * @Create by metaX.h on 2022/3/4 22:55
 */
public class RdbmsOutFallDrainer extends OutFallDrainer {
    private static final Logger LOG = LogManager.getLogger(RdbmsOutFallDrainer.class);

    protected void doExecute(BasePipingData data) {
        LOG.info("## Start Drainer Data {}", data);
        StructConvertor structConvertor = ConvertorChooser.chooseConvertor(targetDBType);;
        switch (data.getType()) {
            case SCHEMA:
                SchemaStructBasePipingData schemaStruct = (SchemaStructBasePipingData) data;
                dataExecutor.execute(null, structConvertor.convert(schemaStruct), () -> {
                    LocalDataSaveTool.updateLocalDataStatus(data, AbstractLocalStorageEntity.LocalStorageStatus.COMPLETE.getStatus());
                    ack(data);
                    LOG.info("## Complete Drainer Data {}", data);
                });
                break;
            case TABLE:
                TableStructBasePipingData tableStruct = (TableStructBasePipingData) data;
                dataExecutor.execute(tableStruct.getSchemaName(), structConvertor.convert(tableStruct), () -> {
                    LocalDataSaveTool.updateLocalDataStatus(tableStruct, AbstractLocalStorageEntity.LocalStorageStatus.COMPLETE.getStatus());
                    // 表结构迁移完成，开始迁移数据
                    ackAndStartFullTableData(tableStruct);
                    LOG.info("## Complete Drainer Data {}", data);
                });
                break;
            case FULL_DATA:
                FullTableDataBasePipingData tableData = (FullTableDataBasePipingData) data;
                dataExecutor.executeBatch(tableData.getSchemaName(), structConvertor.convert(tableData), tableData.getContain(), () -> {
                    LocalDataSaveTool.updateLocalDataPosition(tableData, tableData.getPosition());
                    ack(tableData);
                    LOG.info("## Complete Drainer Data {}", data);
                });
                break;

        }

    }

    private void ackAndStartFullTableData(TableStructBasePipingData tableStruct) {
        FullTableDataBasePipingData fullTableDataBasePipingData = new FullTableDataBasePipingData(tableStruct);
        ack(fullTableDataBasePipingData);
    }

}
